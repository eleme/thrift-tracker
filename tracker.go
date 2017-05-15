package tracker

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/damnever/thrift-tracker/tracking"
	"github.com/google/uuid"
)

type ctxKey string

const (
	CtxKeySequenceID  ctxKey = "__thrift_tracking_sequence_id"
	CtxKeyRequestID   ctxKey = "__thrift_tracking_request_id"
	CtxKeyRequestMeta ctxKey = "__thrift_tracking_request_meta"
	// CtxKeyResponseMeta           ctxKey = "__thrift_tracking_response_meta"
	TrackingAPIName              string = "ElemeThriftTrackingAPI"
	VersionDefault               int32  = 0
	VersionRequestHeader         int32  = 1
	VersionRequestResponseHeader int32  = 2 // reserved
	VersionMax                   int32  = VersionRequestHeader
)

type HandShaker interface {
	Negotiation(curSeqID int32, iprot, oprot thrift.TProtocol) error
	TryUpgrade(seqID int32, iprot, oprot thrift.TProtocol) (bool, thrift.TException)
	RequestHeaderSupported() bool
	// ResponseHeaderSupported() bool
}

type Tracker interface {
	HandShaker

	RequestSeqIDFromCtx(ctx context.Context) (string, string)
	TryReadRequestHeader(iprot thrift.TProtocol) (context.Context, error) // context will pass into service handler
	TryWriteRequestHeader(ctx context.Context, oprot thrift.TProtocol) error
	// TryReadResponseHeader(iprot thrift.TProtocol) error
	// TryWriteResponseHeader(ctx context.Context, oprot thrift.TProtocol) error
}

type NewTrackerFactoryFunc func(client, server string, hooks Hooks) func() Tracker

type Hooks struct {
	onHandshakRequest   func(args *tracking.UpgradeArgs_)
	onRecvHeaderRequest func(header *tracking.RequestHeader)
}

var DefaultHooks = Hooks{
	onHandshakRequest:   func(args *tracking.UpgradeArgs_) { fmt.Printf("%#+v\n", args) },
	onRecvHeaderRequest: func(header *tracking.RequestHeader) { fmt.Printf("%#+v\n", header) },
}

type SimpleTracker struct {
	mu      *sync.RWMutex
	version int32
	client  string
	server  string
	hooks   Hooks
}

func NewSimpleTrackerFactory(client, server string, hooks Hooks) func() Tracker {
	return func() Tracker {
		return NewSimpleTracker(client, server, hooks)
	}
}

func NewSimpleTracker(client, server string, hooks Hooks) Tracker {
	return &SimpleTracker{
		mu:      &sync.RWMutex{},
		version: VersionDefault,
		client:  client,
		server:  server,
		hooks:   hooks,
	}
}

func (t *SimpleTracker) Negotiation(curSeqID int32, iprot, oprot thrift.TProtocol) error {
	// send
	if err := oprot.WriteMessageBegin(TrackingAPIName, thrift.CALL, curSeqID); err != nil {
		return err
	}
	args := tracking.NewUpgradeArgs_()
	args.AppID = t.client
	args.Version = VersionDefault // XXX: be compatible with thriftpy
	if err := args.Write(oprot); err != nil {
		return err
	}
	if err := oprot.WriteMessageEnd(); err != nil {
		return err
	}
	if err := oprot.Flush(); err != nil {
		return err
	}

	// recv
	method, mTypeID, seqID, err := iprot.ReadMessageBegin()
	if err != nil {
		return err
	}
	if method != TrackingAPIName {
		return thrift.NewTApplicationException(thrift.WRONG_METHOD_NAME,
			"tracker negotiation failed: wrong method name")
	}
	if curSeqID != seqID {
		return thrift.NewTApplicationException(thrift.BAD_SEQUENCE_ID,
			"tracker negotiation failed: out of sequence response")
	}
	if mTypeID == thrift.EXCEPTION {
		err0 := thrift.NewTApplicationException(thrift.UNKNOWN_APPLICATION_EXCEPTION,
			"Unknown Exception")
		var err1, err error
		if err1, err = err0.Read(iprot); err != nil {
			return err
		}
		if err = iprot.ReadMessageEnd(); err != nil {
			return err
		}
		if err0.TypeId() == thrift.UNKNOWN_METHOD { // server does not support tracker, ignore
			return nil
		}
		return err1
	}
	if mTypeID != thrift.REPLY {
		return thrift.NewTApplicationException(thrift.INVALID_MESSAGE_TYPE_EXCEPTION,
			"tracker negotiation failed: invalid message type")
	}
	reply := tracking.NewUpgradeReply()
	if err := reply.Read(iprot); err != nil {
		return err
	}
	if err := iprot.ReadMessageEnd(); err != nil {
		return err
	}
	t.trySetVersion(reply.GetVersion(), VersionRequestHeader)
	return nil
}

func (t *SimpleTracker) TryUpgrade(seqID int32, iprot, oprot thrift.TProtocol) (bool, thrift.TException) {
	args := tracking.NewUpgradeArgs_()
	if err := args.Read(iprot); err != nil {
		iprot.ReadMessageEnd()
		x := thrift.NewTApplicationException(thrift.PROTOCOL_ERROR, err.Error())
		x.Write(oprot)
		oprot.WriteMessageEnd()
		oprot.Flush()
		return false, err
	}
	iprot.ReadMessageEnd()

	t.hooks.onHandshakRequest(args)
	result := tracking.NewUpgradeReply()
	result.Version = t.trySetVersion(args.GetVersion(), VersionRequestHeader)
	if err := oprot.WriteMessageBegin(TrackingAPIName, thrift.REPLY, seqID); err != nil {
		return false, err
	}
	if err := result.Write(oprot); err != nil {
		return false, err
	}
	if err := oprot.WriteMessageEnd(); err != nil {
		return false, err
	}
	if err := oprot.Flush(); err != nil {
		return false, err
	}
	return true, nil
}

func (t *SimpleTracker) trySetVersion(version int32, defaultVersion int32) int32 {
	t.mu.Lock()
	defer t.mu.Unlock()
	if version == VersionDefault {
		version = defaultVersion
	}
	if version < t.version || version > VersionRequestResponseHeader { // XXX: be compatible with thriftpy
		return VersionDefault
	}
	if version == VersionRequestResponseHeader { // XXX: only support request header, be compatible with thriftpy
		version = defaultVersion
	}
	t.version = version
	return version
}

func (t *SimpleTracker) RequestHeaderSupported() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.version >= VersionRequestHeader
}

func (t *SimpleTracker) RequestSeqIDFromCtx(ctx context.Context) (string, string) {
	var reqID, seqID string

	if v, ok := ctx.Value(CtxKeyRequestID).(string); ok {
		reqID = v
	} else {
		reqID = uuid.New().String()
	}

	if v, ok := ctx.Value(CtxKeySequenceID).(string); ok {
		var curSeqID string
		ids := strings.Split(v, ".") // "{precvSeqID}.{...}.{curSeqID}"
		if n := len(ids); n > 1 {
			curSeqID = ids[n-1]
		} else {
			curSeqID = ids[0]
		}
		if cur, err := strconv.Atoi(curSeqID); err == nil {
			seqID = fmt.Sprintf("%v.%d", v, cur+1)
		}
	}
	if seqID == "" {
		seqID = "1"
	}

	return reqID, seqID
}

func (t *SimpleTracker) TryReadRequestHeader(iprot thrift.TProtocol) (context.Context, error) {
	if !t.RequestHeaderSupported() {
		return context.TODO(), nil
	}
	header := tracking.NewRequestHeader()
	if err := header.Read(iprot); err != nil {
		return context.TODO(), err
	}
	ctx := context.Background()
	ctx = context.WithValue(ctx, CtxKeyRequestID, header.GetRequestID())
	ctx = context.WithValue(ctx, CtxKeySequenceID, header.GetSeq())
	ctx = context.WithValue(ctx, CtxKeyRequestMeta, header.GetMeta())
	t.hooks.onRecvHeaderRequest(header)
	return ctx, nil
}

func (t *SimpleTracker) TryWriteRequestHeader(ctx context.Context, oprot thrift.TProtocol) error {
	if !t.RequestHeaderSupported() {
		return nil
	}
	header := tracking.NewRequestHeader()
	if meta, ok := ctx.Value(CtxKeyRequestMeta).(map[string]string); ok {
		header.Meta = make(map[string]string, len(meta))
		for k, v := range meta {
			header.Meta[k] = v
		}
	}
	header.RequestID, header.Seq = t.RequestSeqIDFromCtx(ctx)
	return header.Write(oprot)
}
