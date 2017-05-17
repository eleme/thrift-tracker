package main

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	tracker "github.com/eleme/thrift-tracker"
	"github.com/eleme/thrift-tracker/example/calculator"
)

const (
	ServerA     string = "server-A"
	ServerB     string = "server-B"
	ServerC     string = "server-C"
	ServerBAddr string = ":8010"
	ServerCAddr string = ":8020"
)

func ppCtx(name string, ctx context.Context) {
	fmt.Printf("server(%v):\n", name)
	fmt.Printf("  - RequestID: %#+v\n", ctx.Value(tracker.CtxKeyRequestID))
	fmt.Printf("  - SequenceID: %#+v\n", ctx.Value(tracker.CtxKeySequenceID))
	fmt.Printf("  - Meta: %#+v\n", ctx.Value(tracker.CtxKeyRequestMeta))
}

// ServerB's handler
type handlerB struct {
	client *calculator.CalculatorServiceClient
}

func (h *handlerB) Ping(ctx context.Context) (bool, error) {
	ppCtx(ServerB, ctx)
	meta := map[string]string{"clientB": "ping"}
	if origin, ok := ctx.Value(tracker.CtxKeyRequestMeta).(map[string]string); ok {
		for k, v := range origin {
			meta[k] = v
		}
	}
	ctx = context.WithValue(ctx, tracker.CtxKeyRequestMeta, meta)
	h.client.Ping(ctx)
	return true, nil
}

func (h *handlerB) Add(ctx context.Context, num1, num2 int32) (int32, error) {
	ppCtx(ServerB, ctx)
	meta := map[string]string{"clientB": "add"}
	if origin, ok := ctx.Value(tracker.CtxKeyRequestMeta).(map[string]string); ok {
		for k, v := range origin {
			meta[k] = v
		}
	}
	ctx = context.WithValue(ctx, tracker.CtxKeyRequestMeta, meta)
	h.client.Add(ctx, num1+1, num2+2)
	return num1 + num2, nil
}

// ServerC's handler
type handlerC struct{}

func (h *handlerC) Ping(ctx context.Context) (bool, error) {
	ppCtx(ServerC, ctx)
	return true, nil
}

func (h *handlerC) Add(ctx context.Context, num1, num2 int32) (int32, error) {
	ppCtx(ServerC, ctx)
	return num1 + num2, nil
}

func NewClient(addr, cliName, srvName string) (*calculator.CalculatorServiceClient, error) {
	transportFactory := thrift.NewTBufferedTransportFactory(4096)
	socket, err := thrift.NewTSocket(addr)
	if err != nil {
		return nil, err
	}
	if err = socket.Open(); err != nil {
		return nil, err
	}
	transport := transportFactory.GetTransport(socket)
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()

	ttracker := tracker.NewSimpleTracker(cliName, srvName, tracker.DefaultHooks)
	client, err := calculator.NewCalculatorServiceClientFactory(ttracker, transport, protocolFactory)
	if err != nil {
		return nil, err
	}
	return client, nil
}

func RunServer(addr, cliName, srvName string, handler calculator.CalculatorService) {
	ttracker := tracker.NewSimpleTracker(cliName, srvName, tracker.DefaultHooks)
	processor := calculator.NewCalculatorServiceProcessor(ttracker, handler)

	transport, err := thrift.NewTServerSocket(addr)
	must(err)
	transportFactory := thrift.NewTBufferedTransportFactory(4096)
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	server := thrift.NewTSimpleServer4(
		processor,
		transport,
		transportFactory,
		protocolFactory,
	)
	must(server.Serve())
}

func main() {
	go func() {
		RunServer(ServerCAddr, ServerC, ServerC, &handlerC{})
	}()
	time.Sleep(10 * time.Millisecond) // wait server C started

	clientB, err := NewClient(ServerCAddr, ServerB, ServerC)
	must(err)
	go func() {
		RunServer(ServerBAddr, ServerB, ServerC, &handlerB{client: clientB})
	}()
	time.Sleep(10 * time.Millisecond) // wait server B started

	clientA, err := NewClient(ServerBAddr, ServerA, ServerB)
	must(err)
	ctx := context.Background()
	ctx = context.WithValue(ctx, tracker.CtxKeyRequestMeta, map[string]string{"clientA": "ping"})
	_, err = clientA.Ping(ctx)
	must(err)
	ctx = context.WithValue(ctx, tracker.CtxKeyRequestMeta, map[string]string{"clientA": "add"})
	_, err = clientA.Add(ctx, 1, 2)
	must(err)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
