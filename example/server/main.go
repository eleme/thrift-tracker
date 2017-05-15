package main

import (
	"context"
	"fmt"

	"github.com/apache/thrift/lib/go/thrift"
	tracker "github.com/damnever/thrift-tracker"
	"github.com/damnever/thrift-tracker/example/calculator"
)

type handler struct{}

func ppCtx(ctx context.Context) {
	fmt.Printf("- RequestID: %#+v\n", ctx.Value(tracker.CtxKeyRequestID))
	fmt.Printf("- SequenceID: %#+v\n", ctx.Value(tracker.CtxKeySequenceID))
	fmt.Printf("- Meta: %#+v\n", ctx.Value(tracker.CtxKeyRequestMeta))
}

func (h *handler) Ping(ctx context.Context) (bool, error) {
	ppCtx(ctx)
	return true, nil
}

func (h *handler) Add(ctx context.Context, num1, num2 int32) (int32, error) {
	ppCtx(ctx)
	return num1 + num2, nil
}

func main() {
	h := &handler{}
	t := tracker.NewSimpleTracker("client.test", "server.test", tracker.DefaultHooks)
	processor := calculator.NewCalculatorServiceProcessor(t, h)

	transport, err := thrift.NewTServerSocket(":8010")
	if err != nil {
		panic(err)
	}

	transportFactory := thrift.NewTBufferedTransportFactory(4096)
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	server := thrift.NewTSimpleServer4(
		processor,
		transport,
		transportFactory,
		protocolFactory,
	)
	if err = server.Serve(); err != nil {
		panic(err)
	}

}
