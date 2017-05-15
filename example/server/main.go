package main

import (
	"context"

	"github.com/apache/thrift/lib/go/thrift"
	tracker "github.com/damnever/thrift-tracker"
	"github.com/damnever/thrift-tracker/example/calculator"
)

type handler struct {
	ctx context.Context
}

func (h *handler) Ping() (bool, error) {
	return true, nil
}

func (h *handler) Add(num1, num2 int32) (int32, error) {
	return num1 + num2, nil
}

func (h *handler) Ctx() context.Context {
	return h.ctx
}

func main() {
	ctx := context.Background()
	ctx = context.WithValue(ctx, tracker.CtxKeyResponseMeta, map[string]string{"server": "test"})
	h := &handler{ctx: ctx}
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
