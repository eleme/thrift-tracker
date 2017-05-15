package main

import (
	"context"
	"fmt"

	"github.com/apache/thrift/lib/go/thrift"
	tracker "github.com/damnever/thrift-tracker"
	"github.com/damnever/thrift-tracker/example/calculator"
)

func main() {
	transportFactory := thrift.NewTBufferedTransportFactory(4096)
	socket, err := thrift.NewTSocket(":8010")
	if err != nil {
		panic(err)
	}
	if err = socket.Open(); err != nil {
		panic(err)
	}
	transport := transportFactory.GetTransport(socket)
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()

	t := tracker.NewSimpleTracker("client.test", "server.test", tracker.DefaultHooks)
	client, err := calculator.NewCalculatorServiceClientFactory(t, transport, protocolFactory)
	if err != nil {
		fmt.Println("err", err)
		panic(err)
	}

	ctx := context.Background()
	ctx = context.WithValue(ctx, tracker.CtxKeyRequestMeta, map[string]string{"client": "ping"})
	if _, err = client.Ping(ctx); err != nil {
		panic(err)
	}
	ctx = context.WithValue(ctx, tracker.CtxKeyRequestMeta, map[string]string{"client": "add"})
	if _, err = client.Add(ctx, 1, 2); err != nil {
		panic(err)
	}
}
