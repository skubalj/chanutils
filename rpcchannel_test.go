package chanutils

import (
	"context"
	"sync"
	"testing"
)

func Test_RpcChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := NewRpcChannel[int, int](0)
	var wg sync.WaitGroup

	wg.Go(func() {
		for {
			select {
			case <-ctx.Done():
				return
			case req := <-ch.Recv():
				req.Respond(req.Msg * req.Msg)
			}
		}
	})

	res, err := ch.SendAndRecv(ctx, 1)
	requireEqual(t, err, nil)
	requireEqual(t, res, 1)

	res, err = ch.SendAndRecv(ctx, 2)
	requireEqual(t, err, nil)
	requireEqual(t, res, 4)

	res, err = ch.SendAndRecv(ctx, 3)
	requireEqual(t, err, nil)
	requireEqual(t, res, 9)

	cancel()
	wg.Wait()
}
