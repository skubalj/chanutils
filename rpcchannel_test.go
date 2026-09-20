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
	if err != nil {
		t.Errorf("error: %v", err)
	} else if res != 1 {
		t.Errorf("expected 1, got %d", res)
	}

	res, err = ch.SendAndRecv(ctx, 2)
	if err != nil {
		t.Errorf("error: %v", err)
	} else if res != 4 {
		t.Errorf("expected 4, got %d", res)
	}

	res, err = ch.SendAndRecv(ctx, 3)
	if err != nil {
		t.Errorf("error: %v", err)
	} else if res != 9 {
		t.Errorf("expected 9, got %d", res)
	}

	cancel()
	wg.Wait()
}
