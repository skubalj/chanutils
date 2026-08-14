package chanutils

import (
	"context"
	"iter"
	"slices"
	"sync"
	"testing"
	"time"
)

func TestPubSub_spsc(t *testing.T) {
	ps := NewPubSub[int]()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	tx := ps.MakePublisherCtx(ctx)
	defer close(tx)

	rx, rxCancel := ps.MakeSubscriberCtx(ctx)
	defer rxCancel()

	// Assert that I can push data in without blocking
	tx <- 1
	tx <- 2
	tx <- 3

	// Assert that I can pull data out in order
	requireEqual(t, 1, <-rx)
	requireEqual(t, 2, <-rx)
	requireEqual(t, 3, <-rx)
}

func TestPubSub_lateStart(t *testing.T) {
	ps := NewPubSub[int]()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	tx := ps.MakePublisherCtx(ctx)
	defer close(tx)

	rx1, rx1Cancel := ps.MakeSubscriberCtx(ctx)
	defer rx1Cancel()

	// Assert that I can push data in without blocking
	tx <- 1
	tx <- 2
	requireEqual(t, 1, <-rx1)
	requireEqual(t, 2, <-rx1)

	rx2, rx2Cancel := ps.MakeSubscriberCtx(ctx)
	defer rx2Cancel()

	tx <- 3
	tx <- 4
	requireEqual(t, 3, <-rx1)
	requireEqual(t, 4, <-rx1)
	requireEqual(t, 3, <-rx2)
	requireEqual(t, 4, <-rx2)
}

func TestPubSub_mpsc(t *testing.T) {
	ps := NewPubSub[int]()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	var txWg sync.WaitGroup
	tx1 := ps.MakePublisherCtx(ctx)
	tx2 := ps.MakePublisherCtx(ctx)
	rx, rxCancel := ps.MakeSubscriberCtx(ctx)
	defer rxCancel()

	txWg.Go(func() {
		defer close(tx1)
		for i := range 5 {
			tx1 <- i
			time.Sleep(25 * time.Millisecond)
		}
	})
	txWg.Go(func() {
		defer close(tx2)
		for i := range 5 {
			tx2 <- i
			time.Sleep(50 * time.Millisecond)
		}
	})

	txWg.Wait()
	expected := []int{0, 0, 1, 1, 2, 2, 3, 3, 4, 4}
	actual := slices.Sorted(iterTake(AsIter(rx), 10))
	rxCancel()
	requireElementsMatch(t, expected, actual)
}

func TestPubSub_spmc(t *testing.T) {
	ps := NewPubSub[int]()

	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	var a, b, c []int
	{
		ch, cancel := ps.MakeSubscriberCtx(ctx)
		defer cancel()
		wg.Go(func() {
			a = slices.Sorted(iterTake(AsIter(ch), 10))
		})
	}
	{
		ch, cancel := ps.MakeSubscriberCtx(ctx)
		defer cancel()
		wg.Go(func() {
			b = slices.Sorted(iterTake(AsIter(ch), 10))
		})
	}
	{
		ch, cancel := ps.MakeSubscriberCtx(ctx)
		defer cancel()
		wg.Go(func() {
			c = slices.Sorted(iterTake(AsIter(ch), 10))
		})
	}

	tx := ps.MakePublisherCtx(ctx)
	defer close(tx)
	for i := range 10 {
		tx <- i
	}
	wg.Wait()

	expected := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	requireElementsMatch(t, expected, a)
	requireElementsMatch(t, expected, b)
	requireElementsMatch(t, expected, c)
}

func Test_iterTake(t *testing.T) {
	arr := []int{1, 2, 3, 4, 5, 6}
	requireElementsMatch(t, []int{}, slices.Collect(iterTake(slices.Values(arr), 0)))
	requireElementsMatch(t, []int{1}, slices.Collect(iterTake(slices.Values(arr), 1)))
	requireElementsMatch(t, []int{1, 2, 3}, slices.Collect(iterTake(slices.Values(arr), 3)))
	requireElementsMatch(t, []int{1, 2, 3, 4, 5, 6}, slices.Collect(iterTake(slices.Values(arr), 6)))
	requireElementsMatch(t, []int{1, 2, 3, 4, 5, 6}, slices.Collect(iterTake(slices.Values(arr), 8)))
}

func Test_iterTake_chan(t *testing.T) {
	ch := make(chan int)
	go func() {
		for i := range 10 {
			ch <- i
		}
	}()

	expected := []int{0, 1, 2, 3, 4, 5}
	actual := slices.Collect(iterTake(AsIter(ch), 6))
	requireElementsMatch(t, expected, actual)
}

func iterTake[T any](i iter.Seq[T], n int) iter.Seq[T] {
	if n == 0 {
		return func(func(T) bool) {}
	}
	return func(yield func(T) bool) {
		count := 0
		for val := range i {
			count++
			if !yield(val) || count == n {
				return
			}
		}
	}
}

func requireEqual[V comparable](t *testing.T, a, b V) {
	t.Helper()
	if a != b {
		t.Errorf("expected %v == %v", a, b)
	}
}

func requireElementsMatch[V comparable](t *testing.T, a, b []V) {
	t.Helper()
	if len(a) != len(b) {
		t.Errorf("expected len(a) == len(b): got %d != %d", len(a), len(b))
	}

	for i := range a {
		requireEqual(t, a[i], b[i])
	}
}
