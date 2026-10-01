package chanutils

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"
	"sync"
	"testing"
	"time"
)

func Test_ChanClosedError(t *testing.T) {
	wrappedError := fmt.Errorf("wrapped: %w", ChanClosedError)
	if !errors.Is(wrappedError, ChanClosedError) {
		t.Error("ChanClosedError did not match")
	}
}

func Test_AsIter(t *testing.T) {
	ch := make(chan int, 10)
	for i := range 10 {
		ch <- i
	}

	close(ch)
	vals := slices.Collect(square(AsIter(ch)))
	requireSliceEqual(t, []int{0, 1, 4, 9, 16, 25, 36, 49, 64, 81}, vals)
}

func square(x iter.Seq[int]) iter.Seq[int] {
	return func(yield func(int) bool) {
		for i := range x {
			if !yield(i * i) {
				return
			}
		}
	}
}

func TestCondValue(t *testing.T) {
	ctx, cancelTimeout := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancelTimeout()

	runningCtx, cancelRunningCtx := context.WithCancel(ctx)
	defer cancelRunningCtx()

	cv := NewCondValue(0)
	var wg sync.WaitGroup

	seen := make([]int, 0, 8)
	wg.Go(func() {
		cv.Lock()
		defer cv.Unlock()

		seen = append(seen, cv.Get())
		for val := range cv.OnChange(runningCtx) {
			seen = append(seen, val)
		}
		requireNotEqual(t, runningCtx, nil)
	})

	// Ensure that our first thread has time to start and get to its
	// "waiting" position before we start the driver
	time.Sleep(10 * time.Millisecond)

	wg.Go(func() {
		for range 7 {
			cv.Lock()
			cv.Update(cv.Get() + 1)
			cv.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
		cancelRunningCtx()
	})

	wg.Wait()
	requireEqual(t, ctx.Err(), nil)           // Verify that test did not time out
	requireNotEqual(t, runningCtx.Err(), nil) // Verify that test completed

	requireSliceEqual(t, seen, []int{0, 1, 2, 3, 4, 5, 6, 7})
}

func requireEqual[V comparable](t *testing.T, actual, expected V) {
	t.Helper()
	if expected != actual {
		t.Errorf("values are not equal\n\texpected: %v\n\t  actual: %v", expected, actual)
	}
}

func requireNotEqual[V comparable](t *testing.T, actual, expected V) {
	t.Helper()
	if expected == actual {
		t.Errorf("values are equal\n\texpected not: %v\n\t      actual: %v", expected, actual)
	}
}

func requireSliceEqual[V comparable](t *testing.T, actual, expected []V) {
	t.Helper()

	if !slices.Equal(actual, expected) {
		t.Errorf("slices are not equal\n\texpected: %v\n\t  actual: %v", expected, actual)
	}
}
