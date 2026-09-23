package chanutils

import (
	"errors"
	"fmt"
	"iter"
	"slices"
	"testing"
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
	requireElementsMatch(t, []int{0, 1, 4, 9, 16, 25, 36, 49, 64, 81}, vals)
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
