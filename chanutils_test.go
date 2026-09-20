package chanutils

import (
	"errors"
	"fmt"
	"testing"
)

func Test_ChanClosedError(t *testing.T) {
	wrappedError := fmt.Errorf("wrapped: %w", ChanClosedError{})
	if !errors.Is(wrappedError, ChanClosedError{}) {
		t.Error("ChanClosedError did not match")
	}
}
