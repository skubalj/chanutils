// Functions and abstractions over Go channels.
package chanutils

import (
	"context"
	"iter"
	"sync/atomic"
)

// Custom error type to indicate that the channel was closed
type ChanClosedError struct{}

func (e ChanClosedError) Error() string { return "channel closed" }

// Send a value to tx, then wait for a response from rx
//
// For general-purpose RPC routines, you may find [RpcChannel] to be more
// ergonomic. However, this lower-level function still comes in handy when
// additional control is desired.
func SendAndRecv[T, U any](ctx context.Context, tx chan<- T, rx <-chan U, value T) (U, error) {
	var zero U
	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case tx <- value:
	}

	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case v, ok := <-rx:
		if !ok {
			return zero, ChanClosedError{}
		}
		return v, nil
	}
}

// Forward each element read from rx into tx.
//
// This function will block on rx, so you probably want to call it in its own goroutine.
// Returns [ChanClosedError] if rx is closed. Else, the error that closed the context.
//
// # Example
//
//	rx := mySvc1.OutputCh()
//	tx := mySvc2.InputCh()
//	go chutils.Forward(ctx, rx, tx)
func Forward[T any](ctx context.Context, rx <-chan T, tx chan<- T) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case val, ok := <-rx:
			if !ok {
				return ChanClosedError{}
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case tx <- val:
			}
		}
	}
}

// Removes all values currently in the channel
//
// Note that in a multithreaded context, this function is inherently racey:
// a different thread could drop new messages into the channel while this
// function returns, so the channel is NOT guaranteed to be empty after
// Drain returns.
func Drain[T any](rx <-chan T) {
	for {
		select {
		case <-rx:
		default:
			return
		}
	}
}

// Simple adapter to the [iter.Seq] interface
func AsIter[T any](ch <-chan T) iter.Seq[T] {
	return func(yield func(T) bool) {
		for x := range ch {
			if !yield(x) {
				return
			}
		}
	}
}

// A simple synchronization primitive that allows only a certain number of
// consumers access to a critical section at a time.
type Semaphore chan struct{}

// Create a new Semaphore with the specified number of permits
func NewSemaphore(numPermits int) Semaphore {
	return make(Semaphore, numPermits)
}

// Acquire a permit from the semaphore
//
// This function will block forever if required. You may want to prefer
// [Semaphore.AcquireCtx], so that this method does not deadlock forever if
// the semaphore has been put into an inconsistent state (such as by forgetting
// to call [Semaphore.Release] after Acquire).
func (s Semaphore) Acquire() {
	s <- struct{}{}
}

// Attempt to acquire a permit if one is available
//
// Returns true if a permit was acquired.
func (s Semaphore) TryAcquire() bool {
	select {
	case s <- struct{}{}:
		return true
	default:
		return false
	}
}

// Acquire a permit from the semaphore, unless cancelled by the provided context.
//
// Returns nil if a permit has been acquired, or the error from the context, if
// cancelled
func (s Semaphore) AcquireCtx(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s <- struct{}{}:
		return nil
	}
}

// Release a single permit from the semaphore
func (s Semaphore) Release() {
	<-s
}

// Release all permits from the semaphore
func (s Semaphore) ReleaseAll() {
	Drain(s)
}

// A synchronization primitive that allows threads to wait for some event to
// happen, then get notified when it does. (A condvar without a value)
//
// This struct implements a scheme described in the documentation for [sync.Cond]:
//
//	For many simple use cases, users will be better off using channels than a
//	Cond (Broadcast corresponds to closing a channel, and Signal corresponds to
//	sending on a channel).
//
// This scheme has advantages over using a [sync.Locker] based condvar, namely
// that multiple goroutines can be awakened at the same time without creating
// lock contention.
//
// Gate differs from the trivial channel implementation by automatically
// resetting after the channel is closed. This allows a single gate to be used
// to announce changes to all listeners multiple times, but it means that some
// care must be taken when waiting for changes: because no lock is held while
// re-evaluating the condition, it is IMPERATIVE that you reset your Waiter
// channel BEFORE you re-evaluate the condition to prevent missing changes.
//
// # Example
//
// Note that we reset the waiter first. This ensures that no changes can
// occur between loading the value and waiting again. In this order, if
// a change were to occur between the reset of the waiter and the load,
// the worst that would happen is that we would evaluate the same value
// twice. Note that this does NOT guarantee that each value will be seen
// at least once.
// 
//	func WaitUntil(ctx context.Context, value *atomic.Int32, pred func(int32) bool) int32 {
//		waiter := g.Waiter()
//		for {
//			select {
//			case <-ctx.Done():
//				return 0
//			case <-waiter:
//				waiter = g.Waiter()
//				v := value.Load()
//				if pred(v) {
//					return v
//				}
//			}
//		}
//	}
type Gate struct {
	p atomic.Pointer[chan struct{}]
}

func NewGate() *Gate {
	ch := make(chan struct{})
	g := new(Gate)
	g.p.Store(&ch)
	return g
}

// Get a channel that will produce a value when released
func (g *Gate) Waiter() <-chan struct{} {
	return *g.p.Load()
}

// Release a single consumer that is waiting
func (g *Gate) ReleaseOne() {
	(*g.p.Load()) <- struct{}{}
}

// Release all consumers that are currently waiting
func (g *Gate) ReleaseAll() {
	ch := make(chan struct{})
	old := g.p.Swap(&ch)
	close(*old)
}
