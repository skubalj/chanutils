// Functions and abstractions over Go channels.
package chanutils

import (
	"context"
	"errors"
	"iter"
	"sync"
	"sync/atomic"
)

// Custom error type to indicate that the channel was closed
var ChanClosedError = errors.New("channel closed")

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
			return zero, ChanClosedError
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
				return ChanClosedError
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
//
// The channel must be closed, or the yield function must return false
// (such as by breaking or returning from a for loop driving the iterator)
func AsIter[T any](ch <-chan T) iter.Seq[T] {
	return func(yield func(T) bool) {
		for x := range ch {
			if !yield(x) {
				return
			}
		}
	}
}

// Simple adapter for the [iter.Seq] interface that will stop returning
// elements when the context is cancelled or the channel is closed.
func AsIterCtx[T any](ctx context.Context, ch <-chan T) iter.Seq[T] {
	return func(yield func(T) bool) {
		for {
			select {
			case <-ctx.Done():
				return
			case x, ok := <-ch:
				if !ok || !yield(x) {
					return
				}
			}
		}
	}
}

// Pipe values from the source iterator into the channel. This function blocks
// until the iterator ends or the context is closed.
func PipeIter[T any](ctx context.Context, src iter.Seq[T], sink chan<- T) {
	for val := range src {
		select {
		case <-ctx.Done():
			return
		case sink <- val:
		}
	}
}

// Return a channel that will yield each element from the iterator. The
// channel will be closed when the iterator ends or the context is closed.
func FromIter[T any](ctx context.Context, itr iter.Seq[T]) <-chan T {
	ch := make(chan T)
	go func() {
		defer close(ch)
		PipeIter(ctx, itr, ch)
	}()
	return ch
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

// Perform the given callback while holding a permit
func (s Semaphore) WithPermit(fn func()) {
	s.Acquire()
	defer s.Release()
	fn()
}

// Perform the given callback while holding a permit, returning an error if the
// context is cancelled before a permit could be acquired.
func (s Semaphore) WithPermitCtx(ctx context.Context, fn func()) error {
	err := s.AcquireCtx(ctx)
	if err != nil {
		return err
	}
	defer s.Release()
	fn()
	return nil
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
	p    atomic.Pointer[chan struct{}]
	once sync.Once
}

func NewGate() *Gate {
	g := new(Gate)
	g.init()
	return g
}

func (g *Gate) init() {
	g.once.Do(func() {
		ch := make(chan struct{})
		g.p.Store(&ch)
	})
}

// Get a channel that will produce a value when released
func (g *Gate) Waiter() <-chan struct{} {
	g.init()
	return *g.p.Load()
}

// Release a single consumer that is waiting
//
// Returns false if no consumers are currently waiting
func (g *Gate) ReleaseOne() bool {
	g.init()
	select {
	case (*g.p.Load()) <- struct{}{}:
		return true
	default:
		return false
	}
}

// Release all consumers that are currently waiting and immediately rotate the
// underlying channel.
func (g *Gate) ReleaseAndReset() {
	g.init()
	close(g.Reset())
}

// Release all consumers, but leave the closed channel in place.
//
// You must call Reset before using any other Release function or you will
// cause a panic due to a double-closed channel.
//
// This is useful when a Gate is used to indicate that something "is happening",
// as opposed to indicating that something "has happened".
func (g *Gate) ReleaseNoReset() {
	g.init()
	close(*g.p.Load())
}

// Rotate the underlying channel used for notification, returning the old
// channel.
// 
// Calling Reset without closing the channel can leak deadlocked goroutines.
// This method is intended to be called after ReleaseNoReset.
func (g *Gate) Reset() chan struct{} {
	g.init()
	ch := make(chan struct{})
	return *g.p.Swap(&ch)
}

// An observable value; conceptually a sync.Cond that owns the data it is
// guarding, backed by a [Gate].
//
// The major advantage over sync.Cond is that waits take contexts and return
// immediately when the context is cancelled. However, note that users must
// still manually lock and unlock the CondValue around the critical section.
type CondValue[T any] struct {
	// channel based notifier
	notifier *Gate
	// Held while the value is inspected or modified
	lock  sync.Mutex
	value T
}

// Create a new CondValue with the given initial value
func NewCondValue[T any](init T) *CondValue[T] {
	return &CondValue[T]{
		notifier: NewGate(),
		value:    init,
	}
}

// Lock the internal mutex so that the value doesn't change
func (c *CondValue[T]) Lock() { c.lock.Lock() }

// Unlock the internal mutex so that the value can be changed
func (c *CondValue[T]) Unlock() { c.lock.Unlock() }

// Get the current value from the condvar.
//
// Note that this function does not lock the mutex. As with other methods on
// this type, Lock must be called before this function.
func (c *CondValue[T]) Get() T { return c.value }

// Wait for an update to be signaled, or until the context expires and return
// the current value.
//
// The mutex must be locked with Lock before calling this function. This
// function will unlock the mutex while it waits, but will always lock the
// mutex before it returns.
func (c *CondValue[T]) Wait(ctx context.Context) (T, error) {
	// Note that we get the waiter before we unlock. This ensures that we don't
	// miss any updates that happen between unlocking and entering the select
	waiter := c.notifier.Waiter()
	c.lock.Unlock()

	select {
	case <-ctx.Done():
		c.lock.Lock()
		return c.value, ctx.Err()
	case <-waiter:
		// We can't defer this lock because we must prevent concurrent access
		// to it when we copy it into the return variable.
		c.lock.Lock()
		return c.value, nil
	}
}

// Return the first value seen for which the predicate evaluates to true. If
// the initial value passes the predicate, then it will be returned immediately
// without waiting. Otherwise, we will repeatedly wait for changes and return
// the first passing value.
//
// Note that the same caveats apply here as with OnChange: it is not guaranteed
// that every state change will be inspected. However, the value that is
// returned is guaranteed to have been tested with the predicate.
//
// The mutex must be locked with Lock before calling this function. This
// function will unlock the mutex while it waits, but will always lock the
// mutex before it returns.
func (c *CondValue[T]) WaitUntil(ctx context.Context, predicate func(T) bool) (value T, err error) {
	if initialValue := c.Get(); predicate(initialValue) {
		return initialValue, nil
	}

	for candidate := range c.OnChange(ctx) {
		if predicate(candidate) {
			return candidate, nil
		}
	}
	return value, ctx.Err()
}

// Set the value stored inside this CondVar and notify all waiters.
//
// The mutex must be locked with Lock before calling this function.
func (c *CondValue[T]) Store(value T) {
	defer c.notifier.ReleaseAndReset()
	c.value = value
}

// Use the given callback to update the value
func (c *CondValue[T]) Update(cb func(*T)) {
	val := c.Get()
	cb(&val)
	c.Store(val)
}

// Return an iterator that yields the value when the CondVar's notifier is triggered.
//
// Note that it is not guaranteed that each independent state will be yielded.
// It is possible for other waiting threads to modify the value before it is
// seen by this thread. If you need multiple consumers to see every value that
// occurs, you may want to consider using an [Observable].
//
// The mutex must be locked with Lock before calling this function. This
// function will unlock the mutex while it waits, but will always lock the
// mutex before it returns.
func (c *CondValue[T]) OnChange(ctx context.Context) iter.Seq[T] {
	return func(yield func(T) bool) {
		for {
			value, err := c.Wait(ctx)
			if err != nil || !yield(value) {
				return
			}
		}
	}
}
