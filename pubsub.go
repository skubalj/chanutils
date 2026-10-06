package chanutils

import (
	"context"
	"iter"
	"sync"
)

// A multi-producer, multi-consumer channel where each consumer receives every
// value from each producer.
//
// This struct is the "core" and contains the actual data buffer. You can
// acquire publisher and subscriber channels by using the `Publisher` and
// `Subscriber` methods on this struct.
//
// Internally, this subscriber is backed by a singly linked list. It is
// important to properly dispose of the provided channels to prevent leaking
// resources
type PubSub[T any] struct {
	mtx sync.Mutex
	end *cons[T]
}

// Equivalent to new(PubSub[T])
func NewPubSub[T any]() *PubSub[T] {
	return &PubSub[T]{end: zeroCons[T]()}
}

// Get a channel that can be used to publish messages to all subscribers
//
// You must close the channel when you are done.
func (ps *PubSub[T]) MakePublisher() chan<- T {
	source := make(chan T)
	ps.RegisterPublisher(source)
	return source
}

// Like [PubSub.MakePublisher], but cleans up resources automatically if the
// context is cancelled. Note that if the context is cancelled, sending
// values on the channel will block indefinitely.
func (ps *PubSub[T]) MakePublisherCtx(ctx context.Context) chan<- T {
	source := make(chan T)
	ps.RegisterPublisherCtx(ctx, source)
	return source
}

// Like [PubSub.MakePublisher], but you brought your own channel.
//
// You must cancel the source channel to free the system resources
func (ps *PubSub[T]) RegisterPublisher(source <-chan T) {
	go func() {
		for value := range source {
			ps.Send(value)
		}
	}()
}

// Like [PubSub.MakePublisherCtx], but you brought your own channel.
//
// System resources will be freed when either the source channel is closed, or
// the context is cancelled.
func (ps *PubSub[T]) RegisterPublisherCtx(ctx context.Context, source <-chan T) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case value, ok := <-source:
				if !ok {
					return
				}
				ps.Send(value)
			}
		}
	}()
}

// Directly send a value to all subscribers without creating a dedicated
// publisher channel
func (ps *PubSub[T]) Send(value T) {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	ps.end = ps.end.Append(value)
}

func (ps *PubSub[T]) Iter() iter.Seq[T] {
	return ps.IterCtx(context.Background())
}

func (ps *PubSub[T]) IterCtx(ctx context.Context) iter.Seq[T] {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	if ps.end == nil {
		ps.end = zeroCons[T]()
	}

	return ps.end.Iter(ctx)
}

// Get a channel that will return every value produced by any of the publishers.
//
// To ensure that resources get released, you must call the CancelFunc.
func (ps *PubSub[T]) MakeSubscriber() (<-chan T, CancelFunc) {
	return ps.MakeSubscriberCtx(context.Background())
}

// Like [PubSub.MakeSubscriber], but releases resources and closes the returned
// channel automatically when the context is cancelled.
//
// While the context will clean up resources, it is still advisable to call
// the cancel function proactively when you are done using the channel.
func (ps *PubSub[T]) MakeSubscriberCtx(ctx context.Context) (<-chan T, CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	itr := ps.IterCtx(ctx)
	sink := make(chan T)

	go func() {
		defer close(sink)
		defer cancel()
		PipeIter(ctx, itr, sink)
	}()

	return sink, CancelFunc(cancel)
}

// Like [PubSub.MakeSubscriber], but you brought your own channel.
//
// The provided channel will not be closed automatically when the cancel
// function is called.
func (ps *PubSub[T]) RegisterSubscriber(sink chan<- T) CancelFunc {
	return ps.RegisterSubscriberCtx(context.Background(), sink)
}

// Like [PubSub.MakeSubscriberCtx], but you brought your own channel.
//
// The provided channel will not be closed automatically when the cancel
// function is called.
func (ps *PubSub[T]) RegisterSubscriberCtx(ctx context.Context, sink chan<- T) CancelFunc {
	ctx, cancel := context.WithCancel(ctx)
	itr := ps.IterCtx(ctx)

	go func() {
		defer cancel()
		PipeIter(ctx, itr, sink)
	}()

	return CancelFunc(cancel)
}

// An observable value; conceptually a sync.Cond that owns its data and ensures
// that every state can be seen.
//
// Internally, this Cond is backed by a PubSub channel. This differs from
// CondValue in that it ensures that the OnChange and WaitUntil methods see
// every value. However, this type gives less control over the lock.
type Observable[T any] PubSub[T]

// Create a new CondPubSub with the given initial value
func NewObservable[T any](init T) *Observable[T] {
	c := new(Observable[T])
	c.Store(init)
	return c
}

func (c *Observable[T]) getHead() *cons[T] {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	return c.end
}

// Get the most recent value from the Cond
func (c *Observable[T]) Get() T {
	return c.getHead().value
}

// Wait for an update to be signaled, or until the context expires and return
// the current value.
func (c *Observable[T]) Wait(ctx context.Context) (T, error) {
	end := c.getHead()

	select {
	case <-ctx.Done():
		return end.value, ctx.Err()
	case <-end.wait:
		return end.next.value, nil
	}
}

// Return the first value seen for which the predicate evaluates to true. If
// the initial value passes the predicate, then it will be returned immediately
// without waiting. Otherwise, we will repeatedly wait for changes and return
// the first passing value.
func (c *Observable[T]) WaitUntil(ctx context.Context, predicate func(T) bool) (value T, err error) {
	end := c.getHead()

	if predicate(end.value) {
		return end.value, nil
	}

	for candidate := range end.Iter(ctx) {
		if predicate(candidate) {
			return candidate, nil
		}
	}
	return value, ctx.Err()
}

// Helper function to update the value stored inside this CondVar and notify all waiters.
func (c *Observable[T]) Store(value T) {
	(*PubSub[T])(c).Send(value)
}

// Use the given callback to update the value
func (c *Observable[T]) Update(cb func(*T)) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	val := c.end.value
	cb(&val)
	c.end = c.end.Append(val)
}

// Return an iterator that yields each state that occurs in this Cond
func (c *Observable[T]) OnChange(ctx context.Context) iter.Seq[T] {
	return (*PubSub[T])(c).IterCtx(ctx)
}

// Function to release resources associated with a PubSub resource
type CancelFunc func()

type cons[T any] struct {
	value T
	wait  chan struct{}
	next  *cons[T]
}

func zeroCons[T any]() *cons[T] {
	return &cons[T]{
		wait: make(chan struct{}),
		next: nil,
	}
}

func (c *cons[T]) Append(value T) *cons[T] {
	next := &cons[T]{
		value: value,
		wait:  make(chan struct{}),
		next:  nil,
	}

	if c != nil {
		c.next = next
		close(c.wait)
	}

	return next
}

func (c *cons[T]) Iter(ctx context.Context) iter.Seq[T] {
	return func(yield func(T) bool) {
		for {
			select {
			case <-ctx.Done():
				return
			case <-c.wait:
				c = c.next
				if !yield(c.value) {
					return
				}
			}
		}
	}
}
