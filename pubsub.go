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

	if ps.end == nil {
		ps.end = zeroCons[T]()
	}

	ps.end.next = newCons(value)
	close(ps.end.wait)
	ps.end = ps.end.next
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

	ptr := ps.end
	return func(yield func(T) bool) {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ptr.wait:
				ptr = ptr.next
				if !yield(ptr.value) {
					return
				}
			}
		}
	}
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

func newCons[T any](value T) *cons[T] {
	return &cons[T]{
		value: value,
		wait:  make(chan struct{}),
		next:  nil,
	}
}
