package chanutils

import (
	"context"
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

func NewPubSub[T any]() *PubSub[T] {
	var zero T
	initialValue := newCons(zero)
	return &PubSub[T]{end: initialValue}
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
			ps.insert(value)
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
				ps.insert(value)
			}
		}
	}()
}

func (ps *PubSub[T]) insert(value T) {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	ps.end.next = newCons(value)
	close(ps.end.wait)
	ps.end = ps.end.next
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
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	sink := make(chan T)
	ctx, cancel := context.WithCancel(ctx)

	go func(ptr *cons[T]) {
		defer cancel()
		defer close(sink)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ptr.wait:
				ptr = ptr.next
				sink <- ptr.value
			}
		}
	}(ps.end)

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
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	ctx, cancel := context.WithCancel(ctx)
	go func(ptr *cons[T]) {
		defer cancel()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ptr.wait:
				ptr = ptr.next
				sink <- ptr.value
			}
		}
	}(ps.end)

	return CancelFunc(cancel)
}

// Function to release resources associated with a PubSub resource
type CancelFunc func()

type cons[T any] struct {
	value T
	wait  chan struct{}
	next  *cons[T]
}

func newCons[T any](value T) *cons[T] {
	return &cons[T]{
		value: value,
		wait:  make(chan struct{}),
		next:  nil,
	}
}
