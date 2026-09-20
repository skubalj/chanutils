package chanutils

import "context"

// A channel abstraction implementing "request and response" pattern.
type RpcChannel[T, U any] struct {
	ch chan RpcChannelRequest[T, U]
}

// Create a new RpcChannel with a buffer of the given size
func NewRpcChannel[T, U any](buffer int) RpcChannel[T, U] {
	return RpcChannel[T, U]{
		ch: make(chan RpcChannelRequest[T, U], buffer),
	}
}

// Send the given value T and wait for a response.
//
// This method is meant to be used by the client side of the RPC pair. If the
// server closes the response channel, then error will be a [ChanClosedError]
func (c RpcChannel[T, U]) SendAndRecv(ctx context.Context, value T) (U, error) {
	responseCh := make(chan U, 1)
	msg := RpcChannelRequest[T, U]{
		Ctx:        ctx,
		Msg:        value,
		responseCh: responseCh,
	}
	return SendAndRecv(ctx, c.ch, responseCh, msg)
}

// Get a handle used to receive requests sent by [RpcChannel.SendAndRecv]
//
// This method is meant to be used by the server side of the RPC pair.
func (c RpcChannel[T, U]) Recv() <-chan RpcChannelRequest[T, U] {
	return c.ch
}

// A request sent across the RpcChannel
type RpcChannelRequest[T, U any] struct {
	Ctx        context.Context
	Msg        T
	responseCh chan<- U
}

// Get the context that was used to make the request (as passed by the client)
//
// This channel can be used by the server side to proactively cancel work that
// was requested, rather than completing it and pushing the result to a buffer
// that will never be read.
func (m RpcChannelRequest[T, U]) RequestCtx() context.Context {
	return m.Ctx
}

// Send the given value back to the client
//
// As [RpcChannel.SendAndRecv] uses buffered response channels, this method
// will never block.
func (m RpcChannelRequest[T, U]) Respond(value U) {
	m.responseCh <- value
}

// Close the response channel. This can be used by the server side to indicate
// that no response will be provided.
func (m RpcChannelRequest[T, U]) Close() {
	close(m.responseCh)
}
