package chanutils

import "context"

// A channel abstraction implementing a simple "request and response" pattern.
//
// There are two sides to an RPC pattern: the "client" makes requests, and the
// "server" responds to them. This pattern can be used with Go's lightweight
// goroutines to build networks of actors that communicate with each other.
// Think microservices, but within a single process.
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
// server closes the response channel, then the returned error will be a
// [ChanClosedError].
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
	// Context specified by the client.
	//
	// Watching this context can tell the server when the client has cancelled
	// their request, allowing the server to terminate work early.
	Ctx context.Context

	// The actual request data
	Msg T

	// Response channel used to send a reply
	responseCh chan<- U
}

// Send the given value back to the client
//
// As [RpcChannel.SendAndRecv] uses buffered response channels, this method
// will never block.
func (req RpcChannelRequest[T, U]) Respond(value U) {
	req.responseCh <- value
}

// Close the response channel. This can be used by the server side to indicate
// that no response will be provided.
func (req RpcChannelRequest[T, U]) Close() {
	close(req.responseCh)
}
