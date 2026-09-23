package chanutils

import (
	"cmp"
	"context"
)

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
	responseCh := make(chan rpcChannelResponse[U], 1)
	msg := RpcChannelRequest[T, U]{
		Ctx:        ctx,
		Msg:        value,
		responseCh: responseCh,
	}
	res, err := SendAndRecv(ctx, c.ch, responseCh, msg)
	return res.Ok, cmp.Or(err, res.Err)
}

// Get a handle used to receive requests sent by [RpcChannel.SendAndRecv]
//
// This method is meant to be used by the server side of the RPC pair.
func (c RpcChannel[T, U]) Recv() <-chan RpcChannelRequest[T, U] {
	return c.ch
}

// Close the rpc channel.
//
// After this method is called, the channel returned by [RpcChannel.Recv] will
// be closed.
func (c RpcChannel[T, U]) Close() {
	close(c.ch)
}

// Interface for the server side of an RPC channel
//
// Equivalent to `<-chan`
type RpcChannelServer[T, U any] interface {
	Recv() <-chan RpcChannelRequest[T, U]
}

// Interface for the client side of the RPC RpcChannel
//
// Equivalent to `chan<-`
type RpcChannelClient[T, U any] interface {
	SendAndRecv(ctx context.Context, value T) (U, error)
	Close()
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
	//
	// [RpcChannel.SendAndRecv] creates a buffered response channel, so sends on
	// this channel (or calls to Respond or RespondErr) should not block, so long
	// as only a single response is sent.
	//
	// Generally, users should prefer the Respond and RespondError methods
	// over interacting with this channel directly.
	responseCh chan<- rpcChannelResponse[U]
}

// Send the given value back to the client
//
// As [RpcChannel.SendAndRecv] uses buffered response channels, this method
// (or RespondError) will never block, so long as only one of them is called
// a single time.
func (req RpcChannelRequest[T, U]) Respond(value U) {
	req.responseCh <- rpcChannelResponse[U]{Ok: value}
}

// Send the given error back to the client
//
// As [RpcChannel.SendAndRecv] uses buffered response channels, this method
// (or Respond) will never block, so long as only one of them is called
// a single time.
func (req RpcChannelRequest[T, U]) RespondErr(err error) {
	req.responseCh <- rpcChannelResponse[U]{Err: err}
}

// Close the response channel. This can be used by the server side to indicate
// that no response will be provided.
//
// As elsewhere in Go, is not necessary to close the underlying channel.
func (req RpcChannelRequest[T, U]) Close() {
	close(req.responseCh)
}

type rpcChannelResponse[T any] struct {
	Ok  T
	Err error
}
