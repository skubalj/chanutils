# chanutils: utilities for Go channels

Large Go applications tend to be highly concurrent, with many lightweight
goroutines. While the standard library provides implementations of primitives
such as mutexes and condvars, it is most idiomatic to have goroutines
communicate using channels. That's the source of the proverb,

> Don't communicate by sharing memory, share memory by communicating.

These idioms make Go well suited to building actor-model applications.
However, as the number of actors increases, wiring them together can become
tedious. Additionally, while native Go channels are multi-producer,
multi-consumer, they are designed such that each message is seen by a single
consumer. This means that additional goroutines are often needed to implement
a "fanout" to pass messages to multiple consumers.

This package is a collection of abstractions for working with Go channels,
including functions for common patterns, some simple synchronization
primitives, and an implementation of multi-producer, multi-consumer pubsub
and RPC channels.

These abstractions are the result of years of writing similar patterns across
multiple professional projects. Rather than re-writing these same abstractions
for the nth time, I have decided to simply collect them into a single library
where they can be referenced, tested, and maintained independently of any given
employer's codebase. I hope that you find them to be as useful as I do.

## License

This project is available under the terms of the permissive MIT license.
