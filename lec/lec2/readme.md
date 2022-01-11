## Mutex & Cond

- Cond broadcast/signal before unlock

## Channel

- Channel blocks until both writing and reading side is ready.
- Start a go routine reading/writing the channel, then write/read the channel.

## New and not New

One may construct an object via `obj := Obj {}`, or some constructor function `NewObj()` that returns an address of an object.

`new(Type)` returns an address of an object that has zero value. This serves as a default `New*` type constructor.

It is conventional to wrap a `obj := Obj {}` inside a `New*` type constructor and return its address.

## RPC

semantics under failure:

- at-least-once: the rpc ensures that the procedure is executed at least once by the server. This does not align with most systems.
- at-most-once: the procedure is executed at most once by the server. This is the requirement of most systems.
- exactly-once: quite hard to implement, available under ideal circumstances.

So RPC are not identical to procedure calls.
