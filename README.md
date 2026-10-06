# weightedsemaphore

A concurrency-safe weighted semaphore for Go, built only on the standard
library. It provides weighted acquire/release with a strict FIFO wait
queue, context cancellation, a fair TryAcquire and Close semantics.

## Features

- Fixed positive capacity; every request takes `n` weighted units (`0 < n <= capacity`)
- `Acquire(ctx, n)` blocks in a FIFO queue; later requests never overtake the queue head
- Canceled waiters are removed safely without affecting others
- `TryAcquire(n)` never blocks and never jumps ahead of queued waiters
- `Release(n)` wakes waiters in FIFO order and rejects over-release
- `Close()` fails pending and future acquisitions with `ErrClosed`; held units stay releasable
- Race-safe queries: `Capacity()`, `Available()`, `Waiters()`

## Layout

- `semaphore.go` - the library (package `weightedsemaphore`)
- `semaphore_test.go` - automated tests
- `cmd/demo/main.go` - minimal CLI demo

## Usage

```go
sem := weightedsemaphore.New(5)

if err := sem.Acquire(ctx, 2); err != nil {
    // context canceled or semaphore closed
}
defer sem.Release(2)

if sem.TryAcquire(1) {
    sem.Release(1)
}
```

Invalid requests (`n <= 0` or `n > capacity`) return an error immediately.
`New` panics on a non-positive capacity. After `Close`, `Acquire` and
`TryAcquire` fail while `Release` of already-held units keeps working.

## Demo

```
go run ./cmd/demo
```

## Test

```
go test -race ./...
```
