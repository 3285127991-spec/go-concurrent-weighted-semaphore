// Command demo is a minimal demonstration of the weightedsemaphore package.
package main

import (
	"context"
	"fmt"
	"time"

	"weightedsemaphore"
)

func waitForWaiters(s *weightedsemaphore.Semaphore, want int) {
	for s.Waiters() != want {
		time.Sleep(time.Millisecond)
	}
}

func main() {
	s, err := weightedsemaphore.New(3)
	if err != nil {
		panic(err)
	}
	ctx := context.Background()

	// 1. Acquire 2 of 3 units immediately.
	if err := s.Acquire(ctx, 2); err != nil {
		panic(err)
	}
	fmt.Printf("main: acquired 2, available=%d\n", s.Available())

	// 2. A worker asking for 2 units blocks because only 1 is free.
	workerDone := make(chan error, 1)
	go func() {
		workerDone <- s.Acquire(ctx, 2)
	}()
	waitForWaiters(s, 1)
	fmt.Printf("worker: waiting for 2 units (waiters=%d)\n", s.Waiters())

	// 3. TryAcquire does not block and must not overtake the waiter.
	fmt.Println("main: TryAcquire(1) =", s.TryAcquire(1))

	// 4. Releasing 2 units wakes the queued worker.
	if err := s.Release(2); err != nil {
		panic(err)
	}
	if err := <-workerDone; err != nil {
		panic(err)
	}
	fmt.Printf("worker: woke up and acquired 2, available=%d\n", s.Available())

	// 5. TryAcquire succeeds once resources are free and nobody waits.
	if err := s.Release(2); err != nil {
		panic(err)
	}
	fmt.Println("main: TryAcquire(1) =", s.TryAcquire(1))

	// 6. Close rejects new acquisitions and fails queued waiters.
	blocked := make(chan error, 1)
	go func() {
		blocked <- s.Acquire(ctx, 3) // only 2 units are free now
	}()
	waitForWaiters(s, 1)
	s.Close()
	fmt.Println("waiter after Close:", <-blocked)
	fmt.Println("main: Acquire after Close:", s.Acquire(ctx, 1))
}
