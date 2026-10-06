// Command demo is a minimal in-process demonstration of the weighted
// semaphore: acquire, wait, wake on release, TryAcquire and Close.
package main

import (
	"context"
	"fmt"
	"time"

	"weightedsemaphore"
)

func main() {
	ctx := context.Background()
	sem := weightedsemaphore.New(3)
	fmt.Printf("new semaphore: capacity=%d available=%d\n", sem.Capacity(), sem.Available())

	// TryAcquire succeeds when units are free and nobody is waiting.
	fmt.Println("TryAcquire(1):", sem.TryAcquire(1))
	must(sem.Release(1))

	// Acquire all 3 units.
	must(sem.Acquire(ctx, 3))
	fmt.Printf("Acquire(3) ok -> available=%d\n", sem.Available())

	// A goroutine asking for 1 unit blocks in the FIFO wait queue.
	woke := make(chan struct{})
	go func() {
		defer close(woke)
		if err := sem.Acquire(ctx, 1); err != nil {
			fmt.Println("waiter:", err)
			return
		}
		fmt.Println("waiter woke: Acquire(1) ok")
		must(sem.Release(1))
	}()
	time.Sleep(50 * time.Millisecond) // let the waiter queue up
	fmt.Printf("blocked waiter queued: waiters=%d\n", sem.Waiters())

	// TryAcquire never overtakes the queued waiter.
	fmt.Println("TryAcquire(1) while queued:", sem.TryAcquire(1))

	// Release wakes the waiter in FIFO order.
	must(sem.Release(3))
	<-woke
	fmt.Printf("after wake: available=%d waiters=%d\n", sem.Available(), sem.Waiters())

	// Close fails pending and future acquisitions.
	must(sem.Acquire(ctx, 3))
	blocked := make(chan error, 1)
	go func() { blocked <- sem.Acquire(ctx, 1) }()
	time.Sleep(50 * time.Millisecond) // let the waiter queue up
	sem.Close()
	fmt.Println("pending Acquire after Close:", <-blocked)
	fmt.Println("new Acquire after Close:", sem.Acquire(ctx, 1))
	fmt.Println("TryAcquire after Close:", sem.TryAcquire(1))

	// Units already handed out can still be released.
	must(sem.Release(3))
	fmt.Printf("final: available=%d waiters=%d\n", sem.Available(), sem.Waiters())
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
