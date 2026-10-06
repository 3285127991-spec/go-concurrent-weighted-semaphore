// Package weightedsemaphore implements a concurrency-safe weighted
// semaphore with FIFO waiters, context cancellation and Close support.
package weightedsemaphore

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// ErrClosed is returned by Acquire and TryAcquire after the semaphore
// has been closed, and is delivered to every pending waiter on Close.
var ErrClosed = errors.New("semaphore: closed")

// waiter represents one pending Acquire call in the FIFO queue.
type waiter struct {
	n       int64
	ch      chan struct{} // closed by the semaphore to wake the waiter
	granted bool          // units were assigned before the wake
	err     error         // wake reason when not granted (ErrClosed)
}

// Semaphore is a weighted semaphore. The zero value is not usable;
// create one with New.
type Semaphore struct {
	mu        sync.Mutex
	capacity  int64
	available int64
	queue     []*waiter
	closed    bool
}

// New returns a Semaphore that hands out up to capacity units.
// It panics if capacity is not positive.
func New(capacity int64) *Semaphore {
	if capacity <= 0 {
		panic(fmt.Sprintf("semaphore: capacity must be positive, got %d", capacity))
	}
	return &Semaphore{capacity: capacity, available: capacity}
}

// Acquire blocks until n units are available, the context is canceled,
// or the semaphore is closed. Waiters are served in FIFO order and a
// later request can never overtake an earlier one.
func (s *Semaphore) Acquire(ctx context.Context, n int64) error {
	if err := s.validate(n); err != nil {
		return err
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	if len(s.queue) == 0 && s.available >= n {
		s.available -= n
		s.mu.Unlock()
		return nil
	}
	w := &waiter{n: n, ch: make(chan struct{})}
	s.queue = append(s.queue, w)
	s.mu.Unlock()

	select {
	case <-w.ch:
		if w.err != nil {
			return w.err
		}
		return nil
	case <-ctx.Done():
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if w.granted {
		// Units were assigned concurrently with the cancellation;
		// the acquisition stands so the units are not leaked.
		return nil
	}
	if w.err != nil {
		return w.err
	}
	s.removeLocked(w)
	s.wakeLocked()
	return ctx.Err()
}

// TryAcquire takes n units without blocking. It succeeds only when no
// earlier waiter is queued and enough units are free. It reports false
// after the semaphore is closed.
func (s *Semaphore) TryAcquire(n int64) bool {
	if err := s.validate(n); err != nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || len(s.queue) > 0 || s.available < n {
		return false
	}
	s.available -= n
	return true
}

// Release returns n units to the semaphore and wakes queued waiters in
// FIFO order as long as the head waiter fits. It reports an error when
// the release would exceed the capacity. Release stays valid after Close.
func (s *Semaphore) Release(n int64) error {
	if err := s.validate(n); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.available+n > s.capacity {
		return fmt.Errorf("semaphore: release of %d units exceeds capacity (%d available of %d)", n, s.available, s.capacity)
	}
	s.available += n
	s.wakeLocked()
	return nil
}

// Capacity returns the total number of units the semaphore manages.
func (s *Semaphore) Capacity() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.capacity
}

// Available returns the number of units currently free.
func (s *Semaphore) Available() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.available
}

// Waiters returns the number of Acquire calls currently queued.
func (s *Semaphore) Waiters() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.queue)
}

// Close rejects future acquisitions and makes every pending Acquire
// return ErrClosed. Units already handed out may still be released.
// Close is idempotent.
func (s *Semaphore) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	for _, w := range s.queue {
		w.err = ErrClosed
		close(w.ch)
	}
	s.queue = nil
}

// validate rejects non-positive requests and requests that can never
// be satisfied because they exceed the capacity.
func (s *Semaphore) validate(n int64) error {
	if n <= 0 {
		return fmt.Errorf("semaphore: n must be positive, got %d", n)
	}
	if n > s.capacity {
		return fmt.Errorf("semaphore: n=%d exceeds capacity %d", n, s.capacity)
	}
	return nil
}

// removeLocked unlinks w from the wait queue. The caller must hold s.mu.
func (s *Semaphore) removeLocked(w *waiter) {
	for i, q := range s.queue {
		if q == w {
			s.queue = append(s.queue[:i], s.queue[i+1:]...)
			return
		}
	}
}

// wakeLocked grants units to queued waiters in FIFO order, stopping at
// the first waiter that does not fit. The caller must hold s.mu.
func (s *Semaphore) wakeLocked() {
	if s.closed {
		return
	}
	for len(s.queue) > 0 {
		w := s.queue[0]
		if w.n > s.available {
			return
		}
		s.available -= w.n
		w.granted = true
		close(w.ch)
		s.queue = s.queue[1:]
	}
}
