// Package weightedsemaphore provides a concurrency-safe weighted semaphore
// with strict FIFO waiters, context cancellation, and Close semantics.
package weightedsemaphore

import (
	"container/list"
	"context"
	"errors"
	"sync"
)

var (
	// ErrClosed is returned by Acquire when the semaphore has been closed.
	ErrClosed = errors.New("weightedsemaphore: semaphore is closed")
	// ErrInvalidN is returned when n is not positive or exceeds capacity.
	ErrInvalidN = errors.New("weightedsemaphore: n must be positive and <= capacity")
	// ErrOverRelease is returned when Release would return more units than
	// are currently acquired.
	ErrOverRelease = errors.New("weightedsemaphore: release exceeds acquired units")
)

// Semaphore is a weighted semaphore with a strict FIFO wait queue.
// The zero value is not usable; create one with New.
type Semaphore struct {
	mu        sync.Mutex
	capacity  int64
	available int64
	waiters   list.List // FIFO queue of *waiter
	closed    bool
}

// waiter represents one blocked Acquire call.
type waiter struct {
	n    int64
	ch   chan struct{} // closed exactly once when the waiter is dequeued
	err  error         // nil means granted; set before ch is closed
	elem *list.Element // non-nil while still queued
}

// New creates a Semaphore with the given positive capacity.
func New(capacity int64) (*Semaphore, error) {
	if capacity <= 0 {
		return nil, ErrInvalidN
	}
	return &Semaphore{capacity: capacity, available: capacity}, nil
}

// Acquire blocks until n units are available, the context is canceled, or the
// semaphore is closed. Requests are served in strict FIFO order: a later
// request never overtakes an earlier queued one.
func (s *Semaphore) Acquire(ctx context.Context, n int64) error {
	if n <= 0 || n > s.capacity {
		return ErrInvalidN
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	if s.waiters.Len() == 0 && s.available >= n {
		s.available -= n
		s.mu.Unlock()
		return nil
	}
	w := &waiter{n: n, ch: make(chan struct{})}
	w.elem = s.waiters.PushBack(w)
	s.mu.Unlock()

	select {
	case <-w.ch:
		return w.err
	case <-ctx.Done():
		s.mu.Lock()
		if w.elem != nil {
			// Still queued: remove safely. No resources change hands, so
			// no other waiter needs to be woken.
			s.waiters.Remove(w.elem)
			w.elem = nil
			s.mu.Unlock()
			return ctx.Err()
		}
		s.mu.Unlock()
		// Already dequeued: the outcome (granted or closed) is decided.
		<-w.ch
		return w.err
	}
}

// TryAcquire acquires n units without blocking. It succeeds only if no
// earlier waiter is queued and enough units are available. It returns false
// for invalid n or after Close.
func (s *Semaphore) TryAcquire(n int64) bool {
	if n <= 0 || n > s.capacity {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.waiters.Len() != 0 || s.available < n {
		return false
	}
	s.available -= n
	return true
}

// Release returns n units to the semaphore and wakes queued waiters in FIFO
// order, stopping at the first waiter that cannot be satisfied. It reports an
// error if n is invalid or more units are released than currently acquired.
// Release remains allowed after Close for units acquired beforehand.
func (s *Semaphore) Release(n int64) error {
	if n <= 0 || n > s.capacity {
		return ErrInvalidN
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if n > s.capacity-s.available {
		return ErrOverRelease
	}
	s.available += n
	s.notifyLocked()
	return nil
}

// notifyLocked grants queued waiters in FIFO order while they fit. The head
// of the queue is never skipped. Caller must hold s.mu.
func (s *Semaphore) notifyLocked() {
	for e := s.waiters.Front(); e != nil; {
		w := e.Value.(*waiter)
		if w.n > s.available {
			return
		}
		s.available -= w.n
		next := e.Next()
		s.waiters.Remove(e)
		w.err = nil
		close(w.ch)
		w.elem = nil
		e = next
	}
}

// Capacity returns the total capacity of the semaphore.
func (s *Semaphore) Capacity() int64 {
	return s.capacity
}

// Available returns the number of currently free units.
func (s *Semaphore) Available() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.available
}

// Waiters returns the number of currently queued Acquire calls.
func (s *Semaphore) Waiters() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.waiters.Len()
}

// Close rejects all future acquisitions and makes every queued Acquire return
// ErrClosed. Units already acquired may still be released. Close is
// idempotent.
func (s *Semaphore) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	for e := s.waiters.Front(); e != nil; e = e.Next() {
		w := e.Value.(*waiter)
		w.err = ErrClosed
		close(w.ch)
		w.elem = nil
	}
	s.waiters.Init()
}
