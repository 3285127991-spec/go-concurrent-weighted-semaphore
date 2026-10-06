package weightedsemaphore

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const testTimeout = 2 * time.Second

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func awaitErr(t *testing.T, what string, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(testTimeout):
		t.Fatalf("timeout waiting for %s", what)
		return nil
	}
}

func TestAcquireImmediate(t *testing.T) {
	s := New(3)
	if err := s.Acquire(context.Background(), 2); err != nil {
		t.Fatalf("Acquire(2): %v", err)
	}
	if got := s.Available(); got != 1 {
		t.Fatalf("Available = %d, want 1", got)
	}
	if !s.TryAcquire(1) {
		t.Fatal("TryAcquire(1) = false, want true")
	}
	if got := s.Available(); got != 0 {
		t.Fatalf("Available = %d, want 0", got)
	}
}

func TestAcquireWaitsForRelease(t *testing.T) {
	s := New(2)
	if err := s.Acquire(context.Background(), 2); err != nil {
		t.Fatalf("Acquire(2): %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- s.Acquire(context.Background(), 1) }()
	waitFor(t, "waiter to queue", func() bool { return s.Waiters() == 1 })

	select {
	case err := <-done:
		t.Fatalf("Acquire returned %v before any Release", err)
	default:
	}

	if err := s.Release(1); err != nil {
		t.Fatalf("Release(1): %v", err)
	}
	if err := awaitErr(t, "blocked Acquire", done); err != nil {
		t.Fatalf("blocked Acquire: %v", err)
	}
	if got := s.Available(); got != 0 {
		t.Fatalf("Available = %d, want 0", got)
	}
}

func TestFIFOOrder(t *testing.T) {
	s := New(1)
	if err := s.Acquire(context.Background(), 1); err != nil {
		t.Fatalf("Acquire(1): %v", err)
	}
	order := make(chan int, 3)
	for i := 1; i <= 3; i++ {
		go func() {
			if err := s.Acquire(context.Background(), 1); err != nil {
				t.Errorf("Acquire %d: %v", i, err)
				return
			}
			order <- i
			_ = s.Release(1)
		}()
		waitFor(t, "waiter to queue", func() bool { return s.Waiters() == i })
	}
	if err := s.Release(1); err != nil {
		t.Fatalf("Release(1): %v", err)
	}
	for want := 1; want <= 3; want++ {
		select {
		case got := <-order:
			if got != want {
				t.Fatalf("FIFO violation: woke %d, want %d", got, want)
			}
		case <-time.After(testTimeout):
			t.Fatalf("timeout waiting for waiter %d", want)
		}
	}
}

func TestTryAcquireFairness(t *testing.T) {
	s := New(2)
	if err := s.Acquire(context.Background(), 1); err != nil {
		t.Fatalf("Acquire(1): %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- s.Acquire(context.Background(), 2) }()
	waitFor(t, "waiter to queue", func() bool { return s.Waiters() == 1 })

	if s.TryAcquire(1) {
		t.Fatal("TryAcquire succeeded while a waiter was queued")
	}
	if err := s.Release(1); err != nil {
		t.Fatalf("Release(1): %v", err)
	}
	if err := awaitErr(t, "queued Acquire", done); err != nil {
		t.Fatalf("queued Acquire: %v", err)
	}
	if s.TryAcquire(1) {
		t.Fatal("TryAcquire succeeded with no units free")
	}
	if err := s.Release(2); err != nil {
		t.Fatalf("Release(2): %v", err)
	}
	if !s.TryAcquire(2) {
		t.Fatal("TryAcquire failed with all units free and no waiters")
	}
}

func TestContextCancel(t *testing.T) {
	s := New(1)
	if err := s.Acquire(context.Background(), 1); err != nil {
		t.Fatalf("Acquire(1): %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	canceled := make(chan error, 1)
	go func() { canceled <- s.Acquire(ctx, 1) }()
	waitFor(t, "first waiter", func() bool { return s.Waiters() == 1 })

	blocked := make(chan error, 1)
	go func() { blocked <- s.Acquire(context.Background(), 1) }()
	waitFor(t, "second waiter", func() bool { return s.Waiters() == 2 })

	cancel()
	if err := awaitErr(t, "canceled Acquire", canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Acquire = %v, want context.Canceled", err)
	}
	waitFor(t, "waiter removal", func() bool { return s.Waiters() == 1 })

	if err := s.Release(1); err != nil {
		t.Fatalf("Release(1): %v", err)
	}
	if err := awaitErr(t, "remaining waiter", blocked); err != nil {
		t.Fatalf("remaining waiter: %v", err)
	}
}

func TestCancelHeadWakesNext(t *testing.T) {
	s := New(3)
	if err := s.Acquire(context.Background(), 1); err != nil {
		t.Fatalf("Acquire(1): %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	head := make(chan error, 1)
	go func() { head <- s.Acquire(ctx, 3) }() // needs 3, only 2 free
	waitFor(t, "head waiter", func() bool { return s.Waiters() == 1 })

	next := make(chan error, 1)
	go func() { next <- s.Acquire(context.Background(), 2) }() // fits, but may not overtake
	waitFor(t, "second waiter", func() bool { return s.Waiters() == 2 })

	cancel()
	if err := awaitErr(t, "canceled head", head); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled head = %v, want context.Canceled", err)
	}
	if err := awaitErr(t, "next waiter", next); err != nil {
		t.Fatalf("next waiter: %v", err)
	}
	if got := s.Available(); got != 0 {
		t.Fatalf("Available = %d, want 0", got)
	}
}

func TestInvalidN(t *testing.T) {
	s := New(2)
	for _, n := range []int64{0, -1, 3} {
		if err := s.Acquire(context.Background(), n); err == nil {
			t.Errorf("Acquire(%d) = nil, want error", n)
		}
		if s.TryAcquire(n) {
			t.Errorf("TryAcquire(%d) = true, want false", n)
		}
		if err := s.Release(n); err == nil {
			t.Errorf("Release(%d) = nil, want error", n)
		}
	}
	for _, c := range []int64{0, -1} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("New(%d) did not panic", c)
				}
			}()
			New(c)
		}()
	}
	if got := s.Available(); got != 2 {
		t.Fatalf("Available = %d, want 2 (invalid calls must not change state)", got)
	}
}

func TestOverRelease(t *testing.T) {
	s := New(2)
	if err := s.Release(1); err == nil {
		t.Fatal("Release(1) on full semaphore = nil, want error")
	}
	if err := s.Acquire(context.Background(), 2); err != nil {
		t.Fatalf("Acquire(2): %v", err)
	}
	if err := s.Release(2); err != nil {
		t.Fatalf("Release(2): %v", err)
	}
	if err := s.Release(1); err == nil {
		t.Fatal("Release(1) after returning to full = nil, want error")
	}
	if got := s.Available(); got != 2 {
		t.Fatalf("Available = %d, want 2", got)
	}
}

func TestClose(t *testing.T) {
	s := New(2)
	if err := s.Acquire(context.Background(), 1); err != nil {
		t.Fatalf("Acquire(1): %v", err)
	}
	errs := make(chan error, 2)
	go func() { errs <- s.Acquire(context.Background(), 2) }()
	go func() { errs <- s.Acquire(context.Background(), 1) }()
	waitFor(t, "two waiters", func() bool { return s.Waiters() == 2 })

	s.Close()
	for i := 0; i < 2; i++ {
		if err := awaitErr(t, "waiter after Close", errs); !errors.Is(err, ErrClosed) {
			t.Fatalf("waiter after Close = %v, want ErrClosed", err)
		}
	}
	if got := s.Waiters(); got != 0 {
		t.Fatalf("Waiters = %d, want 0", got)
	}
	if err := s.Acquire(context.Background(), 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("Acquire after Close = %v, want ErrClosed", err)
	}
	if s.TryAcquire(1) {
		t.Fatal("TryAcquire after Close = true, want false")
	}
	if err := s.Release(1); err != nil {
		t.Fatalf("Release after Close: %v", err)
	}
	if got := s.Available(); got != 2 {
		t.Fatalf("Available = %d, want 2", got)
	}
	s.Close() // idempotent
}

func TestQueries(t *testing.T) {
	s := New(4)
	if got := s.Capacity(); got != 4 {
		t.Fatalf("Capacity = %d, want 4", got)
	}
	if got := s.Available(); got != 4 {
		t.Fatalf("Available = %d, want 4", got)
	}
	if got := s.Waiters(); got != 0 {
		t.Fatalf("Waiters = %d, want 0", got)
	}
	if err := s.Acquire(context.Background(), 3); err != nil {
		t.Fatalf("Acquire(3): %v", err)
	}
	if got := s.Available(); got != 1 {
		t.Fatalf("Available = %d, want 1", got)
	}
	done := make(chan error, 1)
	go func() { done <- s.Acquire(context.Background(), 2) }()
	waitFor(t, "waiter", func() bool { return s.Waiters() == 1 })
	s.Close()
	if err := awaitErr(t, "waiter after Close", done); !errors.Is(err, ErrClosed) {
		t.Fatalf("waiter = %v, want ErrClosed", err)
	}
	if got := s.Waiters(); got != 0 {
		t.Fatalf("Waiters = %d, want 0", got)
	}
}

func TestConcurrent(t *testing.T) {
	const capacity = 5
	s := New(capacity)
	var inUse int64
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			n := int64(g%3 + 1)
			use := func(units int64) {
				if cur := atomic.AddInt64(&inUse, units); cur > capacity {
					t.Errorf("oversell: %d units in use, capacity %d", cur, capacity)
				}
			}
			for i := 0; i < 50; i++ {
				if err := s.Acquire(context.Background(), n); err != nil {
					t.Errorf("Acquire(%d): %v", n, err)
					return
				}
				use(n)
				atomic.AddInt64(&inUse, -n)
				if err := s.Release(n); err != nil {
					t.Errorf("Release(%d): %v", n, err)
					return
				}
				if s.TryAcquire(1) {
					use(1)
					atomic.AddInt64(&inUse, -1)
					if err := s.Release(1); err != nil {
						t.Errorf("Release(1): %v", err)
						return
					}
				}
				_ = s.Capacity()
				_ = s.Available()
				_ = s.Waiters()
			}
		}(g)
	}
	wg.Wait()
	if got := s.Available(); got != capacity {
		t.Fatalf("Available = %d, want %d", got, capacity)
	}
	if got := s.Waiters(); got != 0 {
		t.Fatalf("Waiters = %d, want 0", got)
	}
}
