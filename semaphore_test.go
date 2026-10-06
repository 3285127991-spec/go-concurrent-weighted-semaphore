package weightedsemaphore

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func recvResult(t *testing.T, what string, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for %s", what)
		return nil
	}
}

func TestImmediateAcquire(t *testing.T) {
	s, err := New(4)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Acquire(context.Background(), 3); err != nil {
		t.Fatalf("Acquire(3): %v", err)
	}
	if got := s.Available(); got != 1 {
		t.Fatalf("Available = %d, want 1", got)
	}
	if !s.TryAcquire(1) {
		t.Fatal("TryAcquire(1) should succeed with 1 unit left")
	}
	if s.TryAcquire(1) {
		t.Fatal("TryAcquire(1) should fail with 0 units left")
	}
	if err := s.Release(4); err != nil {
		t.Fatalf("Release(4): %v", err)
	}
	if got := s.Available(); got != 4 {
		t.Fatalf("Available = %d, want 4", got)
	}
}

func TestAcquireBlocksUntilRelease(t *testing.T) {
	s, _ := New(2)
	if err := s.Acquire(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- s.Acquire(context.Background(), 1)
	}()
	waitFor(t, "waiter to queue", func() bool { return s.Waiters() == 1 })

	select {
	case err := <-done:
		t.Fatalf("Acquire returned %v before any Release", err)
	case <-time.After(20 * time.Millisecond):
	}

	if err := s.Release(2); err != nil {
		t.Fatal(err)
	}
	if err := recvResult(t, "blocked Acquire", done); err != nil {
		t.Fatalf("blocked Acquire: %v", err)
	}
	if got := s.Available(); got != 1 {
		t.Fatalf("Available = %d, want 1", got)
	}
}

func TestFIFOOrder(t *testing.T) {
	s, _ := New(4)
	if err := s.Acquire(context.Background(), 4); err != nil {
		t.Fatal(err)
	}
	grants := make(chan string, 3)
	var wg sync.WaitGroup
	start := func(name string, n int64, releaseAfter int64) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.Acquire(context.Background(), n); err != nil {
				t.Errorf("%s Acquire: %v", name, err)
				return
			}
			grants <- name
			if releaseAfter > 0 {
				if err := s.Release(releaseAfter); err != nil {
					t.Errorf("%s Release: %v", name, err)
				}
			}
		}()
	}
	// Enqueue deterministically: A(2), then B(2), then C(1).
	start("A", 2, 1) // A returns 1 unit later so C can proceed
	waitFor(t, "A queued", func() bool { return s.Waiters() == 1 })
	start("B", 2, 0)
	waitFor(t, "B queued", func() bool { return s.Waiters() == 2 })
	start("C", 1, 0)
	waitFor(t, "C queued", func() bool { return s.Waiters() == 3 })

	// 1 free unit satisfies C(1) but head A(2) blocks the whole queue.
	if err := s.Release(1); err != nil {
		t.Fatal(err)
	}
	select {
	case name := <-grants:
		t.Fatalf("%s overtook the queue head", name)
	case <-time.After(20 * time.Millisecond):
	}

	if err := s.Release(1); err != nil { // A(2) fits now
		t.Fatal(err)
	}
	if name := <-grants; name != "A" {
		t.Fatalf("first grant = %s, want A", name)
	}
	if err := s.Release(2); err != nil { // B(2) fits now
		t.Fatal(err)
	}
	if name := <-grants; name != "B" {
		t.Fatalf("second grant = %s, want B", name)
	}
	// A's deferred Release(1) frees the unit C needs.
	if name := <-grants; name != "C" {
		t.Fatalf("third grant = %s, want C", name)
	}
	wg.Wait()
}

func TestTryAcquireFairness(t *testing.T) {
	s, _ := New(2)
	if err := s.Acquire(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- s.Acquire(context.Background(), 2) // queues behind the held unit
	}()
	waitFor(t, "waiter queued", func() bool { return s.Waiters() == 1 })

	if s.TryAcquire(1) {
		t.Fatal("TryAcquire must not overtake a queued waiter")
	}
	if err := s.Release(1); err != nil {
		t.Fatal(err)
	}
	if err := recvResult(t, "queued Acquire", done); err != nil {
		t.Fatalf("queued Acquire: %v", err)
	}
	if s.TryAcquire(1) {
		t.Fatal("TryAcquire should fail: 0 units available")
	}
	if err := s.Release(2); err != nil {
		t.Fatal(err)
	}
	if !s.TryAcquire(2) {
		t.Fatal("TryAcquire should succeed with no waiters and enough units")
	}
}

func TestContextCancel(t *testing.T) {
	s, _ := New(2)
	if err := s.Acquire(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	ctx1, cancel1 := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() {
		first <- s.Acquire(ctx1, 1)
	}()
	second := make(chan error, 1)
	go func() {
		second <- s.Acquire(context.Background(), 2)
	}()
	waitFor(t, "two waiters queued", func() bool { return s.Waiters() == 2 })

	cancel1()
	if err := recvResult(t, "canceled Acquire", first); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Acquire = %v, want context.Canceled", err)
	}
	waitFor(t, "waiter removed", func() bool { return s.Waiters() == 1 })

	// The remaining waiter is unaffected and is granted on Release.
	if err := s.Release(2); err != nil {
		t.Fatal(err)
	}
	if err := recvResult(t, "surviving Acquire", second); err != nil {
		t.Fatalf("surviving Acquire: %v", err)
	}
	if got := s.Available(); got != 0 {
		t.Fatalf("Available = %d, want 0 (canceled waiter consumed nothing)", got)
	}
}

func TestInvalidN(t *testing.T) {
	if _, err := New(0); !errors.Is(err, ErrInvalidN) {
		t.Fatalf("New(0) = %v, want ErrInvalidN", err)
	}
	if _, err := New(-2); !errors.Is(err, ErrInvalidN) {
		t.Fatalf("New(-2) = %v, want ErrInvalidN", err)
	}
	s, _ := New(3)
	for _, n := range []int64{0, -1, 4} {
		if err := s.Acquire(context.Background(), n); !errors.Is(err, ErrInvalidN) {
			t.Errorf("Acquire(%d) = %v, want ErrInvalidN", n, err)
		}
		if s.TryAcquire(n) {
			t.Errorf("TryAcquire(%d) = true, want false", n)
		}
		if err := s.Release(n); !errors.Is(err, ErrInvalidN) {
			t.Errorf("Release(%d) = %v, want ErrInvalidN", n, err)
		}
	}
	if got := s.Available(); got != 3 {
		t.Fatalf("Available = %d, want 3 (invalid calls changed nothing)", got)
	}
}

func TestOverRelease(t *testing.T) {
	s, _ := New(3)
	if err := s.Acquire(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	if err := s.Release(3); !errors.Is(err, ErrOverRelease) {
		t.Fatalf("Release(3) with 2 held = %v, want ErrOverRelease", err)
	}
	if err := s.Release(2); err != nil {
		t.Fatalf("Release(2): %v", err)
	}
	if err := s.Release(1); !errors.Is(err, ErrOverRelease) {
		t.Fatalf("Release(1) with 0 held = %v, want ErrOverRelease", err)
	}
	if got := s.Available(); got != 3 {
		t.Fatalf("Available = %d, want 3", got)
	}
}

func TestClose(t *testing.T) {
	s, _ := New(2)
	if err := s.Acquire(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	waiterErrs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			waiterErrs <- s.Acquire(context.Background(), 1)
		}()
	}
	waitFor(t, "waiters queued", func() bool { return s.Waiters() == 2 })

	s.Close()
	for i := 0; i < 2; i++ {
		if err := recvResult(t, "closed waiter", waiterErrs); !errors.Is(err, ErrClosed) {
			t.Fatalf("waiter got %v, want ErrClosed", err)
		}
	}
	if got := s.Waiters(); got != 0 {
		t.Fatalf("Waiters = %d, want 0 after Close", got)
	}
	if err := s.Acquire(context.Background(), 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("Acquire after Close = %v, want ErrClosed", err)
	}
	if s.TryAcquire(1) {
		t.Fatal("TryAcquire after Close = true, want false")
	}
	// Units acquired before Close may still be released.
	if err := s.Release(2); err != nil {
		t.Fatalf("Release after Close: %v", err)
	}
	if got := s.Available(); got != 2 {
		t.Fatalf("Available = %d, want 2", got)
	}
	s.Close() // idempotent
}

func TestQueries(t *testing.T) {
	s, _ := New(5)
	if got := s.Capacity(); got != 5 {
		t.Fatalf("Capacity = %d, want 5", got)
	}
	if got := s.Available(); got != 5 {
		t.Fatalf("Available = %d, want 5", got)
	}
	if got := s.Waiters(); got != 0 {
		t.Fatalf("Waiters = %d, want 0", got)
	}
	if err := s.Acquire(context.Background(), 5); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- s.Acquire(context.Background(), 2)
	}()
	waitFor(t, "waiter queued", func() bool { return s.Waiters() == 1 })
	if err := s.Release(5); err != nil {
		t.Fatal(err)
	}
	if err := recvResult(t, "queued Acquire", done); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "waiter drained", func() bool { return s.Waiters() == 0 })
	if got := s.Available(); got != 3 {
		t.Fatalf("Available = %d, want 3", got)
	}
}

func TestConcurrent(t *testing.T) {
	const capacity = 8
	s, _ := New(capacity)
	var inflight atomic.Int64
	var maxSeen atomic.Int64

	stop := make(chan struct{})
	var watchers sync.WaitGroup
	watchers.Add(1)
	go func() { // exercise concurrent queries under -race
		defer watchers.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = s.Available()
				_ = s.Waiters()
				_ = s.Capacity()
			}
		}
	}()

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			n := int64(id%3 + 1)
			for i := 0; i < 5; i++ {
				if err := s.Acquire(context.Background(), n); err != nil {
					t.Errorf("Acquire(%d): %v", n, err)
					return
				}
				cur := inflight.Add(n)
				for {
					old := maxSeen.Load()
					if cur <= old || maxSeen.CompareAndSwap(old, cur) {
						break
					}
				}
				inflight.Add(-n)
				if err := s.Release(n); err != nil {
					t.Errorf("Release(%d): %v", n, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(stop)
	watchers.Wait()

	if got := maxSeen.Load(); got > capacity {
		t.Fatalf("peak in-flight units = %d, exceeds capacity %d", got, capacity)
	}
	if got := s.Available(); got != capacity {
		t.Fatalf("Available = %d, want %d after all releases", got, capacity)
	}
	if got := s.Waiters(); got != 0 {
		t.Fatalf("Waiters = %d, want 0 (no leaked waiters)", got)
	}
}
