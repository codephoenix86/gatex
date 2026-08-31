package breaker

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewBreakerStartsClosed(t *testing.T) {
	t.Parallel()

	circuitBreaker := New()
	if got := circuitBreaker.State(); got != StateClosed {
		t.Errorf("State() = %s, want %s", got, StateClosed)
	}
}

func TestNewBreakerRejectsInvalidFailureThreshold(t *testing.T) {
	t.Parallel()

	for _, threshold := range []int{0, -1} {
		if _, err := NewWithFailureThreshold(threshold); !errors.Is(err, ErrInvalidFailureThreshold) {
			t.Errorf("NewWithFailureThreshold(%d) error = %v, want %v", threshold, err, ErrInvalidFailureThreshold)
		}
	}
}

func TestNewBreakerRejectsInvalidRecoveryConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  Config
		wantErr error
	}{
		{name: "negative failure threshold", config: Config{FailureThreshold: -1}, wantErr: ErrInvalidFailureThreshold},
		{name: "negative open timeout", config: Config{OpenTimeout: -1}, wantErr: ErrInvalidOpenTimeout},
		{name: "negative probe limit", config: Config{HalfOpenMaxRequests: -1}, wantErr: ErrInvalidHalfOpenMaxRequests},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewWithConfig(test.config); !errors.Is(err, test.wantErr) {
				t.Errorf("NewWithConfig() error = %v, want %v", err, test.wantErr)
			}
		})
	}
}

func TestBreakerOpensAfterConsecutiveFailures(t *testing.T) {
	t.Parallel()

	circuitBreaker, err := NewWithFailureThreshold(3)
	if err != nil {
		t.Fatalf("NewWithFailureThreshold() error = %v", err)
	}
	for failure := 1; failure <= 3; failure++ {
		mustAcquirePermit(t, circuitBreaker).RecordFailure()
		wantState := StateClosed
		if failure == 3 {
			wantState = StateOpen
		}
		if got := circuitBreaker.State(); got != wantState {
			t.Fatalf("State() after %d failures = %s, want %s", failure, got, wantState)
		}
		if got := circuitBreaker.ConsecutiveFailures(); got != failure {
			t.Fatalf("ConsecutiveFailures() = %d, want %d", got, failure)
		}
	}
}

func TestBreakerSuccessResetsConsecutiveFailures(t *testing.T) {
	t.Parallel()

	circuitBreaker, err := NewWithFailureThreshold(3)
	if err != nil {
		t.Fatalf("NewWithFailureThreshold() error = %v", err)
	}
	mustAcquirePermit(t, circuitBreaker).RecordFailure()
	mustAcquirePermit(t, circuitBreaker).RecordFailure()
	mustAcquirePermit(t, circuitBreaker).RecordSuccess()
	if got := circuitBreaker.ConsecutiveFailures(); got != 0 {
		t.Fatalf("ConsecutiveFailures() = %d, want 0", got)
	}

	mustAcquirePermit(t, circuitBreaker).RecordFailure()
	mustAcquirePermit(t, circuitBreaker).RecordFailure()
	if got := circuitBreaker.State(); got != StateClosed {
		t.Errorf("State() after reset and two failures = %s, want %s", got, StateClosed)
	}
}

func TestOpenBreakerIgnoresLateResults(t *testing.T) {
	t.Parallel()

	circuitBreaker, err := NewWithFailureThreshold(1)
	if err != nil {
		t.Fatalf("NewWithFailureThreshold() error = %v", err)
	}
	trippingRequest := mustAcquirePermit(t, circuitBreaker)
	lateRequest := mustAcquirePermit(t, circuitBreaker)
	trippingRequest.RecordFailure()
	lateRequest.RecordSuccess()
	lateRequest.RecordFailure()

	if got := circuitBreaker.State(); got != StateOpen {
		t.Errorf("State() = %s, want %s", got, StateOpen)
	}
	if got := circuitBreaker.ConsecutiveFailures(); got != 1 {
		t.Errorf("ConsecutiveFailures() = %d, want 1", got)
	}
}

func TestOpenBreakerAdmitsLimitedHalfOpenProbeBatchAfterTimeout(t *testing.T) {
	t.Parallel()

	clock := newBreakerClock(time.Unix(1_700_000_000, 0))
	circuitBreaker := mustConfiguredBreaker(t, Config{
		FailureThreshold:    1,
		OpenTimeout:         time.Minute,
		HalfOpenMaxRequests: 2,
	}, clock.Now)
	mustAcquirePermit(t, circuitBreaker).RecordFailure()

	if permit, err := circuitBreaker.Acquire(); !errors.Is(err, ErrOpen) || permit != nil {
		t.Fatalf("Acquire() before timeout = (%v, %v), want (nil, %v)", permit, err, ErrOpen)
	}
	clock.Advance(time.Minute)
	firstProbe := mustAcquirePermit(t, circuitBreaker)
	secondProbe := mustAcquirePermit(t, circuitBreaker)
	if permit, err := circuitBreaker.Acquire(); !errors.Is(err, ErrHalfOpenProbeLimit) || permit != nil {
		t.Fatalf("Acquire() beyond probe limit = (%v, %v), want (nil, %v)", permit, err, ErrHalfOpenProbeLimit)
	}
	if got := circuitBreaker.State(); got != StateHalfOpen {
		t.Fatalf("State() = %s, want %s", got, StateHalfOpen)
	}

	firstProbe.RecordSuccess()
	if got := circuitBreaker.State(); got != StateHalfOpen {
		t.Fatalf("State() after first probe = %s, want %s", got, StateHalfOpen)
	}
	secondProbe.RecordSuccess()
	if got := circuitBreaker.State(); got != StateClosed {
		t.Errorf("State() after successful probe batch = %s, want %s", got, StateClosed)
	}
	if permit, err := circuitBreaker.Acquire(); err != nil || permit == nil {
		t.Errorf("Acquire() after recovery = (%v, %v), want non-nil permit", permit, err)
	}
}

func TestFailedHalfOpenProbeReopensBreakerAndRestartsTimeout(t *testing.T) {
	t.Parallel()

	clock := newBreakerClock(time.Unix(1_700_000_000, 0))
	circuitBreaker := mustConfiguredBreaker(t, Config{
		FailureThreshold:    1,
		OpenTimeout:         time.Minute,
		HalfOpenMaxRequests: 2,
	}, clock.Now)
	mustAcquirePermit(t, circuitBreaker).RecordFailure()
	clock.Advance(time.Minute)
	firstProbe := mustAcquirePermit(t, circuitBreaker)
	secondProbe := mustAcquirePermit(t, circuitBreaker)
	firstProbe.RecordSuccess()
	secondProbe.RecordFailure()

	if got := circuitBreaker.State(); got != StateOpen {
		t.Fatalf("State() after failed probe = %s, want %s", got, StateOpen)
	}
	if permit, err := circuitBreaker.Acquire(); !errors.Is(err, ErrOpen) || permit != nil {
		t.Fatalf("Acquire() after failed probe = (%v, %v), want (nil, %v)", permit, err, ErrOpen)
	}
	clock.Advance(time.Minute)
	if permit, err := circuitBreaker.Acquire(); err != nil || permit == nil {
		t.Errorf("Acquire() after restarted timeout = (%v, %v), want probe permit", permit, err)
	}
}

func TestStaleClosedResultCannotCompleteHalfOpenProbe(t *testing.T) {
	t.Parallel()

	clock := newBreakerClock(time.Unix(1_700_000_000, 0))
	circuitBreaker := mustConfiguredBreaker(t, Config{
		FailureThreshold:    1,
		OpenTimeout:         time.Minute,
		HalfOpenMaxRequests: 1,
	}, clock.Now)
	trippingRequest := mustAcquirePermit(t, circuitBreaker)
	lateRequest := mustAcquirePermit(t, circuitBreaker)
	trippingRequest.RecordFailure()
	clock.Advance(time.Minute)
	probe := mustAcquirePermit(t, circuitBreaker)

	lateRequest.RecordSuccess()
	if got := circuitBreaker.State(); got != StateHalfOpen {
		t.Fatalf("State() after stale success = %s, want %s", got, StateHalfOpen)
	}
	probe.RecordSuccess()
	if got := circuitBreaker.State(); got != StateClosed {
		t.Errorf("State() after probe success = %s, want %s", got, StateClosed)
	}
}

func TestBreakerLimitsConcurrentHalfOpenAdmission(t *testing.T) {
	t.Parallel()

	const (
		probeLimit = 3
		workers    = 1_000
	)
	clock := newBreakerClock(time.Unix(1_700_000_000, 0))
	circuitBreaker := mustConfiguredBreaker(t, Config{
		FailureThreshold:    1,
		OpenTimeout:         time.Minute,
		HalfOpenMaxRequests: probeLimit,
	}, clock.Now)
	mustAcquirePermit(t, circuitBreaker).RecordFailure()
	clock.Advance(time.Minute)

	start := make(chan struct{})
	var admitted atomic.Int64
	var group sync.WaitGroup
	group.Add(workers)
	for range workers {
		go func() {
			defer group.Done()
			<-start
			if _, err := circuitBreaker.Acquire(); err == nil {
				admitted.Add(1)
			}
		}()
	}
	close(start)
	group.Wait()

	if got := admitted.Load(); got != probeLimit {
		t.Errorf("admitted probes = %d, want %d", got, probeLimit)
	}
}

func TestCancelledHalfOpenPermitReleasesProbeSlot(t *testing.T) {
	t.Parallel()

	clock := newBreakerClock(time.Unix(1_700_000_000, 0))
	circuitBreaker := mustConfiguredBreaker(t, Config{
		FailureThreshold:    1,
		OpenTimeout:         time.Minute,
		HalfOpenMaxRequests: 1,
	}, clock.Now)
	mustAcquirePermit(t, circuitBreaker).RecordFailure()
	clock.Advance(time.Minute)

	unusedProbe := mustAcquirePermit(t, circuitBreaker)
	unusedProbe.Cancel()
	replacementProbe := mustAcquirePermit(t, circuitBreaker)
	unusedProbe.RecordFailure()
	if got := circuitBreaker.State(); got != StateHalfOpen {
		t.Fatalf("State() after cancelled probe result = %s, want %s", got, StateHalfOpen)
	}
	replacementProbe.RecordSuccess()
	if got := circuitBreaker.State(); got != StateClosed {
		t.Errorf("State() after replacement probe = %s, want %s", got, StateClosed)
	}
}

func TestBreakerCountsConcurrentFailuresSafely(t *testing.T) {
	t.Parallel()

	const (
		failureThreshold = 100
		workers          = 1_000
	)
	circuitBreaker, err := NewWithFailureThreshold(failureThreshold)
	if err != nil {
		t.Fatalf("NewWithFailureThreshold() error = %v", err)
	}

	permits := make([]*Permit, 0, workers)
	for range workers {
		permits = append(permits, mustAcquirePermit(t, circuitBreaker))
	}

	start := make(chan struct{})
	var group sync.WaitGroup
	group.Add(workers)
	for _, permit := range permits {
		go func() {
			defer group.Done()
			<-start
			permit.RecordFailure()
		}()
	}
	close(start)
	group.Wait()

	if got := circuitBreaker.State(); got != StateOpen {
		t.Errorf("State() = %s, want %s", got, StateOpen)
	}
	if got := circuitBreaker.ConsecutiveFailures(); got != failureThreshold {
		t.Errorf("ConsecutiveFailures() = %d, want %d", got, failureThreshold)
	}
}

func TestBreakerTransitionsThroughRecoveryCycle(t *testing.T) {
	t.Parallel()

	circuitBreaker := New()
	wantStates := []State{StateOpen, StateHalfOpen, StateClosed}
	for _, want := range wantStates {
		if err := circuitBreaker.TransitionTo(want); err != nil {
			t.Fatalf("TransitionTo(%s) error = %v", want, err)
		}
		if got := circuitBreaker.State(); got != want {
			t.Fatalf("State() = %s, want %s", got, want)
		}
	}
}

func TestHalfOpenBreakerCanReopen(t *testing.T) {
	t.Parallel()

	circuitBreaker := New()
	for _, state := range []State{StateOpen, StateHalfOpen, StateOpen} {
		if err := circuitBreaker.TransitionTo(state); err != nil {
			t.Fatalf("TransitionTo(%s) error = %v", state, err)
		}
	}
	if got := circuitBreaker.State(); got != StateOpen {
		t.Errorf("State() = %s, want %s", got, StateOpen)
	}
}

func TestBreakerRejectsInvalidTransitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		start   State
		next    State
		wantErr error
	}{
		{name: "closed to half-open", start: StateClosed, next: StateHalfOpen, wantErr: ErrInvalidTransition},
		{name: "open to closed", start: StateOpen, next: StateClosed, wantErr: ErrInvalidTransition},
		{name: "unknown state", start: StateClosed, next: State(99), wantErr: ErrInvalidState},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			circuitBreaker := New()
			if test.start == StateOpen {
				if err := circuitBreaker.TransitionTo(StateOpen); err != nil {
					t.Fatalf("prepare breaker: %v", err)
				}
			}

			err := circuitBreaker.TransitionTo(test.next)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("TransitionTo(%s) error = %v, want %v", test.next, err, test.wantErr)
			}
			if got := circuitBreaker.State(); got != test.start {
				t.Errorf("State() after rejected transition = %s, want %s", got, test.start)
			}
		})
	}
}

func TestBreakerRepeatedStateIsNoOp(t *testing.T) {
	t.Parallel()

	circuitBreaker := New()
	if err := circuitBreaker.TransitionTo(StateClosed); err != nil {
		t.Errorf("TransitionTo(StateClosed) error = %v", err)
	}
}

func TestBreakerStateIsSafeUnderConcurrentAccess(t *testing.T) {
	t.Parallel()

	circuitBreaker := New()

	const (
		readers     = 32
		transitions = 500
	)
	start := make(chan struct{})
	var group sync.WaitGroup
	group.Add(readers + 1)
	for range readers {
		go func() {
			defer group.Done()
			<-start
			for range transitions {
				_ = circuitBreaker.State()
			}
		}()
	}
	go func() {
		defer group.Done()
		<-start
		for range transitions {
			for _, state := range []State{StateOpen, StateHalfOpen, StateClosed} {
				if err := circuitBreaker.TransitionTo(state); err != nil {
					t.Errorf("TransitionTo(%s) error = %v", state, err)
					return
				}
			}
		}
	}()
	close(start)
	group.Wait()
	if got := circuitBreaker.State(); got != StateClosed {
		t.Errorf("final State() = %s, want %s", got, StateClosed)
	}
}

func mustAcquirePermit(t *testing.T, circuitBreaker *Breaker) *Permit {
	t.Helper()
	permit, err := circuitBreaker.Acquire()
	if err != nil {
		t.Fatalf("Acquire() error = %v, want permit", err)
	}
	return permit
}

func mustConfiguredBreaker(t *testing.T, config Config, now func() time.Time) *Breaker {
	t.Helper()
	circuitBreaker, err := newWithConfig(config, now)
	if err != nil {
		t.Fatalf("newWithConfig() error = %v", err)
	}
	return circuitBreaker
}

type breakerClock struct {
	mu  sync.Mutex
	now time.Time
}

func newBreakerClock(now time.Time) *breakerClock {
	return &breakerClock{now: now}
}

func (c *breakerClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *breakerClock) Advance(duration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(duration)
}
