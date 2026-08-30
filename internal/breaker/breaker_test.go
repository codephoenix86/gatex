package breaker

import (
	"errors"
	"sync"
	"testing"
)

func TestNewBreakerStartsClosed(t *testing.T) {
	t.Parallel()

	circuitBreaker := New()
	if got := circuitBreaker.State(); got != StateClosed {
		t.Errorf("State() = %s, want %s", got, StateClosed)
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
