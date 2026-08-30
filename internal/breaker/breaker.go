package breaker

import (
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrInvalidFailureThreshold indicates that a breaker was configured with
	// no failures required to trip it.
	ErrInvalidFailureThreshold = errors.New("circuit breaker failure threshold must be greater than zero")

	// ErrInvalidState indicates that a transition target is not a recognized
	// circuit-breaker state.
	ErrInvalidState = errors.New("circuit breaker state is invalid")

	// ErrInvalidTransition indicates that the requested state change would
	// bypass part of the circuit-breaker recovery cycle.
	ErrInvalidTransition = errors.New("circuit breaker transition is invalid")
)

// DefaultFailureThreshold is used when a backend pool does not configure its
// own consecutive-failure threshold.
const DefaultFailureThreshold = 5

// State describes whether a circuit breaker permits normal traffic, rejects
// it, or is testing whether its dependency has recovered.
type State uint8

const (
	// StateClosed is the normal operating state. Requests may reach the pool.
	StateClosed State = iota

	// StateOpen protects a failing pool from receiving requests.
	StateOpen

	// StateHalfOpen admits controlled probes before normal traffic resumes.
	StateHalfOpen
)

// String returns the operator-facing name of a circuit-breaker state.
func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return fmt.Sprintf("State(%d)", s)
	}
}

// Breaker owns the state machine for one backend pool. Its methods are safe
// for concurrent request and health-management goroutines.
type Breaker struct {
	mu                  sync.RWMutex
	state               State
	failureThreshold    int
	consecutiveFailures int
}

// New creates a closed circuit breaker with DefaultFailureThreshold.
func New() *Breaker {
	return &Breaker{
		state:            StateClosed,
		failureThreshold: DefaultFailureThreshold,
	}
}

// NewWithFailureThreshold creates a closed circuit breaker that opens after
// failureThreshold consecutive failures.
func NewWithFailureThreshold(failureThreshold int) (*Breaker, error) {
	if failureThreshold <= 0 {
		return nil, ErrInvalidFailureThreshold
	}
	return &Breaker{
		state:            StateClosed,
		failureThreshold: failureThreshold,
	}, nil
}

// State reports the breaker's current state.
func (b *Breaker) State() State {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.state
}

// ConsecutiveFailures reports the current closed-state failure streak.
func (b *Breaker) ConsecutiveFailures() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.consecutiveFailures
}

// RecordSuccess clears a closed breaker's consecutive-failure streak. Results
// that complete after the breaker has left the closed state are ignored.
func (b *Breaker) RecordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == StateClosed {
		b.consecutiveFailures = 0
	}
}

// RecordFailure adds to a closed breaker's consecutive-failure streak and
// opens it when the configured threshold is reached. Results that complete
// after the breaker has left the closed state are ignored.
func (b *Breaker) RecordFailure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state != StateClosed {
		return
	}

	b.consecutiveFailures++
	if b.consecutiveFailures >= b.failureThreshold {
		b.state = StateOpen
	}
}

// TransitionTo moves the breaker along a valid state-machine edge. Repeating
// the current state is a no-op. A closed breaker must open before it can become
// half-open, and an open breaker must pass through half-open before closing.
func (b *Breaker) TransitionTo(next State) error {
	if !next.valid() {
		return fmt.Errorf("%w: %s", ErrInvalidState, next)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state == next {
		return nil
	}
	if !validTransition(b.state, next) {
		return fmt.Errorf("%w: %s to %s", ErrInvalidTransition, b.state, next)
	}
	b.state = next
	if next == StateClosed {
		b.consecutiveFailures = 0
	}
	return nil
}

func (s State) valid() bool {
	return s == StateClosed || s == StateOpen || s == StateHalfOpen
}

func validTransition(current, next State) bool {
	switch current {
	case StateClosed:
		return next == StateOpen
	case StateOpen:
		return next == StateHalfOpen
	case StateHalfOpen:
		return next == StateClosed || next == StateOpen
	default:
		return false
	}
}
