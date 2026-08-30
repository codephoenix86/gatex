package breaker

import (
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrInvalidState indicates that a transition target is not a recognized
	// circuit-breaker state.
	ErrInvalidState = errors.New("circuit breaker state is invalid")

	// ErrInvalidTransition indicates that the requested state change would
	// bypass part of the circuit-breaker recovery cycle.
	ErrInvalidTransition = errors.New("circuit breaker transition is invalid")
)

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
	mu    sync.RWMutex
	state State
}

// New creates a circuit breaker in the closed state.
func New() *Breaker {
	return &Breaker{state: StateClosed}
}

// State reports the breaker's current state.
func (b *Breaker) State() State {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.state
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
