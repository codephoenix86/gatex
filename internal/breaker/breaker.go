package breaker

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	// ErrInvalidFailureThreshold indicates that a breaker was configured with
	// no failures required to trip it.
	ErrInvalidFailureThreshold = errors.New("circuit breaker failure threshold must be greater than zero")

	// ErrInvalidOpenTimeout indicates that a breaker was configured with a
	// negative recovery wait.
	ErrInvalidOpenTimeout = errors.New("circuit breaker open timeout must be greater than zero")

	// ErrInvalidHalfOpenMaxRequests indicates that a breaker was configured
	// with a negative trial-request limit.
	ErrInvalidHalfOpenMaxRequests = errors.New("circuit breaker half-open max requests must be greater than zero")

	// ErrInvalidState indicates that a transition target is not a recognized
	// circuit-breaker state.
	ErrInvalidState = errors.New("circuit breaker state is invalid")

	// ErrInvalidTransition indicates that the requested state change would
	// bypass part of the circuit-breaker recovery cycle.
	ErrInvalidTransition = errors.New("circuit breaker transition is invalid")
)

const (
	// DefaultFailureThreshold is used when a backend pool does not configure
	// its own consecutive-failure threshold.
	DefaultFailureThreshold = 5

	// DefaultOpenTimeout is how long an open breaker waits before admitting
	// recovery probes.
	DefaultOpenTimeout = 30 * time.Second

	// DefaultHalfOpenMaxRequests is the size of the recovery probe batch.
	DefaultHalfOpenMaxRequests = 1
)

// Config controls failure tripping and half-open recovery behavior. Zero
// values use the package defaults.
type Config struct {
	FailureThreshold    int
	OpenTimeout         time.Duration
	HalfOpenMaxRequests int
}

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
	openTimeout         time.Duration
	halfOpenMaxRequests int
	openedAt            time.Time
	generation          uint64
	halfOpenAdmitted    int
	halfOpenSuccesses   int
	now                 func() time.Time
}

// New creates a closed circuit breaker with DefaultFailureThreshold.
func New() *Breaker {
	return &Breaker{
		state:               StateClosed,
		failureThreshold:    DefaultFailureThreshold,
		openTimeout:         DefaultOpenTimeout,
		halfOpenMaxRequests: DefaultHalfOpenMaxRequests,
		now:                 time.Now,
	}
}

// NewWithFailureThreshold creates a closed circuit breaker that opens after
// failureThreshold consecutive failures.
func NewWithFailureThreshold(failureThreshold int) (*Breaker, error) {
	if failureThreshold <= 0 {
		return nil, ErrInvalidFailureThreshold
	}
	return NewWithConfig(Config{FailureThreshold: failureThreshold})
}

// NewWithConfig creates a closed circuit breaker with the supplied behavior.
func NewWithConfig(config Config) (*Breaker, error) {
	return newWithConfig(config, time.Now)
}

func newWithConfig(config Config, now func() time.Time) (*Breaker, error) {
	if config.FailureThreshold < 0 {
		return nil, ErrInvalidFailureThreshold
	}
	if config.OpenTimeout < 0 {
		return nil, ErrInvalidOpenTimeout
	}
	if config.HalfOpenMaxRequests < 0 {
		return nil, ErrInvalidHalfOpenMaxRequests
	}
	if now == nil {
		return nil, errors.New("circuit breaker clock cannot be nil")
	}
	if config.FailureThreshold == 0 {
		config.FailureThreshold = DefaultFailureThreshold
	}
	if config.OpenTimeout == 0 {
		config.OpenTimeout = DefaultOpenTimeout
	}
	if config.HalfOpenMaxRequests == 0 {
		config.HalfOpenMaxRequests = DefaultHalfOpenMaxRequests
	}

	return &Breaker{
		state:               StateClosed,
		failureThreshold:    config.FailureThreshold,
		openTimeout:         config.OpenTimeout,
		halfOpenMaxRequests: config.HalfOpenMaxRequests,
		now:                 now,
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

// Acquire admits normal traffic while closed. Once the open timeout expires,
// it lazily moves the breaker to half-open and admits at most the configured
// number of probe requests. The caller must record exactly one outcome on an
// admitted Permit.
func (b *Breaker) Acquire() (*Permit, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state == StateOpen {
		if b.now().Before(b.openedAt.Add(b.openTimeout)) {
			return nil, false
		}
		b.transitionLocked(StateHalfOpen)
	}
	if b.state == StateHalfOpen {
		if b.halfOpenAdmitted >= b.halfOpenMaxRequests {
			return nil, false
		}
		b.halfOpenAdmitted++
	}

	return &Permit{
		breaker:    b,
		generation: b.generation,
		probe:      b.state == StateHalfOpen,
	}, true
}

// Permit represents one request admitted by a Breaker. A permit accepts only
// its first result, and results from an obsolete breaker generation are
// ignored.
type Permit struct {
	breaker    *Breaker
	generation uint64
	probe      bool
	once       sync.Once
}

// RecordSuccess reports that the permitted upstream request completed without
// a transport error or server-side failure response.
func (p *Permit) RecordSuccess() {
	p.record(true)
}

// RecordFailure reports that the permitted upstream request failed.
func (p *Permit) RecordFailure() {
	p.record(false)
}

func (p *Permit) record(success bool) {
	if p == nil || p.breaker == nil {
		return
	}
	p.once.Do(func() {
		p.breaker.record(p.generation, p.probe, success)
	})
}

func (b *Breaker) record(generation uint64, probe, success bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.generation != generation {
		return
	}

	if probe {
		if b.state != StateHalfOpen {
			return
		}
		if !success {
			b.transitionLocked(StateOpen)
			return
		}
		b.halfOpenSuccesses++
		if b.halfOpenSuccesses == b.halfOpenMaxRequests {
			b.transitionLocked(StateClosed)
		}
		return
	}

	if b.state != StateClosed {
		return
	}
	if success {
		b.consecutiveFailures = 0
		return
	}
	b.consecutiveFailures++
	if b.consecutiveFailures == b.failureThreshold {
		b.transitionLocked(StateOpen)
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
	b.transitionLocked(next)
	return nil
}

func (b *Breaker) transitionLocked(next State) {
	b.state = next
	b.generation++
	b.halfOpenAdmitted = 0
	b.halfOpenSuccesses = 0
	if next == StateOpen {
		b.openedAt = b.now()
	}
	if next == StateClosed {
		b.consecutiveFailures = 0
	}
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
