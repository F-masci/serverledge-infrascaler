package node

import (
	"context"
	"sync"
)

var (
	drainMu     sync.Mutex
	draining    bool
	inFlight    int64
	drainDoneCh chan struct{}
)

// TryAcquireRequest registers a new in-flight request if the node is not draining.
// Returns false when the node has entered draining mode and must reject new work.
func TryAcquireRequest() bool {
	drainMu.Lock()
	defer drainMu.Unlock()

	// If the node is draining, we cannot accept new requests.
	if draining {
		return false
	}

	inFlight++

	return true
}

// ReleaseRequest decrements the in-flight request counter and notifies any
// drain waiter once the node is draining and no requests remain active.
func ReleaseRequest() {
	drainMu.Lock()

	if inFlight > 0 {
		inFlight--
	}

	// If the node is draining and there are no more in-flight requests, notify any waiter.
	if draining && inFlight == 0 && drainDoneCh != nil {
		close(drainDoneCh)
		drainDoneCh = nil
	}

	drainMu.Unlock()
}

// StartDrain transitions the node into draining mode idempotently.
// It returns whether the node was already draining and the current in-flight count.
func StartDrain() (alreadyDraining bool, currentInFlight int64) {
	drainMu.Lock()
	defer drainMu.Unlock()

	already := draining
	if !draining {
		draining = true

		// If there are in-flight requests, create a channel to notify when they complete.
		if inFlight > 0 {
			drainDoneCh = make(chan struct{})
		}
	}

	return already, inFlight
}

// WaitForDrain blocks until all in-flight requests complete or ctx is canceled.
// If the node is not draining or already has zero in-flight requests, it returns immediately.
func WaitForDrain(ctx context.Context) error {
	drainMu.Lock()

	// If the node is not draining or there are no in-flight requests, return immediately.
	if !draining || inFlight == 0 {
		drainMu.Unlock()
		return nil
	}

	ch := drainDoneCh
	drainMu.Unlock()
	if ch == nil {
		return nil
	}

	// Wait for either the drain to complete or the context to be canceled.
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// IsDraining reports whether the node is currently draining.
func IsDraining() bool {
	drainMu.Lock()
	defer drainMu.Unlock()
	return draining
}

// InFlightRequests returns the current number of active requests on the node.
func InFlightRequests() int64 {
	drainMu.Lock()
	defer drainMu.Unlock()
	return inFlight
}

// ResetDrainState resets drain state and counters. Intended for unit tests.
func ResetDrainState() {
	drainMu.Lock()
	defer drainMu.Unlock()

	draining = false
	inFlight = 0
	drainDoneCh = nil
}
