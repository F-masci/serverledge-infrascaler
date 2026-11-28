package api

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/internal/node"
	"github.com/serverledge-faas/serverledge/internal/registration"
)

const defaultDrainTimeoutSeconds = 30

// DrainRequest configures an HTTP drain request on POST /drain.
type DrainRequest struct {
	TimeoutSeconds int  `json:"timeout_seconds,omitempty"`
	Shutdown       bool `json:"shutdown,omitempty"`
}

// DrainResponse reports the current drain state of the node.
type DrainResponse struct {
	Draining         bool  `json:"draining"`
	Deregistered     bool  `json:"deregistered"`
	InFlightRequests int64 `json:"in_flight_requests"`
	Drained          bool  `json:"drained"`
	ShutdownStarted  bool  `json:"shutdown_started,omitempty"`
}

var (
	shutdownMu      sync.Mutex
	shutdownStarted bool
)

func defaultDrainTimeout() time.Duration {
	secs := config.GetInt(config.NODE_DRAIN_TIMEOUT_S, defaultDrainTimeoutSeconds)
	if secs <= 0 {
		secs = defaultDrainTimeoutSeconds
	}
	return time.Duration(secs) * time.Second
}

// DrainNode handles POST /drain.
// It transitions the node to draining mode, immediately deregisters it from etcd,
// and returns the current in-flight request count. If shutdown=true is passed,
// the node will wait for in-flight requests (up to timeout_seconds) and then
// cleanly stop containers and shut down the server.
func DrainNode(c echo.Context) error {
	var req DrainRequest

	reqBody := c.Request().Body
	if reqBody != nil {
		if err := json.NewDecoder(reqBody).Decode(&req); err != nil && err != io.EOF {
			return c.String(http.StatusBadRequest, "invalid drain request payload")
		}
	}

	node.StartDrain()

	deregErr := registration.Deregister()
	if deregErr != nil {
		log.Printf("[Drain] Failed to deregister node from etcd: %v", deregErr)
	}

	var triggeredShutdown bool
	if req.Shutdown {
		timeout := defaultDrainTimeout()
		if req.TimeoutSeconds > 0 {
			timeout = time.Duration(req.TimeoutSeconds) * time.Second
		}
		shutdownMu.Lock()
		if !shutdownStarted {
			shutdownStarted = true
			triggeredShutdown = true
			e := c.Echo()

			go func() {
				GracefulShutdown(e, timeout)
				os.Exit(0)
			}()
		} else {
			triggeredShutdown = true
		}
		shutdownMu.Unlock()
	} else {
		shutdownMu.Lock()
		triggeredShutdown = shutdownStarted
		shutdownMu.Unlock()
	}

	inFlight := node.InFlightRequests()
	resp := DrainResponse{
		Draining:         node.IsDraining(),
		Deregistered:     deregErr == nil,
		InFlightRequests: inFlight,
		Drained:          node.IsDraining() && inFlight == 0,
		ShutdownStarted:  triggeredShutdown,
	}

	return c.JSON(http.StatusOK, resp)
}

// GetDrainStatus handles GET /drain and reports the node's current drain state.
func GetDrainStatus(c echo.Context) error {
	shutdownMu.Lock()
	shuttingDown := shutdownStarted
	shutdownMu.Unlock()

	draining := node.IsDraining()
	inFlight := node.InFlightRequests()
	resp := DrainResponse{
		Draining:         draining,
		Deregistered:     !registration.IsRegistered(),
		InFlightRequests: inFlight,
		Drained:          draining && inFlight == 0,
		ShutdownStarted:  shuttingDown,
	}
	return c.JSON(http.StatusOK, resp)
}

// GracefulShutdown performs an ordered node shutdown:
// 1. Enter draining mode (reject new invocations)
// 2. Deregister from etcd so the LB and peers stop routing to this node
// 3. Wait for in-flight requests to finish (up to drainTimeout)
// 4. Shut down containers, stop the janitor, and shut down the Echo HTTP server
func GracefulShutdown(e *echo.Echo, drainTimeout time.Duration) {
	node.StartDrain()

	if err := registration.Deregister(); err != nil {
		log.Printf("[Shutdown] Error deregistering from etcd: %v", err)
	}

	if drainTimeout <= 0 {
		drainTimeout = defaultDrainTimeout()
	}
	drainCtx, drainCancel := context.WithTimeout(context.Background(), drainTimeout)
	defer drainCancel()

	if err := node.WaitForDrain(drainCtx); err != nil {
		log.Printf("[Shutdown] Drain timed out after %v with %d in-flight requests remaining", drainTimeout, node.InFlightRequests())
	} else {
		log.Printf("[Shutdown] Drain completed with 0 in-flight requests")
	}

	node.ShutdownAllContainers()
	node.StopJanitor()

	if e != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := e.Shutdown(ctx); err != nil {
			log.Printf("[Shutdown] Error shutting down HTTP server: %v", err)
		}
	}
}

// ResetDrainControllerForTest resets internal shutdown state for unit tests.
func ResetDrainControllerForTest() {
	shutdownMu.Lock()
	shutdownStarted = false
	shutdownMu.Unlock()
	node.ResetDrainState()
}
