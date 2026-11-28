package test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/serverledge-faas/serverledge/internal/api"
	"github.com/serverledge-faas/serverledge/internal/node"
	"github.com/serverledge-faas/serverledge/internal/registration"
	"github.com/serverledge-faas/serverledge/utils"
)

func TestNodeDrainLifecycleAndWait(t *testing.T) {
	api.ResetDrainControllerForTest()
	t.Cleanup(api.ResetDrainControllerForTest)

	if node.IsDraining() {
		t.Fatal("expected node not to be draining initially")
	}
	if !node.TryAcquireRequest() {
		t.Fatal("expected first request to be admitted before drain")
	}
	if !node.TryAcquireRequest() {
		t.Fatal("expected second request to be admitted before drain")
	}
	if got := node.InFlightRequests(); got != 2 {
		t.Fatalf("expected 2 in-flight requests, got %d", got)
	}

	already, inFlight := node.StartDrain()
	if already {
		t.Fatal("expected first StartDrain call to report alreadyDraining=false")
	}
	if inFlight != 2 {
		t.Fatalf("expected 2 in-flight requests at StartDrain, got %d", inFlight)
	}

	// Second call must be idempotent.
	already, inFlight = node.StartDrain()
	if !already || inFlight != 2 {
		t.Fatalf("expected idempotent StartDrain(true, 2), got (%v, %d)", already, inFlight)
	}

	// New requests must be rejected once draining.
	if node.TryAcquireRequest() {
		t.Fatal("expected new request to be rejected while draining")
	}

	// WaitForDrain must respect context timeout while requests are still active.
	shortCtx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := node.WaitForDrain(shortCtx); err == nil {
		t.Fatal("expected WaitForDrain to time out while 2 requests remain in flight")
	}

	// WaitForDrain must unblock promptly as soon as all in-flight requests finish.
	waitDone := make(chan error, 1)
	go func() {
		ctx, c := context.WithTimeout(context.Background(), 2*time.Second)
		defer c()
		waitDone <- node.WaitForDrain(ctx)
	}()

	node.ReleaseRequest()
	select {
	case err := <-waitDone:
		t.Fatalf("WaitForDrain returned early with 1 request still in flight: %v", err)
	case <-time.After(30 * time.Millisecond):
	}

	node.ReleaseRequest()
	select {
	case err := <-waitDone:
		if err != nil {
			t.Fatalf("expected WaitForDrain to succeed, got: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitForDrain did not unblock after last in-flight request completed")
	}

	if got := node.InFlightRequests(); got != 0 {
		t.Fatalf("expected 0 in-flight requests after drain, got %d", got)
	}
}

func TestDrainEndpointsAndStatusReporting(t *testing.T) {
	api.ResetDrainControllerForTest()
	t.Cleanup(api.ResetDrainControllerForTest)

	e := echo.New()

	// Simulate an active in-flight request before triggering POST /drain.
	if !node.TryAcquireRequest() {
		t.Fatal("failed to acquire initial in-flight request")
	}

	// Trigger POST /drain.
	drainReq := httptest.NewRequest(http.MethodPost, "/drain", strings.NewReader(`{}`))
	drainRec := httptest.NewRecorder()
	if err := api.DrainNode(e.NewContext(drainReq, drainRec)); err != nil {
		t.Fatalf("DrainNode returned error: %v", err)
	}
	if drainRec.Code != http.StatusOK {
		t.Fatalf("expected 200 from POST /drain, got %d: %s", drainRec.Code, drainRec.Body.String())
	}

	var drainResp api.DrainResponse
	if err := json.Unmarshal(drainRec.Body.Bytes(), &drainResp); err != nil {
		t.Fatalf("failed to decode DrainResponse: %v", err)
	}
	if !drainResp.Draining || drainResp.InFlightRequests != 1 || drainResp.Drained {
		t.Fatalf("unexpected DrainResponse while 1 request in flight: %+v", drainResp)
	}

	// GET /status must expose Draining=true and InFlightRequests=1.
	statusReq := httptest.NewRequest(http.MethodGet, "/status", nil)
	statusRec := httptest.NewRecorder()
	if err := api.GetServerStatus(e.NewContext(statusReq, statusRec)); err != nil {
		t.Fatalf("GetServerStatus returned error: %v", err)
	}
	if statusRec.Code != http.StatusOK {
		t.Fatalf("expected 200 from GET /status, got %d", statusRec.Code)
	}
	var statusInfo registration.StatusInformation
	if err := json.Unmarshal(statusRec.Body.Bytes(), &statusInfo); err != nil {
		t.Fatalf("failed to decode StatusInformation: %v", err)
	}
	if !statusInfo.Draining || statusInfo.InFlightRequests != 1 {
		t.Fatalf("unexpected StatusInformation during drain: draining=%v inFlight=%d",
			statusInfo.Draining, statusInfo.InFlightRequests)
	}

	// New invocations and prewarm requests must be rejected with 503 while draining.
	invReq := httptest.NewRequest(http.MethodPost, "/invoke/demo", strings.NewReader(`{}`))
	invRec := httptest.NewRecorder()
	invCtx := e.NewContext(invReq, invRec)
	invCtx.SetParamNames("fun")
	invCtx.SetParamValues("demo")
	if err := api.InvokeFunction(invCtx); err != nil {
		t.Fatalf("InvokeFunction returned error: %v", err)
	}
	if invRec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 for InvokeFunction while draining, got %d", invRec.Code)
	}

	prewarmReq := httptest.NewRequest(http.MethodPost, "/prewarm", strings.NewReader(`{"Function":"demo","Instances":1}`))
	prewarmRec := httptest.NewRecorder()
	if err := api.PrewarmFunction(e.NewContext(prewarmReq, prewarmRec)); err != nil {
		t.Fatalf("PrewarmFunction returned error: %v", err)
	}
	if prewarmRec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 for PrewarmFunction while draining, got %d", prewarmRec.Code)
	}

	// Complete the in-flight request and verify GET /drain reports Drained=true.
	node.ReleaseRequest()

	getDrainReq := httptest.NewRequest(http.MethodGet, "/drain", nil)
	getDrainRec := httptest.NewRecorder()
	if err := api.GetDrainStatus(e.NewContext(getDrainReq, getDrainRec)); err != nil {
		t.Fatalf("GetDrainStatus returned error: %v", err)
	}
	var finalDrain api.DrainResponse
	if err := json.Unmarshal(getDrainRec.Body.Bytes(), &finalDrain); err != nil {
		t.Fatalf("failed to decode final DrainResponse: %v", err)
	}
	if !finalDrain.Draining || finalDrain.InFlightRequests != 0 || !finalDrain.Drained {
		t.Fatalf("expected drained state after last request finished, got %+v", finalDrain)
	}
}

func TestWatchAreaNodesNotifiesOnWorkerChangesAndIgnoresLB(t *testing.T) {
	etcdClient, err := utils.GetEtcdClient()
	if err != nil {
		t.Skipf("etcd not available: %v", err)
	}

	area := "watch-test-area"
	watchCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events, err := registration.WatchAreaNodes(watchCtx, area)
	if err != nil {
		t.Fatalf("WatchAreaNodes failed: %v", err)
	}

	// Writing a load balancer key under __lb must NOT trigger a worker membership event.
	lbKey := "registry/" + area + "/__lb/amd64/lb-1"
	putCtx, putCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer putCancel()
	if _, err := etcdClient.Put(putCtx, lbKey, "127.0.0.1;1323;9876;amd64;amd64"); err != nil {
		t.Fatalf("failed to put lb key: %v", err)
	}
	t.Cleanup(func() {
		c, done := context.WithTimeout(context.Background(), 2*time.Second)
		defer done()
		_, _ = etcdClient.Delete(c, lbKey)
	})

	select {
	case <-events:
		t.Fatal("expected __lb key update to be ignored by WatchAreaNodes")
	case <-time.After(150 * time.Millisecond):
	}

	// Adding a worker node must trigger an event promptly.
	workerKey := publishTestPeer(t, area, "worker-1", "amd64", "amd64", "127.0.0.1:1324", 9877)
	select {
	case <-events:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for WatchAreaNodes event on worker PUT")
	}

	// Deleting the worker node must also trigger an event promptly.
	delCtx, delCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer delCancel()
	if _, err := etcdClient.Delete(delCtx, workerKey); err != nil {
		t.Fatalf("failed to delete worker key: %v", err)
	}
	select {
	case <-events:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for WatchAreaNodes event on worker DELETE")
	}
}

