package test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/serverledge-faas/serverledge/internal/api"
	"github.com/serverledge-faas/serverledge/internal/container"
	"github.com/serverledge-faas/serverledge/internal/function"
	"github.com/serverledge-faas/serverledge/internal/lb"
	"github.com/serverledge-faas/serverledge/internal/node"
	"github.com/serverledge-faas/serverledge/internal/workflow"
	"github.com/serverledge-faas/serverledge/utils"
	"github.com/spf13/cast"
)

// TestContainerPool executes repeatedly different functions (**not compositions**) to verify the container pool
func TestContainerPool(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}
	// creating inc and double functions
	pyFuncs := []string{"inc", "double"}
	for _, name := range pyFuncs {
		fn, err := InitializePyFunction(name, "handler", function.NewSignature().
			AddInput("n", function.Int{}).
			AddOutput("n", function.Int{}).
			Build())
		utils.AssertNil(t, err)

		createApiIfNotExistsTest(t, fn, HOST, PORT)
	}

	// executing function
	channel := make(chan error)
	const n = 3
	for i := 0; i < n; i++ {
		for _, name := range pyFuncs {
			x := make(map[string]interface{})
			x["n"] = 1
			fnName := name
			go func() {
				time.Sleep(5 * time.Second)
				err := invokeApiTest(fnName, x, HOST, PORT)
				channel <- err
			}()
		}
	}

	// wait for all functions to complete and checking the errors
	for i := 0; i < len(pyFuncs)*n; i++ {
		err := <-channel
		utils.AssertNil(t, err)
	}
	// delete each function
	for _, name := range pyFuncs {
		deleteApiTest(t, name, HOST, PORT)
	}
	//utils.AssertTrueMsg(t, workflow.IsEmptyPartialDataCache(), "partial data cache is not empty")
}

// TestCreateWorkflow tests the compose REST API that creates a new function composition
func TestCreateWorkflow(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}
	fcName := "sequence"
	oldW, found := workflow.Get(fcName)
	if found {
		oldW.Delete()
	}

	fn, err := InitializePyFunction("inc", "handler", function.NewSignature().
		AddInput("n", function.Int{}).
		AddOutput("n", function.Int{}).
		Build())
	utils.AssertNilMsg(t, err, "failed to initialize function")
	wflow, err := CreateSequenceWorkflow(fn, fn, fn)
	wflow.Name = fcName
	utils.AssertNil(t, err)
	err = createWorkflowApiTest(wflow, HOST, PORT)
	if err != nil {
		fmt.Println(err)
		t.Fail()
	}

	// here we do not use REST API
	getFC, b := workflow.Get(fcName)
	utils.AssertTrue(t, b)
	utils.AssertTrueMsg(t, wflow.Equals(getFC), "composition comparison failed")
	err = wflow.Delete()
	utils.AssertNilMsg(t, err, "failed to delete composition")

}

// TestInvokeWorkflow tests the REST API that executes a given function composition
func TestInvokeWorkflow(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}
	fcName := "sequence"
	fn, err := initializeJsFunction("inc", function.NewSignature().
		AddInput("n", function.Int{}).
		AddOutput("n", function.Int{}).
		Build())
	utils.AssertNilMsg(t, err, "failed to initialize function")
	wflow, err := CreateSequenceWorkflow(fn, fn, fn)
	wflow.Name = fcName
	utils.AssertNil(t, err)
	err = createWorkflowApiTest(wflow, HOST, PORT)
	if err != nil {
		fmt.Println(err)
		t.Fail()
	}

	// === this is the test ===
	params := make(map[string]interface{})
	params["n"] = 1
	invokeWorkflowApiTest(t, params, fcName, HOST, PORT, false)

	// here we do not use REST API
	getFC, b := workflow.Get(fcName)
	utils.AssertTrue(t, b)
	utils.AssertTrueMsg(t, wflow.Equals(getFC), "composition comparison failed")
	err = wflow.Delete()
	utils.AssertNilMsg(t, err, "failed to delete composition")

}

// TestInvokeWorkflow tests the REST API that executes a given function composition
func TestInvokeWorkflow_DifferentFunctions(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}
	fcName := "sequence"
	fnJs, err := initializeJsFunction("inc", function.NewSignature().
		AddInput("n", function.Int{}).
		AddOutput("n", function.Int{}).
		Build())
	utils.AssertNilMsg(t, err, "failed to initialize javascript function")
	fnPy, err := InitializePyFunction("double", "handler", function.NewSignature().
		AddInput("n", function.Int{}).
		AddOutput("n", function.Int{}).
		Build())
	utils.AssertNilMsg(t, err, "failed to initialize python function")
	wflow, err := CreateSequenceWorkflow(fnPy, fnJs, fnPy, fnJs)
	wflow.Name = fcName
	utils.AssertNil(t, err)
	err = createWorkflowApiTest(wflow, HOST, PORT)
	if err != nil {
		fmt.Println(err)
		t.Fail()
	}

	// === this is the test ===
	params := make(map[string]interface{})
	params["n"] = 1
	invokeWorkflowApiTest(t, params, fcName, HOST, PORT, false)

	// here we do not use REST API
	getFC, b := workflow.Get(fcName)
	utils.AssertTrue(t, b)
	utils.AssertTrueMsg(t, wflow.Equals(getFC), "composition comparison failed")
	err = wflow.Delete()
	utils.AssertNilMsg(t, err, "failed to delete composition")

}

// TestDeleteWorkflow tests the compose REST API that deletes a function composition
func TestDeleteWorkflow(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}
	fcName := "sequence"
	fn, err := InitializePyFunction("inc", "handler", function.NewSignature().
		AddInput("n", function.Int{}).
		AddOutput("n", function.Int{}).
		Build())
	if err != nil {
		fmt.Printf("inc creation failed: %v\n", err)
		t.Fail()
	}
	db, err := InitializePyFunction("double", "handler", function.NewSignature().
		AddInput("n", function.Int{}).
		AddOutput("n", function.Int{}).
		Build())
	utils.AssertNilMsg(t, err, "failed to initialize function")
	wflow, err := CreateSequenceWorkflow(fn, db, fn)
	wflow.Name = fcName
	utils.AssertNil(t, err)

	err = wflow.Save()
	utils.AssertNil(t, err)

	// the API under test is the following
	deleteWorkflowApiTest(t, fcName, HOST, PORT)

	list, err := workflow.GetAllWorkflows()
	found := false
	for _, w := range list {
		if strings.Compare(w, wflow.Name) == 0 {
			found = true
		}
	}
	utils.AssertFalse(t, found)
}

// TestAsyncInvokeWorkflow tests the REST API that executes a given function composition
func TestAsyncInvokeWorkflow(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}
	fcName := "sequence"

	fn, err := InitializePyFunction("inc", "handler", function.NewSignature().
		AddInput("n", function.Int{}).
		AddOutput("n", function.Int{}).
		Build())
	utils.AssertNilMsg(t, err, "failed to initialize function")
	wflow, err := CreateSequenceWorkflow(fn, fn, fn)
	wflow.Name = fcName
	utils.AssertNil(t, err)
	err = createWorkflowApiTest(wflow, HOST, PORT)
	if err != nil {
		fmt.Println(err)
		t.Fail()
	}

	// === this is the test ===
	params := make(map[string]interface{})
	params["n"] = 1
	invocationResult := invokeWorkflowApiTest(t, params, fcName, HOST, PORT, true)

	reqIdStruct := &function.AsyncResponse{}

	errUnmarshal := json.Unmarshal([]byte(invocationResult), reqIdStruct)
	utils.AssertNil(t, errUnmarshal)

	// wait until the result is available
	i := 0
	for {
		pollResult := pollWorkflowTest(t, reqIdStruct.ReqId, HOST, PORT)

		var response workflow.InvocationResponse
		errUnmarshalExecResult := json.Unmarshal([]byte(pollResult), &response)

		if errUnmarshalExecResult != nil {
			var unmarshalError *json.UnmarshalTypeError
			if errors.As(errUnmarshalExecResult, &unmarshalError) {
				utils.AssertFalseMsg(t, true, errUnmarshalExecResult.Error())
			}
			i++
			time.Sleep(200 * time.Millisecond)
		} else {
			utils.AssertEquals(t, 4, cast.ToInt(response.Result["n"]))
			break
		}
	}

	err = wflow.Delete()
	utils.AssertNilMsg(t, err, "failed to delete composition")
}

// TestMismatchingArch tests that the execution fails if the node's architecture doesn't support the function's one
// (and offloading is disabled)
func TestMismatchingArchNoOffload(t *testing.T) {

	name := "double"
	fn, err := InitializePyFunction(name, "handler", function.NewSignature().
		AddInput("input", function.Int{}).
		AddOutput("result", function.Int{}).
		Build())
	utils.AssertNil(t, err)
	currentArch := runtime.GOARCH
	for i, arch := range fn.SupportedArchs {
		if arch == currentArch {
			fn.SupportedArchs = append(fn.SupportedArchs[:i], fn.SupportedArchs[i+1:]...)
		}
	}

	createApiIfNotExistsTest(t, fn, HOST, PORT)

	// executing all functions
	x := make(map[string]interface{})
	x["input"] = 1
	fnName := name

	time.Sleep(50 * time.Millisecond)
	err = invokeApiTestSetOffloading(fnName, x, HOST, PORT, false) // no offloading
	utils.AssertNonNil(t, err)                                     // Expecting an error due to mismatching architecture

	// delete function
	deleteApiTest(t, name, HOST, PORT)
}

// TestFunctionAPIRejectsNonPositiveCPU covers #2 at the HTTP handler boundary.
// A zero or negative CPU limit must return 422 before looking up the function or
// contacting Docker; otherwise invalid declarations can corrupt CPU accounting.
func TestFunctionAPIRejectsNonPositiveCPU(t *testing.T) {
	for _, cpu := range []string{"0", "-1"} {
		t.Run(cpu, func(t *testing.T) {
			// The update route bypasses the creation conflict check; the CPU value is
			// the only invalid field and must be rejected before any persistence.
			req := httptest.NewRequest(http.MethodPost, "/update", strings.NewReader(`{"Name":"cpu-test","Runtime":"python314","MemoryMB":128,"CPUDemand":`+cpu+`}`))
			rec := httptest.NewRecorder()
			ctx := echo.New().NewContext(req, rec)
			ctx.SetPath("/update")
			if err := api.CreateOrUpdateFunction(ctx); err != nil {
				t.Fatal(err)
			}
			if rec.Code != http.StatusUnprocessableEntity {
				t.Fatalf("expected 422, got %d: %s", rec.Code, rec.Body.String())
			}
		})
	}
}

// TestWarmCPURejectionPreservesIdleContainer checks #1 against the shared pool.
// Dedicated containers are returned to idle in a known order. A CPU rejection
// must preserve inventory and accounting, and later reuse must remain LIFO.
func TestWarmCPURejectionPreservesIdleContainer(t *testing.T) {
	requirePlatformProfile(t, "default")
	waitForIdlePool(t)
	initial := readPoolUsage()
	if initial.availableCPU <= 0 || initial.freeMemory < 257 {
		t.Fatal("warm scenario requires positive CPU and 257 MiB of free shared memory")
	}
	f := &function.Function{Name: "warm-cpu-check", Runtime: "python314", CPUDemand: initial.availableCPU / 2, MemoryMB: 128, MaxConcurrency: 1}
	containers := map[string]*container.Container{}
	active := map[string]bool{}
	t.Cleanup(func() {
		// Only these containers belong to this case; other warm pools stay intact.
		for id, cont := range containers {
			if active[id] {
				node.HandleCompletion(cont, f)
			}
		}
		node.ShutdownWarmContainersFor(f)
		for id := range containers {
			waitForPlatformCondition(t, "destruction of test container "+id, 5*time.Second, func() bool { _, err := container.GetMemoryMB(id); return err != nil })
		}
	})
	if count, err := node.PrewarmInstances(f, 2, false); err != nil || count != 2 {
		t.Fatalf("cannot prepare warm containers: count=%d, error=%v", count, err)
	}
	first, _, err := node.AcquireContainer(f, true)
	if err != nil {
		t.Fatal(err)
	}
	containers[first.ID] = first
	active[first.ID] = true
	last, _, err := node.AcquireContainer(f, true)
	if err != nil {
		t.Fatal(err)
	}
	containers[last.ID] = last
	active[last.ID] = true
	// This completion order defines the idle stack without reading private slices.
	node.HandleCompletion(first, f)
	active[first.ID] = false
	node.HandleCompletion(last, f)
	active[last.ID] = false
	blocker := &function.Function{Name: "cpu-reservation", Runtime: "invalid", CPUDemand: readPoolUsage().availableCPU, MemoryMB: 1}
	if !node.AcquireResourcesForNewContainer(blocker, false) {
		t.Fatal("cannot reserve CPU for rejection scenario")
	}
	reserved := true
	releaseBlocker := func() {
		if reserved {
			// The unknown runtime fails before Docker access and rolls back this reservation.
			node.NewContainerWithAcquiredResources(blocker, false, false)
			reserved = false
		}
	}
	t.Cleanup(releaseBlocker)
	beforeRejection := readPoolUsage()
	if _, _, err := node.AcquireContainer(f, true); !errors.Is(err, node.OutOfResourcesErr) {
		t.Fatalf("expected CPU rejection, got %v", err)
	}
	if node.WarmStatus()[f.Name] != 2 || !samePoolUsage(readPoolUsage(), beforeRejection) {
		t.Fatal("CPU rejection changed idle inventory or resource accounting")
	}
	releaseBlocker()
	got, _, err := node.AcquireContainer(f, true)
	if err != nil {
		t.Fatal(err)
	}
	active[got.ID] = true
	if got.ID != last.ID {
		t.Fatal("warm reuse did not preserve LIFO identity")
	}
}

// TestCreationFailureRollsBackMatchingReservation covers #1 for busy and warm
// reservations. A runtime failure must restore the starting shared counters,
// without assuming that other functions have no idle containers.
func TestCreationFailureRollsBackMatchingReservation(t *testing.T) {
	requirePlatformProfile(t, "default")
	waitForIdlePool(t)
	for _, prewarm := range []bool{false, true} {
		t.Run(map[bool]string{false: "busy", true: "prewarm"}[prewarm], func(t *testing.T) {
			before := readPoolUsage()
			if before.freeMemory < 1 || before.availableCPU < 0.1 {
				t.Fatal("insufficient shared capacity for rollback scenario")
			}
			f := &function.Function{Name: "bad-runtime", Runtime: "invalid", CPUDemand: 0.1, MemoryMB: 1}
			if !node.AcquireResourcesForNewContainer(f, prewarm) {
				t.Fatal("initial reservation failed")
			}
			if _, err := node.NewContainerWithAcquiredResources(f, prewarm, false); err == nil {
				t.Fatal("expected creation failure")
			}
			if after := readPoolUsage(); !samePoolUsage(after, before) {
				t.Fatalf("creation failure changed shared resources: before=%+v after=%+v", before, after)
			}
		})
	}
}

// TestAsyncCreationFailureReleasesResourcesBeforeCallback covers #1.
// The callback snapshots the shared counters under their lock: it must observe
// rollback already completed and must not block on a lock held by its caller.
func TestAsyncCreationFailureReleasesResourcesBeforeCallback(t *testing.T) {
	requirePlatformProfile(t, "default")
	waitForIdlePool(t)
	before := readPoolUsage()
	if before.freeMemory < 1 || before.availableCPU < 0.1 {
		t.Fatal("insufficient shared capacity for asynchronous rollback")
	}
	f := &function.Function{Name: "bad-async", Runtime: "invalid", CPUDemand: 0.1, MemoryMB: 1}
	if !node.AcquireResourcesForNewContainer(f, false) {
		t.Fatal("initial reservation failed")
	}
	done := make(chan bool, 1)
	node.NewContainerWithAcquiredResourcesAsync(f, func(*container.Container) { done <- false }, func(error) { done <- samePoolUsage(readPoolUsage(), before) })
	select {
	case released := <-done:
		if !released {
			t.Fatal("callback observed a missing resource rollback")
		}
	case <-time.After(time.Second):
		t.Fatal("error callback blocked")
	}
}

// TestInvalidCPUReservationIsRejected complements HTTP validation for #2.
// Direct callers can supply NaN or infinity. Invalid demand must leave the
// existing shared counters unchanged, without reinitializing LocalResources.
func TestInvalidCPUReservationIsRejected(t *testing.T) {
	requirePlatformProfile(t, "default")
	waitForIdlePool(t)
	before := readPoolUsage()
	for _, cpu := range []float64{0, -1, math.NaN(), math.Inf(1), math.Inf(-1)} {
		f := &function.Function{Name: "invalid-cpu", CPUDemand: cpu, MemoryMB: 1}
		if node.AcquireResourcesForNewContainer(f, false) {
			t.Errorf("accepted invalid CPU demand %v", cpu)
		}
		if after := readPoolUsage(); !samePoolUsage(after, before) {
			t.Errorf("invalid CPU demand %v changed reservations: %+v", cpu, after)
		}
	}
}

// publishTestPeer uses the registry's wire format instead of modifying private maps.
// It returns the exact etcd key so tests can explicitly replace a same-name peer.
func publishTestPeer(t *testing.T, area, name, tag, arch, address string, udpPort int) string {
	t.Helper()
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatal(err)
	}
	client, err := utils.GetEtcdClient()
	if err != nil {
		t.Fatal(err)
	}
	key := fmt.Sprintf("registry/%s/%s/%s", area, tag, name)
	payload := fmt.Sprintf("%s;%s;%d;%s;%s", host, port, udpPort, arch, tag)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := client.Put(ctx, key, payload); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if _, err := client.Delete(ctx, key); err != nil {
			t.Errorf("cannot remove test registration: %v", err)
		}
	})
	return key
}

// TestTargetRefreshPollsOnceAndReplacesChangedMetadata covers #4 through the
// running reverse proxy and etcd discovery. Consecutive retained-target polls
// must belong to different refresh periods, and a same-name replacement must
// receive both status queries and proxied traffic at its new URL and routing tag.
func TestTargetRefreshPollsOnceAndReplacesChangedMetadata(t *testing.T) {
	requirePlatformProfile(t, "lb")
	polls := make(chan time.Time, 16)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/status" {
			polls <- time.Now()
			io.WriteString(w, `{"TotalCPU":2,"AvailableMemory":1024,"TotalMemory":1024,"LastUpdateTime":1}`)
			return
		}
		io.WriteString(w, "old-backend")
	}))
	defer backend.Close()
	area := "lb-refresh-case"
	oldKey := publishTestPeer(t, area, "refresh-target", "pool-a", "amd64", strings.TrimPrefix(backend.URL, "http://"), 0)
	listener, err := net.Listen("tcp", HOST+":0")
	if err != nil {
		t.Fatal(err)
	}
	proxy := echo.New()
	proxy.Listener = listener
	go lb.StartReverseProxy(proxy, area)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := proxy.Shutdown(ctx); err != nil {
			t.Errorf("cannot stop test proxy: %v", err)
		}
	})

	// Construction polls once; the following two samples are periodic refreshes.
	var samples []time.Time
	for len(samples) < 3 {
		select {
		case instant := <-polls:
			samples = append(samples, instant)
		case <-time.After(3 * time.Second):
			t.Fatal("load balancer did not refresh target status")
		}
	}
	if gap := samples[2].Sub(samples[1]); gap < 800*time.Millisecond {
		t.Fatalf("duplicate status queries within one refresh: gap=%v", gap)
	}

	var newPolls atomic.Int32
	replacement := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/status" {
			newPolls.Add(1)
			io.WriteString(w, `{"TotalCPU":2,"AvailableMemory":1024,"TotalMemory":1024,"LastUpdateTime":2}`)
			return
		}
		io.WriteString(w, "new-backend")
	}))
	defer replacement.Close()
	client, err := utils.GetEtcdClient()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := client.Delete(ctx, oldKey); err != nil {
		t.Fatal(err)
	}
	publishTestPeer(t, area, "refresh-target", "pool-b", "arm64", strings.TrimPrefix(replacement.URL, "http://"), 0)
	probe := &http.Client{Timeout: time.Second}
	waitForPlatformCondition(t, "new target routing and status", 4*time.Second, func() bool {
		response, err := probe.Get("http://" + listener.Addr().String() + "/health")
		if err != nil {
			return false
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		return err == nil && string(body) == "new-backend" && newPolls.Load() > 0
	})
}
