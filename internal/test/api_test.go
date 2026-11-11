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
	"net/url"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hexablock/vivaldi"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/serverledge-faas/serverledge/internal/api"
	"github.com/serverledge-faas/serverledge/internal/cache"
	"github.com/serverledge-faas/serverledge/internal/container"
	"github.com/serverledge-faas/serverledge/internal/executor"
	"github.com/serverledge-faas/serverledge/internal/function"
	"github.com/serverledge-faas/serverledge/internal/lb"
	"github.com/serverledge-faas/serverledge/internal/mab"
	"github.com/serverledge-faas/serverledge/internal/node"
	"github.com/serverledge-faas/serverledge/internal/registration"
	"github.com/serverledge-faas/serverledge/internal/scheduling"
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

// TestEdgeOnlyLocalRejectionsComplete checks the externally observable #5 paths.
// Architecture mismatch and insufficient CPU must both terminate with a resource
// rejection. Unlike TestMismatchingArchNoOffload, which uses the shared default
// policy, this case requires the EdgePolicy selected by TestMain. It checks public scheduling
// results, not the private decision count.
func TestEdgeOnlyLocalRejectionsComplete(t *testing.T) {
	requirePlatformProfile(t, "edge")
	for _, scenario := range []struct {
		name, arch string
		cpu        float64
	}{
		{"architecture", "unsupported", 1}, {"capacity", runtime.GOARCH, 1e9},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			request := &function.Request{Ctx: context.WithValue(context.Background(), "ReqId", t.Name()), Arrival: time.Now(), Fun: &function.Function{Name: t.Name(), SupportedArchs: []string{scenario.arch}, CPUDemand: scenario.cpu, MemoryMB: 128}}
			done := make(chan error, 1)
			go func() { _, err := scheduling.SubmitRequest(request); done <- err }()
			select {
			case err := <-done:
				if !errors.Is(err, node.OutOfResourcesErr) {
					t.Fatalf("expected local rejection, got %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("local rejection did not complete")
			}
		})
	}
}

// invocationTransport provides controlled executor responses for retry scenarios.
// The Docker client uses its own transport; only executor HTTP calls use this one.
type invocationTransport func(*http.Request) (*http.Response, error)

func (f invocationTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// TestExecutorRetryPreservesRequestBody covers #9 through container.Execute.
// A small Docker API fixture supplies an address, then the executor transport
// consumes the payload and fails once. The second attempt must carry identical data.
func TestExecutorRetryPreservesRequestBody(t *testing.T) {
	requirePlatformProfile(t, "executor")
	dockerAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if !strings.HasSuffix(r.URL.Path, "/containers/retry-test/json") {
			http.Error(w, "unexpected Docker API request", http.StatusNotFound)
			return
		}
		io.WriteString(w, `{"Id":"retry-test","NetworkSettings":{"IPAddress":"127.0.0.1"}}`)
	}))
	defer dockerAPI.Close()
	t.Setenv("DOCKER_HOST", dockerAPI.URL)
	t.Setenv("DOCKER_API_VERSION", "1.47")
	t.Setenv("DOCKER_TLS_VERIFY", "")
	container.InitDockerContainerFactory()
	previous := http.DefaultTransport
	t.Cleanup(func() { http.DefaultTransport = previous })
	var bodies []string
	http.DefaultTransport = invocationTransport(func(r *http.Request) (*http.Response, error) {
		payload, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		r.Body.Close()
		bodies = append(bodies, string(payload))
		if len(bodies) == 1 {
			return nil, errors.New("connection lost after body consumption")
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"Success":true,"Result":"ok"}`)), Request: r}, nil
	})
	request := &executor.InvocationRequest{Params: map[string]interface{}{"value": 42}}
	result, _, err := container.Execute("retry-test", request)
	if err != nil || result == nil || !result.Success {
		t.Fatalf("executor retry failed: result=%v, error=%v", result, err)
	}
	if len(bodies) != 2 || bodies[0] == "" || bodies[0] != bodies[1] {
		t.Fatalf("retry changed payload: %q", bodies)
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

// TestNearestNeighborsContainOnlyRegisteredNodes covers #10 via real discovery
// and UDP status responses. With exactly two known peers, the two selected keys
// must identify those peers; zero-filled buffer entries used to produce empty keys.
func TestNearestNeighborsContainOnlyRegisteredNodes(t *testing.T) {
	requirePlatformProfile(t, "discovery")
	area := registration.SelfRegistration.Area
	for index, name := range []string{"near", "far"} {
		peer, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP(HOST)})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { peer.Close() })
		coordinateConfig := vivaldi.DefaultConfig()
		coordinateConfig.Dimensionality = 3
		coordinate := vivaldi.NewCoordinate(coordinateConfig)
		coordinate.Vec[0] = float64(1 + index*9)
		payload, err := json.Marshal(registration.StatusInformation{Coordinates: *coordinate, TotalCPU: 2, TotalMemory: 1024, AvailableMemory: 1024})
		if err != nil {
			t.Fatal(err)
		}
		// Each fixture answers the same one-byte status protocol as a real peer.
		go func() {
			buffer := make([]byte, 32)
			for {
				_, address, err := peer.ReadFromUDP(buffer)
				if err != nil {
					return
				}
				peer.WriteToUDP(payload, address)
			}
		}()
		key := publishTestPeer(t, area, name, "pool", runtime.GOARCH, HOST+":1", peer.LocalAddr().(*net.UDPAddr).Port)
		// Remove discovery membership before closing the UDP responder, so the
		// shared monitor can finish polling this peer instead of waiting on it.
		t.Cleanup(func() {
			client, err := utils.GetEtcdClient()
			if err != nil {
				t.Error(err)
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if _, err := client.Delete(ctx, key); err != nil {
				t.Error(err)
				return
			}
			waitForPlatformCondition(t, "removal of peer "+name, 4*time.Second, func() bool {
				_, found := registration.GetFullNeighborInfo()[name]
				return registration.GetPeerFromKey(name) == nil && !found
			})
		})
	}
	waitForPlatformCondition(t, "two registered nearest neighbors", 5*time.Second, func() bool {
		selected := registration.GetNearestNeighbors()
		if len(selected) != 2 {
			return false
		}
		keys := map[string]bool{}
		for _, peer := range selected {
			keys[peer.Key] = true
		}
		return keys["near"] && keys["far"]
	})
}

// lbTargetSpec separates the routing pool from the node's physical architecture.
// Multiple tags may share one architecture; this is the contract that the retired
// fixed ARM/x86 balancer could not represent.
type lbTargetSpec struct {
	name string
	tag  string
	arch string
}

// generalLBFixture exercises the public balancer with HTTP status responses.
// Resource admission uses the real metric cache and MemoryChecker, rather than
// replacing private hash rings with a test-only implementation.
type generalLBFixture struct {
	balancer *lb.GeneralLoadBalancer
	targets  []*middleware.ProxyTarget
	endpoint *url.URL
}

func newGeneralLBFixture(t *testing.T, specs ...lbTargetSpec) *generalLBFixture {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/status" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(registration.StatusInformation{TotalMemory: 1000000, AvailableMemory: 1000000, TotalCPU: 1000, LastUpdateTime: time.Now().Unix()})
	}))
	t.Cleanup(server.Close)
	endpoint, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	fixture := &generalLBFixture{endpoint: endpoint}
	for _, spec := range specs {
		fixture.targets = append(fixture.targets, fixture.target(t, spec))
	}
	fixture.balancer = lb.NewGeneralLoadBalancer(fixture.targets)
	// Remove only these targets and metrics before closing the status responder.
	t.Cleanup(func() {
		for _, target := range fixture.targets {
			fixture.balancer.RemoveTarget(target.Name)
		}
	})
	return fixture
}

func (f *generalLBFixture) target(t *testing.T, spec lbTargetSpec) *middleware.ProxyTarget {
	t.Helper()
	metadata := echo.Map{"arch": spec.arch}
	if spec.tag != "" {
		metadata["machine_tag"] = spec.tag
	}
	return &middleware.ProxyTarget{Name: strings.ReplaceAll(t.Name(), "/", "-") + "-" + spec.name, URL: f.endpoint, Meta: metadata}
}

func (f *generalLBFixture) add(t *testing.T, spec lbTargetSpec) *middleware.ProxyTarget {
	t.Helper()
	target := f.target(t, spec)
	if !f.balancer.AddTarget(target) {
		t.Fatalf("cannot add target %s", target.Name)
	}
	f.targets = append(f.targets, target)
	return target
}

func generalLBFunction(t *testing.T, architectures []string, pattern string) *function.Function {
	t.Helper()
	fun := &function.Function{Name: "general-" + strings.ReplaceAll(t.Name(), "/", "-"), SupportedArchs: architectures, TagPattern: pattern, MemoryMB: 1, CPUDemand: 0.001}
	// Cache the function explicitly: selection should not depend on an etcd lookup.
	cache.GetCacheInstance().Set(fun.Name, fun, time.Minute)
	t.Cleanup(func() { cache.GetCacheInstance().Delete(fun.Name) })
	return fun
}

func (f *generalLBFixture) invoke(t *testing.T, fun *function.Function) *middleware.ProxyTarget {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/invoke/"+fun.Name, nil)
	context := echo.New().NewContext(request, httptest.NewRecorder())
	context.SetPath("/invoke/" + fun.Name)
	target := f.balancer.Next(context)
	// These tests select targets but do not execute functions. Close a pending MAB
	// selection without reward feedback so successful selections do not leak InFlight.
	if decision, ok := mab.GlobalDecisionStorage.RetrieveAndDelete(request.Header.Get("Serverledge-MAB-Request-ID")); ok {
		mab.GlobalBanditManager.GetBandit(decision.FunctionName).ResolveSelection(decision.SelectedArm, "", decision.Context, nil, nil)
	}
	return target
}

func (f *generalLBFixture) ordinary() *middleware.ProxyTarget {
	context := echo.New().NewContext(httptest.NewRequest(http.MethodGet, "/health", nil), httptest.NewRecorder())
	context.SetPath("/health")
	return f.balancer.Next(context)
}

func setLBResources(target *middleware.ProxyTarget, memory int64, cpu float64) {
	lb.NodeMetrics.Update(target.Name, memory, 1000000, time.Now().Unix(), cpu)
}

// TestGeneralLoadBalancer replaces the retired fixed-ring suite using public APIs.
// Construction, mutation, hashing, compatibility and fallback are checked against
// three pools, including two different pools with the same physical architecture.
// The profile selects RoundRobin, Random or MAB once before the suite starts.
func TestGeneralLoadBalancer(t *testing.T) {
	requirePlatformProfile(t, "lb")
	specs := []lbTargetSpec{{"arm", "arm/tiny", "arm64"}, {"x86-small", "x86/tiny", "amd64"}, {"x86-large", "x86/large", "amd64"}}
	t.Run("dynamic_rings", func(t *testing.T) {
		fixture := newGeneralLBFixture(t, specs...)
		tags := map[string]bool{}
		// Non-invocation routing visits every nonempty tag, even in MAB/Random mode.
		for i := 0; i < 3; i++ {
			target := fixture.ordinary()
			if target == nil {
				t.Fatal("missing target in nonempty pool")
			}
			tags[target.Meta["machine_tag"].(string)] = true
		}
		if len(tags) != 3 {
			t.Fatalf("expected three distinct pools, got %v", tags)
		}
	})
	t.Run("add_remove", func(t *testing.T) {
		fixture := newGeneralLBFixture(t, specs...)
		fourth := fixture.add(t, lbTargetSpec{"gpu", "x86/gpu", "amd64"})
		tags := map[string]bool{}
		for i := 0; i < 4; i++ {
			target := fixture.ordinary()
			if target == nil {
				t.Fatal("missing dynamically added pool")
			}
			tags[target.Meta["machine_tag"].(string)] = true
		}
		if len(tags) != 4 {
			t.Fatalf("expected four pools after addition, got %v", tags)
		}
		for _, target := range fixture.targets[:3] {
			if !fixture.balancer.RemoveTarget(target.Name) {
				t.Fatalf("could not remove %s", target.Name)
			}
		}
		for i := 0; i < 4; i++ {
			if got := fixture.ordinary(); got == nil || got.Name != fourth.Name {
				t.Fatalf("empty rings were not skipped: %v", got)
			}
		}
		if fixture.balancer.RemoveTarget("unknown-target") {
			t.Fatal("unknown target reported as removed")
		}
	})
	t.Run("architecture_compatibility", func(t *testing.T) {
		for _, scenario := range []struct {
			name, arch string
			available  bool
		}{{"arm", "arm64", true}, {"x86", "amd64", true}, {"unsupported", "s390x", false}} {
			t.Run(scenario.name, func(t *testing.T) {
				fixture := newGeneralLBFixture(t, specs...)
				fun := generalLBFunction(t, []string{scenario.arch}, "")
				target := fixture.invoke(t, fun)
				if !scenario.available {
					if target != nil {
						t.Fatal("unsupported architecture received a target")
					}
					return
				}
				if target == nil || target.Meta["arch"] != scenario.arch {
					t.Fatalf("selected incompatible architecture: %v", target)
				}
			})
		}
	})
	t.Run("stable_hash_within_pool", func(t *testing.T) {
		fixture := newGeneralLBFixture(t, specs...)
		fixture.add(t, lbTargetSpec{"x86-large-second", "x86/large", "amd64"})
		fun := generalLBFunction(t, []string{"amd64"}, "x86/large")
		first := fixture.invoke(t, fun)
		if first == nil {
			t.Fatal("no compatible target")
		}
		// Pool selection can alternate for an unconstrained function. Pin one pool to
		// verify the actual consistent-hash guarantee rather than accidental stickiness.
		for i := 0; i < 20; i++ {
			if target := fixture.invoke(t, fun); target == nil || target.Name != first.Name {
				t.Fatalf("function mapping changed without ring mutation: %v", target)
			}
		}
	})
	t.Run("resource_fallback_within_ring", func(t *testing.T) {
		fixture := newGeneralLBFixture(t, specs...)
		second := fixture.add(t, lbTargetSpec{"arm-second", "arm/tiny", "arm64"})
		first := fixture.targets[0]
		fun := generalLBFunction(t, []string{"arm64"}, "arm/tiny")
		setLBResources(first, 0, 1000)
		setLBResources(second, 0, 1000)
		if fixture.invoke(t, fun) != nil {
			t.Fatal("selected a node without enough RAM")
		}
		setLBResources(second, 1000000, 1000)
		if target := fixture.invoke(t, fun); target == nil || target.Name != second.Name {
			t.Fatalf("did not use the only admitted node: %v", target)
		}
		setLBResources(second, 1000000, 0)
		if fixture.invoke(t, fun) != nil {
			t.Fatal("selected a node without enough CPU")
		}
		setLBResources(first, 1000000, 1000)
		if target := fixture.invoke(t, fun); target == nil || target.Name != first.Name {
			t.Fatalf("did not switch to the recovered node: %v", target)
		}
	})
	t.Run("fallback_between_compatible_pools", func(t *testing.T) {
		fixture := newGeneralLBFixture(t, specs...)
		fun := generalLBFunction(t, []string{"arm64", "amd64"}, "")
		setLBResources(fixture.targets[0], 0, 1000)
		setLBResources(fixture.targets[1], 0, 1000)
		// RoundRobin first prioritizes the unavailable ARM pool, forcing inter-pool
		// fallback. Other modes must still return the sole admitted compatible node.
		for i := 0; i < 6; i++ {
			target := fixture.invoke(t, fun)
			if target == nil || target.Name != fixture.targets[2].Name {
				t.Fatalf("compatible pool fallback failed: %v", target)
			}
		}
	})
	t.Run("reject_incompatible_fallback", func(t *testing.T) {
		fixture := newGeneralLBFixture(t, specs...)
		fun := generalLBFunction(t, []string{"arm64"}, "")
		setLBResources(fixture.targets[0], 0, 1000)
		if fixture.invoke(t, fun) != nil {
			t.Fatal("fell back to an incompatible architecture")
		}
		setLBResources(fixture.targets[0], 1000000, 1000)
		if target := fixture.invoke(t, fun); target == nil || target.Name != fixture.targets[0].Name {
			t.Fatalf("compatible recovery failed: %v", target)
		}
	})
	t.Run("empty_rings", func(t *testing.T) {
		fixture := newGeneralLBFixture(t, specs...)
		for _, target := range fixture.targets {
			fixture.balancer.RemoveTarget(target.Name)
		}
		fun := generalLBFunction(t, []string{"arm64", "amd64"}, "")
		if fixture.invoke(t, fun) != nil || fixture.ordinary() != nil {
			t.Fatal("removed nodes remained eligible")
		}
	})
	t.Run("exact_tag_mask", func(t *testing.T) {
		fixture := newGeneralLBFixture(t, specs...)
		fun := generalLBFunction(t, []string{"amd64"}, "x86/large")
		setLBResources(fixture.targets[2], 0, 1000)
		if fixture.invoke(t, fun) != nil {
			t.Fatal("fell back outside the requested tag")
		}
		setLBResources(fixture.targets[2], 1000000, 1000)
		if target := fixture.invoke(t, fun); target == nil || target.Name != fixture.targets[2].Name {
			t.Fatalf("did not use the required pool: %v", target)
		}
	})
	t.Run("tag_pattern", func(t *testing.T) {
		fixture := newGeneralLBFixture(t, specs...)
		fun := generalLBFunction(t, []string{"arm64", "amd64"}, "x86/*")
		for i := 0; i < 8; i++ {
			target := fixture.invoke(t, fun)
			if target == nil || !strings.HasPrefix(target.Meta["machine_tag"].(string), "x86/") {
				t.Fatalf("ignored pool pattern: %v", target)
			}
		}
	})
	t.Run("architecture_metadata_without_tag", func(t *testing.T) {
		// Older registrations may omit machine_tag. The general balancer already
		// supports falling back to arch metadata; removing the old class must retain it.
		fixture := newGeneralLBFixture(t, lbTargetSpec{"arch-only", "", "arm64"})
		fun := generalLBFunction(t, []string{"arm64"}, "")
		if target := fixture.invoke(t, fun); target == nil || target.Name != fixture.targets[0].Name {
			t.Fatalf("architecture-only registration no longer routes: %v", target)
		}
	})
}
