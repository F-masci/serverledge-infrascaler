package test

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/serverledge-faas/serverledge/internal/container"
	"github.com/serverledge-faas/serverledge/internal/function"
	"github.com/serverledge-faas/serverledge/internal/node"
	"github.com/serverledge-faas/serverledge/utils"
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

// poolUsage is a consistent snapshot of public resource counters. Tests compare
// before/after values instead of clearing pools belonging to the shared server.
type poolUsage struct {
	freeMemory      int64
	availableMemory int64
	usedCPU         float64
	availableCPU    float64
}

func readPoolUsage() poolUsage {
	node.LocalResources.RLock()
	defer node.LocalResources.RUnlock()
	return poolUsage{node.LocalResources.FreeMemory(), node.LocalResources.AvailableMemory(), node.LocalResources.UsedCPUs(), node.LocalResources.AvailableCPUs()}
}

func samePoolUsage(left, right poolUsage) bool {
	return left.freeMemory == right.freeMemory && left.availableMemory == right.availableMemory && math.Abs(left.usedCPU-right.usedCPU) < 1e-9 && math.Abs(left.availableCPU-right.availableCPU) < 1e-9
}

// waitForIdlePool waits for completion notifications from preceding invocations.
// The scheduler releases resources asynchronously after returning client results.
func waitForIdlePool(t *testing.T) {
	t.Helper()
	waitForPlatformCondition(t, "the shared pool to finish active invocations", 5*time.Second, func() bool { return math.Abs(readPoolUsage().usedCPU) < 1e-9 })
}
