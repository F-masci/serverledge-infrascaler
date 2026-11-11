package test

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/serverledge-faas/serverledge/internal/function"
	"github.com/serverledge-faas/serverledge/internal/node"
	"github.com/serverledge-faas/serverledge/internal/scheduling"
	"github.com/serverledge-faas/serverledge/utils"
)

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
