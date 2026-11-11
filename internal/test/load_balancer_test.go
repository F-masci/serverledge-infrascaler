package test

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/serverledge-faas/serverledge/internal/cache"
	"github.com/serverledge-faas/serverledge/internal/function"
	"github.com/serverledge-faas/serverledge/internal/lb"
	"github.com/serverledge-faas/serverledge/internal/mab"
	"github.com/serverledge-faas/serverledge/internal/registration"
	"github.com/serverledge-faas/serverledge/utils"
)

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

// TestGeneralLoadBalancerDynamicRings verifies dynamic rings using the selected LB strategy.
func TestGeneralLoadBalancerDynamicRings(t *testing.T) {
	requirePlatformProfile(t, "lb")
	specs := []lbTargetSpec{{"arm", "arm/tiny", "arm64"}, {"x86-small", "x86/tiny", "amd64"}, {"x86-large", "x86/large", "amd64"}}

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

}

// TestGeneralLoadBalancerAddRemove verifies add remove using the selected LB strategy.
func TestGeneralLoadBalancerAddRemove(t *testing.T) {
	requirePlatformProfile(t, "lb")
	specs := []lbTargetSpec{{"arm", "arm/tiny", "arm64"}, {"x86-small", "x86/tiny", "amd64"}, {"x86-large", "x86/large", "amd64"}}

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

}

// TestGeneralLoadBalancerArchitectureCompatibility verifies architecture compatibility using the selected LB strategy.
func TestGeneralLoadBalancerArchitectureCompatibility(t *testing.T) {
	requirePlatformProfile(t, "lb")
	specs := []lbTargetSpec{{"arm", "arm/tiny", "arm64"}, {"x86-small", "x86/tiny", "amd64"}, {"x86-large", "x86/large", "amd64"}}

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

}

// TestGeneralLoadBalancerStableHashWithinPool verifies stable hash within pool using the selected LB strategy.
func TestGeneralLoadBalancerStableHashWithinPool(t *testing.T) {
	requirePlatformProfile(t, "lb")
	specs := []lbTargetSpec{{"arm", "arm/tiny", "arm64"}, {"x86-small", "x86/tiny", "amd64"}, {"x86-large", "x86/large", "amd64"}}

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

}

// TestGeneralLoadBalancerResourceFallbackWithinRing verifies resource fallback within ring using the selected LB strategy.
func TestGeneralLoadBalancerResourceFallbackWithinRing(t *testing.T) {
	requirePlatformProfile(t, "lb")
	specs := []lbTargetSpec{{"arm", "arm/tiny", "arm64"}, {"x86-small", "x86/tiny", "amd64"}, {"x86-large", "x86/large", "amd64"}}

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

}

// TestGeneralLoadBalancerFallbackBetweenCompatiblePools verifies fallback between compatible pools using the selected LB strategy.
func TestGeneralLoadBalancerFallbackBetweenCompatiblePools(t *testing.T) {
	requirePlatformProfile(t, "lb")
	specs := []lbTargetSpec{{"arm", "arm/tiny", "arm64"}, {"x86-small", "x86/tiny", "amd64"}, {"x86-large", "x86/large", "amd64"}}

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

}

// TestGeneralLoadBalancerRejectIncompatibleFallback verifies reject incompatible fallback using the selected LB strategy.
func TestGeneralLoadBalancerRejectIncompatibleFallback(t *testing.T) {
	requirePlatformProfile(t, "lb")
	specs := []lbTargetSpec{{"arm", "arm/tiny", "arm64"}, {"x86-small", "x86/tiny", "amd64"}, {"x86-large", "x86/large", "amd64"}}

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

}

// TestGeneralLoadBalancerEmptyRings verifies empty rings using the selected LB strategy.
func TestGeneralLoadBalancerEmptyRings(t *testing.T) {
	requirePlatformProfile(t, "lb")
	specs := []lbTargetSpec{{"arm", "arm/tiny", "arm64"}, {"x86-small", "x86/tiny", "amd64"}, {"x86-large", "x86/large", "amd64"}}

	fixture := newGeneralLBFixture(t, specs...)
	for _, target := range fixture.targets {
		fixture.balancer.RemoveTarget(target.Name)
	}
	fun := generalLBFunction(t, []string{"arm64", "amd64"}, "")
	if fixture.invoke(t, fun) != nil || fixture.ordinary() != nil {
		t.Fatal("removed nodes remained eligible")
	}

}

// TestGeneralLoadBalancerExactTagMask verifies exact tag mask using the selected LB strategy.
func TestGeneralLoadBalancerExactTagMask(t *testing.T) {
	requirePlatformProfile(t, "lb")
	specs := []lbTargetSpec{{"arm", "arm/tiny", "arm64"}, {"x86-small", "x86/tiny", "amd64"}, {"x86-large", "x86/large", "amd64"}}

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

}

// TestGeneralLoadBalancerTagPattern verifies tag pattern using the selected LB strategy.
func TestGeneralLoadBalancerTagPattern(t *testing.T) {
	requirePlatformProfile(t, "lb")
	specs := []lbTargetSpec{{"arm", "arm/tiny", "arm64"}, {"x86-small", "x86/tiny", "amd64"}, {"x86-large", "x86/large", "amd64"}}

	fixture := newGeneralLBFixture(t, specs...)
	fun := generalLBFunction(t, []string{"arm64", "amd64"}, "x86/*")
	for i := 0; i < 8; i++ {
		target := fixture.invoke(t, fun)
		if target == nil || !strings.HasPrefix(target.Meta["machine_tag"].(string), "x86/") {
			t.Fatalf("ignored pool pattern: %v", target)
		}
	}

}

// TestGeneralLoadBalancerArchitectureMetadataWithoutTag verifies architecture metadata without tag using the selected LB strategy.
func TestGeneralLoadBalancerArchitectureMetadataWithoutTag(t *testing.T) {
	requirePlatformProfile(t, "lb")

	// Older registrations may omit machine_tag. The general balancer already
	// supports falling back to arch metadata; removing the old class must retain it.
	fixture := newGeneralLBFixture(t, lbTargetSpec{"arch-only", "", "arm64"})
	fun := generalLBFunction(t, []string{"arm64"}, "")
	if target := fixture.invoke(t, fun); target == nil || target.Name != fixture.targets[0].Name {
		t.Fatalf("architecture-only registration no longer routes: %v", target)
	}

}
