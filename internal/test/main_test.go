package test

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/serverledge-faas/serverledge/internal/api"
	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/internal/mab"
	"github.com/serverledge-faas/serverledge/internal/metrics"
	"github.com/serverledge-faas/serverledge/internal/node"
	"github.com/serverledge-faas/serverledge/internal/registration"
	"github.com/serverledge-faas/serverledge/internal/scheduling"
	"github.com/serverledge-faas/serverledge/internal/workflow"
	u "github.com/serverledge-faas/serverledge/utils"
	"github.com/spf13/viper"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

const HOST = "127.0.0.1"
const PORT = 1323

// The default profile preserves the existing shared integration environment.
// Other profiles are selected explicitly for a fresh go test invocation; tests
// never relaunch their own binary or replace a running scheduler.
var platformProfile = flag.String("platform-profile", "default", "test environment: default, edge, lb, discovery, executor")

// lbTestMode selects the routing strategy once, before the LB profile starts.
var lbTestMode = flag.String("lb-test-mode", "RoundRobin", "LB test selection: RoundRobin, Random, MAB")

var profileCases = map[string][]string{
	"edge":      {"TestEdgeOnlyLocalRejectionsComplete"},
	"lb":        {"TestTargetRefreshPollsOnceAndReplacesChangedMetadata", "TestGeneralLoadBalancer"},
	"discovery": {"TestNearestNeighborsContainOnlyRegisteredNodes"},
	"executor":  {"TestExecutorRetryPreservesRequestBody"},
}

func getShell() string {
	if IsWindows() {
		return "powershell.exe"
	} else {
		return "/bin/sh"
	}
}

func getShellExt() string {
	if IsWindows() {
		return ".bat"
	} else {
		return ".sh"
	}
}

// readySchedulingPolicy signals when Run has initialized channels, pool and
// container factory. Promoted methods retain the selected production policy.
type readySchedulingPolicy struct {
	scheduling.Policy
	ready chan struct{}
}

func (p *readySchedulingPolicy) Init() {
	p.Policy.Init()
	close(p.ready)
}

// selectProfileCases limits special profiles to compatible test roots. A -run
// selection is intersected with those roots, preserving an optional subtest filter.
// Default-profile tests keep ordinary Go discovery and run selection unchanged.
func selectProfileCases() error {
	if *platformProfile == "lb" && *lbTestMode != "RoundRobin" && *lbTestMode != "Random" && *lbTestMode != "MAB" {
		return fmt.Errorf("unknown LB test mode %q", *lbTestMode)
	}
	if *platformProfile == "default" {
		return nil
	}
	names, ok := profileCases[*platformProfile]
	if !ok {
		return fmt.Errorf("unknown platform profile %q", *platformProfile)
	}
	selection := strings.SplitN(flag.Lookup("test.run").Value.String(), "/", 2)
	expression, err := regexp.Compile(selection[0])
	if err != nil {
		return err
	}
	var matching []string
	for _, name := range names {
		if expression.MatchString(name) {
			matching = append(matching, regexp.QuoteMeta(name))
		}
	}
	if len(matching) == 0 {
		return fmt.Errorf("-run does not select any test for profile %q", *platformProfile)
	}
	filter := "^(" + strings.Join(matching, "|") + ")$"
	if len(selection) == 2 {
		filter += "/" + selection[1]
	}
	return flag.Set("test.run", filter)
}

// TestMain exits only after runPlatformTests returns, so its deferred cleanup
// closes etcd and its client and removes temporary data even after test failure.
func TestMain(m *testing.M) {
	os.Exit(runPlatformTests(m))
}

func runPlatformTests(m *testing.M) int {
	flag.Parse()
	if err := selectProfileCases(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}

	// The executor case supplies a local Docker API fixture and a controlled HTTP
	// transport. No shared server or background monitor may use those globals.
	if *platformProfile == "executor" {
		return m.Run()
	}

	tempDir, err := os.MkdirTemp("", "etcd-data-*")
	if err != nil {
		log.Printf("Cannot prepare etcd: %v", err)
		return 1
	}
	defer os.RemoveAll(tempDir)
	cfg := embed.NewConfig()
	cfg.Dir = tempDir
	cfg.LogOutputs = []string{"/dev/null"}
	etcd, err := embed.StartEtcd(cfg)
	if err != nil {
		log.Printf("Cannot start etcd: %v", err)
		return 1
	}
	defer etcd.Close()
	select {
	case <-etcd.Server.ReadyNotify():
	case <-time.After(60 * time.Second):
		log.Print("Embedded etcd did not become ready")
		return 1
	}

	client, err := clientv3.New(clientv3.Config{Endpoints: []string{etcd.Clients[0].Addr().String()}, DialTimeout: 5 * time.Second})
	if err != nil {
		log.Printf("Cannot connect to etcd: %v", err)
		return 1
	}
	defer client.Close()

	if *platformProfile == "lb" || *platformProfile == "discovery" {
		// These profiles need discovery, but not the shared HTTP API or scheduler.
		// Configuration is fixed before monitors start and is not changed by cases.
		viper.Set(config.API_IP, HOST)
		viper.Set(config.API_PORT, 0)
		viper.Set(config.LISTEN_UDP_PORT, 0)
		viper.Set(config.LB_REFRESH_INTERVAL, 1)
		viper.Set(config.Arch_AWARENESS, true)
		viper.Set(config.REG_NEARBY_INTERVAL, 1)
		viper.Set(config.REG_MONITORING_INTERVAL, 1)
		node.LocalResources.Init()
		node.LocalNode = node.NewIdentifier("test-observer", "test-"+*platformProfile, "test")
		if err := registration.RegisterNode(); err != nil {
			log.Printf("Cannot register observer: %v", err)
			return 1
		}
		defer registration.Deregister()
		if *platformProfile == "lb" {
			viper.Set(config.LB_MODE, *lbTestMode)
			mab.InitBanditManager()
		}
		if *platformProfile == "discovery" {
			if err := registration.StartMonitoring(); err != nil {
				log.Printf("Cannot start discovery: %v", err)
				return 1
			}
		}
		return m.Run()
	}

	outboundIP, err := u.GetOutboundIp()
	if err != nil || outboundIP == nil {
		log.Print("Cannot determine outbound IP for integration tests")
		return 1
	}
	server, err := setupServerledge(outboundIP.String())
	if err != nil {
		log.Printf("Cannot initialize Serverledge: %v", err)
		return 1
	}
	defer func() {
		if err := teardownServerledge(server); err != nil {
			log.Printf("Serverledge cleanup failed: %v", err)
		}
	}()
	if err := waitForServerReady(); err != nil {
		log.Print(err)
		return 1
	}
	return m.Run()
}

// testStartServerledge starts one shared node with the policy chosen for this run.
// Its lifetime covers the entire suite; individual cases clean up their own data.
func testStartServerledge(isInCloud bool, outboundIP string) (*echo.Echo, error) {
	api.CacheSetup()
	var policy scheduling.Policy = &scheduling.DefaultLocalPolicy{}
	if *platformProfile == "edge" {
		policy = &scheduling.EdgePolicy{}
	}
	myArea := config.GetString(config.REGISTRY_AREA, "ROME")
	node.LocalNode = node.NewRandomIdentifier(myArea)
	if err := registration.RegisterNode(); err != nil {
		return nil, err
	}
	metrics.Init()
	workflow.CreateOffloadingPolicy()
	server := echo.New()
	api.RegisterTerminationHandler(server)
	ready := &readySchedulingPolicy{Policy: policy, ready: make(chan struct{})}
	go scheduling.Run(ready)
	select {
	case <-ready.ready:
	case <-time.After(5 * time.Second):
		registration.Deregister()
		return nil, fmt.Errorf("scheduler did not initialize")
	}
	if !isInCloud {
		if err := registration.StartMonitoring(); err != nil {
			return nil, err
		}
	}
	go api.StartAPIServer(server)
	return server, nil
}

func setupServerledge(outboundIP string) (*echo.Echo, error) {
	return testStartServerledge(false, outboundIP)
}

// waitForServerReady closes every probe response instead of retaining connections.
func waitForServerReady() error {
	client := &http.Client{Timeout: time.Second}
	for attempt := 0; attempt < 50; attempt++ {
		response, err := client.Get(fmt.Sprintf("http://%s:%d/status", HOST, PORT))
		if err == nil {
			response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("Serverledge did not become ready")
}

func teardownServerledge(server *echo.Echo) error {
	node.ShutdownAllContainers()
	node.StopJanitor()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	errServer := server.Shutdown(ctx)
	errRegistry := registration.Deregister()
	return u.ReturnNonNilErr(errServer, errRegistry)
}

// requirePlatformProfile makes dependencies on the selected suite explicit.
// A case in another profile is skipped rather than starting a second platform.
func requirePlatformProfile(t *testing.T, expected string) {
	t.Helper()
	if *platformProfile != expected {
		t.Skipf("requires a fresh run with -args -platform-profile=%s", expected)
	}
}
