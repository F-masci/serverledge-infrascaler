package test

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/serverledge-faas/serverledge/internal/container"
	"github.com/serverledge-faas/serverledge/internal/executor"
)

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
