package test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/serverledge-faas/serverledge/internal/api"
)

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
