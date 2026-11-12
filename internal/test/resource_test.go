package test

import (
	"github.com/labstack/echo/v4/middleware"
	"github.com/serverledge-faas/serverledge/internal/function"
	"github.com/serverledge-faas/serverledge/internal/lb"
	"testing"
)

func TestIndependentResourceChecks(t *testing.T) {
	target := &middleware.ProxyTarget{Name: "resource-check-test"}
	fun := &function.Function{Name: "test", MemoryMB: 128, CPUDemand: 0.5}

	for _, tc := range []struct {
		name            string
		memory          int64
		cpu             float64
		memoryOK, cpuOK bool
	}{
		{"CPU exhausted", 256, 0, true, false},
		{"memory exhausted", 0, 2, false, true},
		{"exact capacity", 128, 0.5, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lb.NodeMetrics.Update(target.Name, tc.memory, 1024, 1, tc.cpu)
			if got := (&lb.DefaultMemoryChecker{}).HasEnoughMemory(target, fun); got != tc.memoryOK {
				t.Fatalf("memory admission = %v", got)
			}
			if got := (&lb.DefaultCPUChecker{}).HasEnoughCPU(target, fun); got != tc.cpuOK {
				t.Fatalf("CPU admission = %v", got)
			}
		})
	}
}
