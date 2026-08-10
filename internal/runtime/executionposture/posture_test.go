package executionposture

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/executionmode"
)

func TestParseRequiresExactExecutionPosture(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want Posture
		ok   bool
	}{
		{name: "live", raw: "live", want: Live, ok: true},
		{name: "mock only", raw: "mock_only", want: MockOnly, ok: true},
		{name: "omitted", raw: "", ok: false},
		{name: "unknown", raw: "mock", ok: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := Parse(tc.raw)
			if got != tc.want || ok != tc.ok {
				t.Fatalf("Parse(%q) = %q, %v; want %q, %v", tc.raw, got, ok, tc.want, tc.ok)
			}
		})
	}
}

func TestPostureRootModeAndAdmission(t *testing.T) {
	if got := Live.RootMode(); got != executionmode.Live {
		t.Fatalf("live root mode = %q, want live", got)
	}
	if got := MockOnly.RootMode(); got != executionmode.Mock {
		t.Fatalf("mock_only root mode = %q, want mock", got)
	}
	if err := Live.Admit(executionmode.Live, "event persistence"); err != nil {
		t.Fatalf("live posture rejected live mode: %v", err)
	}
	if err := MockOnly.Admit(executionmode.Mock, "event persistence"); err != nil {
		t.Fatalf("mock_only posture rejected mock mode: %v", err)
	}
	if err := MockOnly.Admit(executionmode.Live, "event persistence"); err == nil {
		t.Fatal("mock_only posture admitted live mode")
	}
}
