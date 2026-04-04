package pipeline

import (
	"testing"

	runtimeflowidentity "swarm/internal/runtime/core/flowidentity"
)

func TestWorkflowInstanceStoreIdentityUsesCanonicalFlowIdentityOwner(t *testing.T) {
	cases := []struct {
		name string
		ref  string
	}{
		{name: "uuid ref", ref: "11111111-1111-1111-1111-111111111111"},
		{name: "bare non-uuid ref", ref: "child"},
		{name: "path ref", ref: "child/inst-1"},
		{name: "trimmed path ref", ref: "/child/inst-1/"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got, want := workflowInstanceRowID(tc.ref), runtimeflowidentity.EntityID(tc.ref); got != want {
				t.Fatalf("workflowInstanceRowID(%q) = %q, want %q", tc.ref, got, want)
			}
			if got, want := FlowInstanceEntityID(tc.ref), runtimeflowidentity.EntityID(tc.ref); got != want {
				t.Fatalf("FlowInstanceEntityID(%q) = %q, want %q", tc.ref, got, want)
			}

			got := workflowInstanceLookupKeys(tc.ref)
			want := runtimeflowidentity.LookupKeys(tc.ref)
			if len(got) != len(want) {
				t.Fatalf("workflowInstanceLookupKeys(%q) len = %d, want %d (%v)", tc.ref, len(got), len(want), got)
			}
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("workflowInstanceLookupKeys(%q)[%d] = %q, want %q", tc.ref, i, got[i], want[i])
				}
			}
		})
	}
}
