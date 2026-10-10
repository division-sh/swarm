package cataloge2e

import (
	"context"
	"errors"
	"testing"

	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

type catalogSemanticFlowReadProbe struct {
	catalogWorkflowPersistence
	rows  []runtimepipeline.WorkflowInstance
	err   error
	calls int
	run   string
}

func (p *catalogSemanticFlowReadProbe) ListWorkflowInstances(_ context.Context, run string) ([]runtimepipeline.WorkflowInstance, error) {
	p.calls++
	p.run = run
	return p.rows, p.err
}

func TestCatalogCausalFlowMatchingUsesOnlySemanticReadOwner(t *testing.T) {
	rows := []runtimepipeline.WorkflowInstance{
		{InstanceID: "unrelated", WorkflowName: "worker", StorageRef: "worker/unrelated", EntityID: "other"},
		{InstanceID: "selected", WorkflowName: "worker", StorageRef: "worker/selected", EntityID: "child", ParentEntityID: "parent"},
		{InstanceID: "sibling", WorkflowName: "sibling", StorageRef: "sibling/selected", EntityID: "child", ParentEntityID: "parent"},
	}
	for _, test := range []struct {
		name, flow, want string
		causal           map[string]struct{}
		required         bool
	}{
		{"exact_parent", " worker ", "selected", map[string]struct{}{"parent": {}}, true},
		{"exact_child", "worker", "selected", map[string]struct{}{"child": {}}, true},
		{"exact_instance", "worker", "selected", map[string]struct{}{"selected": {}}, true},
		{"exact_path", "/worker/selected/", "selected", nil, true},
		{"wrong_causal_entity", "worker", "", map[string]struct{}{"missing": {}}, true},
		{"wrong_flow", "unknown", "", map[string]struct{}{"parent": {}}, true},
		{"empty_flow", "", "", nil, true},
		{"optional_causal_fallback", "worker", "unrelated", map[string]struct{}{"missing": {}}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			probe := &catalogSemanticFlowReadProbe{rows: rows}
			got, found, err := catalogFlowInstanceForCausalFlow(probe, nil, test.causal, test.flow, test.required)
			if err != nil || found != (test.want != "") || got.InstanceID != test.want || probe.calls != 1 || probe.run != catalogRuntimeRunID {
				t.Fatalf("semantic flow selection changed: row=%+v found=%t err=%v calls=%d run=%s", got, found, err, probe.calls, probe.run)
			}
		})
	}
	refusal := errors.New("semantic reader refused")
	probe := &catalogSemanticFlowReadProbe{err: refusal}
	if _, found, err := catalogFlowInstanceForCausalFlow(probe, nil, nil, "worker", false); found || !errors.Is(err, refusal) {
		t.Fatalf("semantic read failure supplied match evidence: found=%t err=%v", found, err)
	}
	if _, found, err := catalogFlowInstanceForCausalFlow(nil, nil, nil, "worker", false); found || err != nil {
		t.Fatalf("absent semantic reader changed: found=%t err=%v", found, err)
	}
}
