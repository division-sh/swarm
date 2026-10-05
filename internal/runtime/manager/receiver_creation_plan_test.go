package manager

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestRebuildPendingCreationConsumesCompiledReceiverOccurrence(t *testing.T) {
	root := canonicalrouting.CopySelectedForkReadiness(t, 0, "node")
	path := filepath.Join(root, "worker-flow", "schema.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(raw, []byte("\nauto_emit_on_create:\n  event: worker.inspect.requested\n")...), 0600); err != nil {
		t.Fatal(err)
	}
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	schema, ok := source.FlowSchemaByID("worker-flow")
	if !ok {
		t.Fatal("missing receiver flow")
	}
	identity := flowidentity.Stored(source, "worker-flow", "worker-flow/worker-001", "worker-001", uuid.NewString(), "")
	current := &pipeline.DynamicFlowRuntimeCreationEventPlan{
		EventID: uuid.NewString(), EventType: "worker-flow/worker-001/worker.inspect.requested",
		RunID: uuid.NewString(), ParentEventID: uuid.NewString(), ExecutionMode: executionmode.Live,
		Payload: []byte(`{"worker_id":"worker-001"}`), CreatedAt: time.Unix(100, 0).UTC(),
	}
	plan, err := rebuildPendingDynamicFlowRuntimeCreationEventPlan(current, false, source, schema, identity)
	if err != nil {
		t.Fatal(err)
	}
	if plan == nil || plan.EventType != current.EventType || plan.RunID != current.RunID || plan.ParentEventID != current.ParentEventID || !plan.CreatedAt.Equal(current.CreatedAt) || string(plan.Payload) != string(current.Payload) {
		t.Fatalf("rebuild changed exact occurrence or lineage: current=%+v rebuilt=%+v", current, plan)
	}
	if proof := semanticview.ResolveFlowEventProof(source, "worker-flow", plan.EventType); !proof.HasSchema || proof.Local != "worker.inspect.requested" || proof.IsAuthored(source) {
		t.Fatalf("rebuilt occurrence lacks its compiled receiver proof: %+v", proof)
	}
	for _, invalid := range []map[string]any{{}, {"worker_id": 17}, {"worker_id": "worker-001", "unexpected": true}} {
		if _, err := rebuildPendingDynamicFlowRuntimeCreationEventPlan(current, false, source, schema, identity); err == nil {
			t.Fatalf("rebuild accepted invalid receiver payload: %+v", invalid)
		}
	}
}
