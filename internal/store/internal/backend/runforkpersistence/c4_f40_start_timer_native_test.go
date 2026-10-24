package runforkpersistence

import (
	"encoding/json"

	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/forkpoint"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func TestC4F40TypedStartTimerCodecAndPreparationHash(t *testing.T) {
	snapshot := inheritedTimerSnapshot(t)
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeRunForkTimerSnapshot(raw)
	if err != nil {
		t.Fatal(err)
	}
	source, err := workflowTimerActivationFromSnapshot(decoded)
	if err != nil {
		t.Fatal(err)
	}
	plan := workflowTimerMaterializerPlan(source, runfork.RunForkPoint{Kind: runfork.RunForkPointRunStart, Revision: 7}).WithHistoricalEvents(7, nil)
	fingerprint := func(plan runfork.RunForkPlan) string {
		t.Helper()
		hash, err := runfork.SelectedPreparationPlanFingerprint(plan, runfork.RunForkContractFrontierAdmission{}, runfork.RunForkSelectedContractRecipientPlanning{}, "c4-timer")
		if err != nil {
			t.Fatal(err)
		}
		return hash
	}
	want := fingerprint(plan)
	for _, tc := range []struct {
		name string
		edit func(*pipeline.WorkflowTimerActivation)
	}{
		{"source_timer", func(a *pipeline.WorkflowTimerActivation) { a.SourceTimerID = "77777777-7777-4777-8777-777777777777" }},
		{"source_run", func(a *pipeline.WorkflowTimerActivation) { a.ForkedFromRunID = "88888888-8888-4888-8888-888888888888" }},
		{"source_arm", func(a *pipeline.WorkflowTimerActivation) { a.SourceArmedAt = a.SourceArmedAt.Add(-time.Microsecond) }},
		{"point_revision", func(a *pipeline.WorkflowTimerActivation) { a.ForkedFromPointRevision++ }},
		{"point_kind", func(a *pipeline.WorkflowTimerActivation) { a.ForkedFromPointKind = forkpoint.DeploymentRevision }},
		{"owner", func(a *pipeline.WorkflowTimerActivation) { a.ReconstructionOwner = "other-cut" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := source.Canonical()
			tc.edit(&changed)
			if err := changed.Validate(); err != nil {
				t.Fatalf("hash counterexample must be a valid distinct lineage: %v", err)
			}
			candidate := plan
			candidate.WorkflowTimers = []pipeline.WorkflowTimerActivationPersistenceRecord{changed.PersistenceRecord()}
			if fingerprint(candidate) == want {
				t.Fatal("preparation hash omitted typed timer lineage")
			}
		})
	}
	if fingerprint(plan) != want {
		t.Fatal("counterexamples mutated fixed source evidence")
	}
}
