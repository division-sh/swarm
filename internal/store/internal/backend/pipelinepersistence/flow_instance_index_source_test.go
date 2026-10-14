package pipelinepersistence

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

type snapshotHistoricalReader struct{ HistoricalFlowInstanceReader }

func TestInstanceSnapshotSourceConflictRequiresValidSourceAndLineage(t *testing.T) {
	artifact := sourceartifactfixture.Artifact()
	source := semanticview.Wrap(&contracts.WorkflowContractBundle{SourceArtifact: artifact, Semantics: contracts.WorkflowSemanticView{Version: "original"}})
	run := runlifecycle.Snapshot{RunID: "11111111-1111-1111-1111-111111111111", State: runlifecycle.StateRunning,
		Origin: runlifecycle.DeploymentRunOrigin(), BundleHash: artifact.BundleHash(), StartedAt: time.Now().UTC()}
	owner := &snapshotHistoricalReader{}
	if err := validateFlowInstanceIndexSnapshot(source, artifact.BundleHash(), run, owner); err != nil {
		t.Fatalf("exact admitted source refused: %v", err)
	}
	for _, fault := range []string{"changed_source", "missing_source", "missing_owner", "missing_lineage", "malformed_run", "malformed_run_id", "malformed_active_hash", "spaced_active_hash", "malformed_request_hash", "crossed_request_hash", "missing_version", "missing_artifact"} {
		t.Run(fault, func(t *testing.T) {
			selected, requested, active, history := source, artifact.BundleHash(), run, HistoricalFlowInstanceReader(owner)
			active.BundleHash = "bundle-v2:sha256:" + strings.Repeat("b", 64)
			switch fault {
			case "missing_source":
				selected = nil
			case "missing_owner":
				history = nil
			case "missing_lineage":
				active.Origin = runlifecycle.RunOrigin{}
			case "malformed_run":
				active.State = "unknown"
			case "malformed_run_id":
				active.RunID = "not-a-run"
			case "malformed_active_hash":
				active.BundleHash = "not-an-artifact"
			case "spaced_active_hash":
				active.BundleHash = " " + active.BundleHash
			case "malformed_request_hash":
				requested = "not-an-artifact"
			case "crossed_request_hash":
				requested = "bundle-v2:sha256:" + strings.Repeat("c", 64)
			case "missing_version":
				selected = semanticview.Wrap(&contracts.WorkflowContractBundle{SourceArtifact: artifact})
			case "missing_artifact":
				selected = semanticview.Wrap(&contracts.WorkflowContractBundle{Semantics: contracts.WorkflowSemanticView{Version: "original"}})
			}
			err := validateFlowInstanceIndexSnapshot(selected, requested, active, history)
			var conflict *pipeline.FlowInstanceSourceConflict
			classified := errors.As(err, &conflict)
			if err == nil || classified != (fault == "changed_source") {
				t.Fatalf("%s source classification=%t error=%v", fault, classified, err)
			}
			if classified && (conflict.RunID != run.RunID || conflict.RequestedBundleHash != requested || conflict.ActiveBundleHash != active.BundleHash) {
				t.Fatalf("stale-source refusal lost admitted coordinates: %+v", conflict)
			}
		})
	}
}
