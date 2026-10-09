package runfork

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/google/uuid"
)

func TestSelectedRecoveryActivatedResultIsOriginalAcknowledgment(t *testing.T) {
	point := RunForkPoint{Kind: RunForkPointRunStart, Revision: 3}
	request := ForkOperationRequest{OperationID: uuid.NewString(), Actor: "operator", TransportHash: "sha256:transport",
		SourceRunID: uuid.NewString(), AtStart: true, ResolvedPoint: &point,
		TargetBundleHash:  "bundle-v2:sha256:" + strings.Repeat("a", 64),
		ContractSelection: RunForkContractSelection{Mode: RunForkContractSelectionModeSelectedContracts}}
	request, hash, err := request.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	child := uuid.NewString()
	record := ForkOperationRecord{Request: request, SemanticHash: hash, ForkRunID: child, BindingID: uuid.NewString(),
		Status: ForkOperationActivated, Result: &ForkOperationResult{
			SourceRunID: request.SourceRunID, SourceRunStatus: RunForkSourceFrozenStatus, SourceFrozen: true,
			ForkRunID: child, ForkRunStatus: RunForkActivatedStatus, ForkPoint: point, BundleHash: request.TargetBundleHash,
			ExecutedEventCount: 2, DataPins: []durabledata.Pin{{RunID: child, RunState: RunForkActivatedStatus,
				Declaration:  durabledata.DeclarationRef{FlowPath: ".", EventName: "root.ready"},
				SchemaDigest: durabledata.SchemaDigest("resource-schema-v1:sha256:" + strings.Repeat("b", 64)),
				VersionID:    durabledata.VersionID("resource-version-v1:sha256:" + strings.Repeat("c", 64)), Selection: "fork_override"}},
		}}
	for _, disposition := range []SelectedForkRecoveryDisposition{SelectedForkRecoveryResume, SelectedForkRecoveryTerminal, SelectedForkRecoveryControlOnly, SelectedForkRecoveryFailed} {
		recovered := SelectedForkRecoveryResult{RunID: child, Disposition: disposition, Operation: &record}
		result, present, err := recovered.ActivatedResult()
		if err != nil || !present || result.ForkRunStatus != RunForkActivatedStatus || result.ForkPoint != point || result.ExecutedEventCount != 2 {
			t.Fatalf("original acknowledgment lost for %s: result=%+v present=%t err=%v", disposition, result, present, err)
		}
		result.DataPins[0].RunState = "failed"
		if record.Result.DataPins[0].RunState != RunForkActivatedStatus {
			t.Fatal("readback aliases durable result pins")
		}
	}
	for _, tc := range []struct {
		name   string
		mutate func(*ForkOperationRecord)
	}{
		{"missing_result", func(r *ForkOperationRecord) { r.Result = nil }},
		{"wrong_hash", func(r *ForkOperationRecord) { r.SemanticHash = "sha256:other" }},
		{"foreign_child", func(r *ForkOperationRecord) { r.ForkRunID = uuid.NewString() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := record
			tc.mutate(&changed)
			if _, _, err := (SelectedForkRecoveryResult{RunID: child, Operation: &changed}).ActivatedResult(); err == nil {
				t.Fatal("contradictory acknowledgment accepted")
			}
		})
	}
	record.Status, record.Result = ForkOperationMaterialized, nil
	if _, present, err := (SelectedForkRecoveryResult{RunID: child, Operation: &record}).ActivatedResult(); err != nil || present {
		t.Fatalf("materialized operation manufactured acknowledgment: present=%t err=%v", present, err)
	}
}
