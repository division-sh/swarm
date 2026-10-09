package runforkexecution

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func TestSelectedFiniteFeedResumeRequiresExactPermanentRequest(t *testing.T) {
	point := runfork.RunForkPoint{Kind: runfork.RunForkPointDeploymentRevision, Revision: 1}
	op := runfork.ForkOperationRequest{
		OperationID: uuid.NewString(), Actor: "test-actor", TransportHash: "sha256:request",
		SourceRunID: uuid.NewString(), TargetBundleHash: "bundle-test", ResolvedPoint: &point,
		ContractSelection: runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts},
	}
	op, hash, err := op.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	childID := uuid.NewString()
	result := runfork.SelectedForkRecoveryResult{
		RunID: childID, ExecutionID: uuid.NewString(),
		Disposition: runfork.SelectedForkRecoveryResume,
		Operation: &runfork.ForkOperationRecord{Request: op, SemanticHash: hash, ForkRunID: childID,
			BindingID: uuid.NewString(), Status: runfork.ForkOperationMaterialized},
		Continuation: &runfork.SelectedForkContinuation{ForkRunStatus: runfork.RunForkMaterializedStatus},
	}
	request := SelectedContractExecutionRequest{
		ForkOperation: &op, Recovery: &result, SourceRunID: op.SourceRunID,
		ExpectedBundleHash: op.TargetBundleHash, ContractSelection: op.ContractSelection,
	}
	if err := validateSelectedForkRecoveryRequest(request); err != nil {
		t.Fatalf("exact deployment resume rejected: %v", err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*SelectedContractExecutionRequest)
	}{
		{"foreign source", func(r *SelectedContractExecutionRequest) { r.SourceRunID = uuid.NewString() }},
		{"different bundle", func(r *SelectedContractExecutionRequest) { r.ExpectedBundleHash = "other" }},
		{"different operation", func(r *SelectedContractExecutionRequest) {
			copy := *r.ForkOperation
			copy.OperationID = uuid.NewString()
			r.ForkOperation = &copy
		}},
		{"different point", func(r *SelectedContractExecutionRequest) {
			copy := *r.ForkOperation
			changed := *copy.ResolvedPoint
			changed.Revision++
			copy.ResolvedPoint = &changed
			r.ForkOperation = &copy
		}},
		{"event point", func(r *SelectedContractExecutionRequest) {
			copy := *r.Recovery
			operation := *copy.Operation
			changed := *operation.Request.ResolvedPoint
			changed.Kind = runfork.RunForkPointEvent
			changed.EventID = uuid.NewString()
			operation.Request.ResolvedPoint = &changed
			copy.Operation = &operation
			r.Recovery = &copy
		}},
		{"wrong disposition", func(r *SelectedContractExecutionRequest) {
			copy := *r.Recovery
			copy.Disposition = runfork.SelectedForkRecoveryControlOnly
			r.Recovery = &copy
		}},
		{"missing predecessor", func(r *SelectedContractExecutionRequest) {
			copy := *r.Recovery
			copy.Disposition = runfork.SelectedForkRecoveryActivate
			copy.ExecutionID = ""
			r.Recovery = &copy
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := request
			tc.mutate(&changed)
			if err := validateSelectedForkRecoveryRequest(changed); err == nil {
				t.Fatal("contradictory selected resume reached preparation")
			}
		})
	}
	first := request
	firstRecovery := result
	firstRecovery.ExecutionID = ""
	first.Recovery = &firstRecovery
	if err := validateSelectedForkRecoveryRequest(first); err != nil {
		t.Fatalf("keyed first attachment rejected: %v", err)
	}
}
