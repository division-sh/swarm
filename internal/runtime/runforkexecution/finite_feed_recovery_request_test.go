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
	result := runfork.SelectedForkRecoveryResult{
		RunID: uuid.NewString(), ExecutionID: uuid.NewString(),
		Disposition: runfork.SelectedForkRecoveryResumeFiniteFeed,
		Resume:      &runfork.SelectedForkFiniteFeedResume{Operation: op},
	}
	request := SelectedContractExecutionRequest{
		ForkOperation: &op, Recovery: &result, SourceRunID: op.SourceRunID,
		ExpectedBundleHash: op.TargetBundleHash, ContractSelection: op.ContractSelection,
	}
	if err := validateSelectedFiniteFeedRecoveryRequest(request); err != nil {
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
			resume := *copy.Resume
			changed := *resume.Operation.ResolvedPoint
			changed.Kind = runfork.RunForkPointEvent
			changed.EventID = uuid.NewString()
			resume.Operation.ResolvedPoint = &changed
			copy.Resume = &resume
			r.Recovery = &copy
		}},
		{"wrong disposition", func(r *SelectedContractExecutionRequest) {
			copy := *r.Recovery
			copy.Disposition = runfork.SelectedForkRecoveryActivateFiniteFeed
			r.Recovery = &copy
		}},
		{"missing predecessor", func(r *SelectedContractExecutionRequest) {
			copy := *r.Recovery
			copy.ExecutionID = ""
			r.Recovery = &copy
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := request
			tc.mutate(&changed)
			if err := validateSelectedFiniteFeedRecoveryRequest(changed); err == nil {
				t.Fatal("contradictory selected resume reached preparation")
			}
		})
	}
}
