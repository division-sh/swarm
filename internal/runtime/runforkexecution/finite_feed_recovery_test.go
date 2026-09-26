package runforkexecution

import (
	"context"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/durabledata"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"
	"github.com/google/uuid"
)

type selectedFiniteFeedBindingReader struct {
	SelectedContractForkLifecycle
	binding runfork.RunForkSelectedContractBinding
	calls   int
}

func (s *selectedFiniteFeedBindingReader) RequireRunForkSelectedContractBinding(_ context.Context, runID string) (runfork.RunForkSelectedContractBinding, error) {
	s.calls++
	if runID != s.binding.ForkRunID {
		return runfork.RunForkSelectedContractBinding{}, context.Canceled
	}
	return s.binding, nil
}

func TestSelectedFiniteFeedRecoveryAdmitsOnlyExactDurableOperation(t *testing.T) {
	recovered, entry := selectedFiniteFeedRecoveryEvidence()
	recovered.Resume.ForkRunStatus = runfork.RunForkMaterializedStatus
	store := &selectedFiniteFeedBindingReader{binding: entry.Binding}
	owner := SelectedContractExecutionOwner{ports: &selectedContractExecutionPorts{fork: store}}
	request := SelectedContractExecutionRequest{}
	admitted, err := owner.admitSelectedFiniteFeedRecovery(context.Background(), recovered, request)
	if err != nil {
		t.Fatal(err)
	}
	if admitted.action != selectedRecoveryResumeFiniteFeed || admitted.binding.ForkRunID != recovered.RunID ||
		admitted.request.ForkOperation == nil || admitted.request.ForkOperation.OperationID != recovered.Resume.Operation.OperationID ||
		admitted.request.SourceRunID != entry.Binding.SourceRunID || admitted.request.ExpectedBundleHash != entry.BundleHash {
		t.Fatalf("recovery admission lost durable identity: %+v", admitted)
	}
	if store.calls != 1 {
		t.Fatalf("durable binding reads=%d, want 1", store.calls)
	}
	fact, err := runtimecorrelation.NewSourceArtifactFact(entry.BundleHash)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := scenarioexecution.NewEffectiveSourceIdentity(fact, "sha256:"+strings.Repeat("b", 64))
	if err != nil {
		t.Fatal(err)
	}
	for _, hostile := range []struct {
		name    string
		request SelectedContractExecutionRequest
	}{
		{"foreign_source", SelectedContractExecutionRequest{SourceRunID: uuid.NewString()}},
		{"matching_source_is_not_caller_authority", SelectedContractExecutionRequest{SourceRunID: entry.Binding.SourceRunID}},
		{"event_selector", SelectedContractExecutionRequest{At: uuid.NewString()}},
		{"foreign_bundle", SelectedContractExecutionRequest{ExpectedBundleHash: "other"}},
		{"matching_bundle_is_not_caller_authority", SelectedContractExecutionRequest{ExpectedBundleHash: entry.BundleHash}},
		{"fork_operation", SelectedContractExecutionRequest{ForkOperation: &runfork.ForkOperationRequest{}}},
		{"empty_supplied_pins", SelectedContractExecutionRequest{DataPinOverrides: []durabledata.ExplicitPin{}}},
		{"supplied_pin", SelectedContractExecutionRequest{DataPinOverrides: []durabledata.ExplicitPin{{Declaration: recovered.Resume.Pins[0].Declaration, VersionID: recovered.Resume.Pins[0].VersionID}}}},
		{"matching_selection_is_not_caller_authority", SelectedContractExecutionRequest{ContractSelection: entry.Binding.ContractSelection}},
		{"conflicting_selection", SelectedContractExecutionRequest{ContractSelection: runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeBundleHash, BundleHash: entry.BundleHash}}},
		{"source_freeze", SelectedContractExecutionRequest{AllowSourceFreeze: true}},
		{"matching_source_fact_is_not_caller_authority", SelectedContractExecutionRequest{SourceArtifactFact: fact}},
		{"effective_source_identity", SelectedContractExecutionRequest{EffectiveSourceIdentity: identity}},
		{"recovery_override", SelectedContractExecutionRequest{Recovery: &recovered}},
	} {
		t.Run(hostile.name, func(t *testing.T) {
			if _, err := owner.admitSelectedFiniteFeedRecovery(context.Background(), recovered, hostile.request); err == nil {
				t.Fatalf("caller-selected recovery context was admitted: %+v", hostile.request)
			}
			if store.calls != 1 {
				t.Fatalf("caller-selected recovery context reached durable binding reader %d times", store.calls-1)
			}
		})
	}
	if store.calls != 1 {
		t.Fatalf("hostile request reached durable binding reader %d times", store.calls-1)
	}
	store.binding.ForkPoint.Revision++
	if _, err := owner.admitSelectedFiniteFeedRecovery(context.Background(), recovered, request); err == nil {
		t.Fatal("foreign durable revision was admitted")
	}
	recovered.Disposition = runfork.SelectedForkRecoveryActivateFiniteFeed
	store.binding = entry.Binding
	admitted, err = owner.admitSelectedFiniteFeedRecovery(context.Background(), recovered, request)
	if err != nil || admitted.action != selectedRecoveryActivateFiniteFeed {
		t.Fatalf("quiesced activation admission=%+v err=%v", admitted, err)
	}
}
