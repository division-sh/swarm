package runforkexecution

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/semanticview"
	"swarm/internal/store"
)

func TestBuildSelectedContractExecutionAdmissionConsumesDurableBinding(t *testing.T) {
	ctx := context.Background()
	forkRunID := uuid.NewString()
	binding := testSelectedContractBinding(forkRunID)
	reader := &fakeSelectedContractBindingReader{binding: binding}
	sourceLoader := &fakeSelectedContractSourceLoader{loaded: testLoadedSelectedSource(binding.ContractSelection)}
	frontier := testContractFrontierAdmission(binding.ContractSelection)
	model := testSelectedContractExecutionModel(t, frontier)

	admission, err := BuildSelectedContractExecutionAdmission(ctx, SelectedContractExecutionAdmissionRequest{
		ForkRunID:         forkRunID,
		BindingReader:     reader,
		SourceLoader:      sourceLoader,
		FrontierAdmission: frontier,
		ExecutionModel:    model,
	})
	if err != nil {
		t.Fatalf("BuildSelectedContractExecutionAdmission: %v", err)
	}
	if reader.requestedForkRunID != forkRunID {
		t.Fatalf("binding reader fork_run_id = %q, want %q", reader.requestedForkRunID, forkRunID)
	}
	if sourceLoader.requestedSelection != binding.ContractSelection {
		t.Fatalf("source loader selection = %#v, want binding selection %#v", sourceLoader.requestedSelection, binding.ContractSelection)
	}
	if admission.Owner != store.RunForkSelectedContractExecutionAdmissionOwner ||
		admission.FutureExecutionOwner != store.RunForkSelectedContractExecutionOwner ||
		!admission.NonMutating ||
		admission.ExecutionSupported {
		t.Fatalf("admission ownership = %#v", admission)
	}
	if admission.ForkRunID != forkRunID ||
		admission.SourceRunID != binding.SourceRunID ||
		admission.ForkEventID != binding.ForkEventID ||
		admission.ContractBindingOwner != store.RunForkSelectedContractBindingOwner {
		t.Fatalf("admission binding lineage = %#v", admission)
	}
	if admission.AdmissionOwner != store.RunForkContractFrontierAdmissionOwner ||
		admission.ExecutionModelOwner != store.RunForkSelectedContractExecutionModelOwner ||
		admission.AdmissionUse != store.RunForkSelectedContractExecutionAdmissionUseDurableBinding {
		t.Fatalf("admission evidence accounting = %#v", admission)
	}
	if admission.SourceWorkflowName != binding.ContractSelection.WorkflowName ||
		admission.SourceWorkflowVersion != binding.ContractSelection.WorkflowVersion {
		t.Fatalf("source workflow = %s@%s", admission.SourceWorkflowName, admission.SourceWorkflowVersion)
	}
	if admission.FrontierEventCount != 1 || len(admission.FrontierEvents) != 1 {
		t.Fatalf("frontier events = %#v", admission.FrontierEvents)
	}
	if !executionBoundaryHas(admission.InvalidPaths, "copy_source_event_deliveries", store.RunForkSelectedContractDispositionInvalid) {
		t.Fatalf("invalid paths = %#v, want source delivery copy invalid", admission.InvalidPaths)
	}
	if !executionBoundaryHas(admission.RequiredConsumers, "handler_execution", store.RunForkSelectedContractDispositionFutureOwnerRequired) {
		t.Fatalf("required consumers = %#v, want handler execution future owner", admission.RequiredConsumers)
	}
	if !executionBoundaryHas(admission.BlockedSiblings, "sessions_turns_audits", store.RunForkSelectedContractDispositionBlockedSibling) {
		t.Fatalf("blocked siblings = %#v, want sessions/turns blocked", admission.BlockedSiblings)
	}
	if !unsupportedBlockerHas(admission.UnsupportedBlockers, store.RunForkBlockerSelectedContractExecutionAdmissionNonMutating) {
		t.Fatalf("unsupported blockers = %#v, want non-mutating admission blocker", admission.UnsupportedBlockers)
	}
}

func TestBuildSelectedContractExecutionAdmissionFailsClosedOnMissingBinding(t *testing.T) {
	ctx := context.Background()
	forkRunID := uuid.NewString()
	selection := testContractSelection()
	frontier := testContractFrontierAdmission(selection)
	model := testSelectedContractExecutionModel(t, frontier)

	_, err := BuildSelectedContractExecutionAdmission(ctx, SelectedContractExecutionAdmissionRequest{
		ForkRunID:         forkRunID,
		BindingReader:     &fakeSelectedContractBindingReader{err: errors.New("selected contract binding not found")},
		SourceLoader:      &fakeSelectedContractSourceLoader{loaded: testLoadedSelectedSource(selection)},
		FrontierAdmission: frontier,
		ExecutionModel:    model,
	})
	if err == nil || !strings.Contains(err.Error(), "load selected-contract binding") {
		t.Fatalf("error = %v, want binding load failure", err)
	}
}

func TestBuildSelectedContractExecutionAdmissionFailsClosedOnUnavailableSelectedSource(t *testing.T) {
	ctx := context.Background()
	forkRunID := uuid.NewString()
	binding := testSelectedContractBinding(forkRunID)
	frontier := testContractFrontierAdmission(binding.ContractSelection)
	model := testSelectedContractExecutionModel(t, frontier)

	_, err := BuildSelectedContractExecutionAdmission(ctx, SelectedContractExecutionAdmissionRequest{
		ForkRunID:         forkRunID,
		BindingReader:     &fakeSelectedContractBindingReader{binding: binding},
		SourceLoader:      &fakeSelectedContractSourceLoader{loaded: LoadedSelectedContractSource{Selection: binding.ContractSelection}},
		FrontierAdmission: frontier,
		ExecutionModel:    model,
	})
	if err == nil || !strings.Contains(err.Error(), "selected semantic source") {
		t.Fatalf("error = %v, want selected source failure", err)
	}
}

func TestBuildSelectedContractExecutionAdmissionFailsClosedOnSourceMismatch(t *testing.T) {
	ctx := context.Background()
	forkRunID := uuid.NewString()
	binding := testSelectedContractBinding(forkRunID)
	frontier := testContractFrontierAdmission(binding.ContractSelection)
	model := testSelectedContractExecutionModel(t, frontier)
	mismatched := binding.ContractSelection
	mismatched.WorkflowVersion = "other-version"

	_, err := BuildSelectedContractExecutionAdmission(ctx, SelectedContractExecutionAdmissionRequest{
		ForkRunID:         forkRunID,
		BindingReader:     &fakeSelectedContractBindingReader{binding: binding},
		SourceLoader:      &fakeSelectedContractSourceLoader{loaded: LoadedSelectedContractSource{Selection: binding.ContractSelection, Source: testSelectedSource(mismatched)}},
		FrontierAdmission: frontier,
		ExecutionModel:    model,
	})
	if err == nil || !strings.Contains(err.Error(), "workflow version mismatch") {
		t.Fatalf("error = %v, want selected source mismatch", err)
	}
}

func TestBuildSelectedContractExecutionAdmissionFailsClosedOnWrongContractsRoot(t *testing.T) {
	ctx := context.Background()
	forkRunID := uuid.NewString()
	binding := testSelectedContractBinding(forkRunID)
	frontier := testContractFrontierAdmission(binding.ContractSelection)
	model := testSelectedContractExecutionModel(t, frontier)
	wrongRoot := binding.ContractSelection
	wrongRoot.ContractsRoot = "/tmp/other-selected-contracts"

	_, err := BuildSelectedContractExecutionAdmission(ctx, SelectedContractExecutionAdmissionRequest{
		ForkRunID:         forkRunID,
		BindingReader:     &fakeSelectedContractBindingReader{binding: binding},
		SourceLoader:      &fakeSelectedContractSourceLoader{loaded: testLoadedSelectedSource(wrongRoot)},
		FrontierAdmission: frontier,
		ExecutionModel:    model,
	})
	if err == nil || !strings.Contains(err.Error(), "selected source selection does not match durable binding") {
		t.Fatalf("error = %v, want wrong contracts_root source failure", err)
	}
}

func TestBuildSelectedContractExecutionAdmissionRequiresCanonicalEvidence(t *testing.T) {
	ctx := context.Background()
	forkRunID := uuid.NewString()
	binding := testSelectedContractBinding(forkRunID)
	frontier := testContractFrontierAdmission(binding.ContractSelection)
	model := testSelectedContractExecutionModel(t, frontier)
	frontier.Owner = "cmd.swarm.local_frontier"

	_, err := BuildSelectedContractExecutionAdmission(ctx, SelectedContractExecutionAdmissionRequest{
		ForkRunID:         forkRunID,
		BindingReader:     &fakeSelectedContractBindingReader{binding: binding},
		SourceLoader:      &fakeSelectedContractSourceLoader{loaded: testLoadedSelectedSource(binding.ContractSelection)},
		FrontierAdmission: frontier,
		ExecutionModel:    model,
	})
	if err == nil || !strings.Contains(err.Error(), store.RunForkContractFrontierAdmissionOwner) {
		t.Fatalf("error = %v, want canonical frontier failure", err)
	}
}

func TestBuildSelectedContractExecutionAdmissionFailsClosedOnStaleModelFrontier(t *testing.T) {
	ctx := context.Background()
	forkRunID := uuid.NewString()
	binding := testSelectedContractBinding(forkRunID)
	frontier := testContractFrontierAdmission(binding.ContractSelection)
	model := testSelectedContractExecutionModel(t, frontier)
	frontier.FrontierEvents[0].EventName = "work.changed"

	_, err := BuildSelectedContractExecutionAdmission(ctx, SelectedContractExecutionAdmissionRequest{
		ForkRunID:         forkRunID,
		BindingReader:     &fakeSelectedContractBindingReader{binding: binding},
		SourceLoader:      &fakeSelectedContractSourceLoader{loaded: testLoadedSelectedSource(binding.ContractSelection)},
		FrontierAdmission: frontier,
		ExecutionModel:    model,
	})
	if err == nil || !strings.Contains(err.Error(), "frontier events do not match") {
		t.Fatalf("error = %v, want stale frontier model failure", err)
	}
}

type fakeSelectedContractBindingReader struct {
	binding            store.RunForkSelectedContractBinding
	err                error
	requestedForkRunID string
}

type fakeSelectedContractSourceLoader struct {
	loaded             LoadedSelectedContractSource
	err                error
	requestedSelection store.RunForkContractSelection
}

func (l *fakeSelectedContractSourceLoader) LoadRunForkSelectedContractSource(_ context.Context, selection store.RunForkContractSelection) (LoadedSelectedContractSource, error) {
	l.requestedSelection = selection
	if l.err != nil {
		return LoadedSelectedContractSource{}, l.err
	}
	return l.loaded, nil
}

func (r *fakeSelectedContractBindingReader) RequireRunForkSelectedContractBinding(_ context.Context, forkRunID string) (store.RunForkSelectedContractBinding, error) {
	r.requestedForkRunID = forkRunID
	if r.err != nil {
		return store.RunForkSelectedContractBinding{}, r.err
	}
	return r.binding, nil
}

func testSelectedContractBinding(forkRunID string) store.RunForkSelectedContractBinding {
	return store.RunForkSelectedContractBinding{
		Owner:       store.RunForkSelectedContractBindingOwner,
		ForkRunID:   forkRunID,
		SourceRunID: uuid.NewString(),
		ForkEventID: uuid.NewString(),
		ContractSelection: store.RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v2",
		},
		CreatedAt: time.Unix(1700000900, 0).UTC(),
	}
}

func testContractSelection() store.RunForkContractSelection {
	return testSelectedContractBinding(uuid.NewString()).ContractSelection
}

func testSelectedSource(selection store.RunForkContractSelection) semanticview.Source {
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name:    selection.WorkflowName,
			Version: selection.WorkflowVersion,
		},
	})
}

func testLoadedSelectedSource(selection store.RunForkContractSelection) LoadedSelectedContractSource {
	return LoadedSelectedContractSource{
		Selection: selection,
		Source:    testSelectedSource(selection),
	}
}

func testContractFrontierAdmission(selection store.RunForkContractSelection) store.RunForkContractFrontierAdmission {
	return store.RunForkContractFrontierAdmission{
		Owner:                        store.RunForkContractFrontierAdmissionOwner,
		ContractSelection:            selection,
		NonMutating:                  true,
		HistoricalExecutionSupported: false,
		FrontierEventCount:           1,
		FrontierEvents: []store.RunForkContractFrontierEvent{{
			SourceEventID:           uuid.NewString(),
			EventName:               "work.begin",
			RuntimeEventOwners:      []string{"alpha-intake"},
			WorkflowNodeSubscribers: []string{"beta-intake"},
			DerivedRecipients: []store.RunForkContractFrontierRecipient{{
				SubscriberType: "node",
				SubscriberID:   "alpha-intake",
				Path:           "flow-a/alpha-intake",
				RouteSource:    "selected_contracts",
			}},
		}},
	}
}

func testSelectedContractExecutionModel(t *testing.T, frontier store.RunForkContractFrontierAdmission) store.RunForkSelectedContractExecution {
	t.Helper()
	model, err := BuildSelectedContractExecutionModel(SelectedContractExecutionModelRequest{Admission: frontier})
	if err != nil {
		t.Fatalf("BuildSelectedContractExecutionModel: %v", err)
	}
	return model
}
