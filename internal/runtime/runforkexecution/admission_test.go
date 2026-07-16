package runforkexecution

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/division-sh/swarm/internal/providerconnectors"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/runbundle"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
)

func TestBuildSelectedContractExecutionAdmissionConsumesDurableBinding(t *testing.T) {
	ctx := context.Background()
	forkRunID := uuid.NewString()
	binding := testSelectedContractBinding(forkRunID)
	reader := &fakeSelectedContractBindingReader{binding: binding}
	sourceLoader := &fakeSelectedContractSourceLoader{loaded: testLoadedSelectedSource(binding.ContractSelection)}
	frontier := testContractFrontierAdmission(binding.ContractSelection)
	routeAdmission := testSelectedContractRouteAdmission(frontier)
	routeTopology := testSelectedContractRouteTopologyFromAdmission(t, frontier, routeAdmission)
	model := testSelectedContractExecutionModel(t, frontier)

	admission, err := BuildSelectedContractExecutionAdmission(ctx, SelectedContractExecutionAdmissionRequest{
		ForkRunID:         forkRunID,
		BindingReader:     reader,
		SourceLoader:      sourceLoader,
		FrontierAdmission: frontier,
		RouteAdmission:    routeAdmission,
		RouteTopology:     routeTopology,
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
	if admission.RouteTopology == nil || admission.RouteTopology.Owner != store.RunForkSelectedContractRouteTopologyOwner {
		t.Fatalf("route topology = %#v, want canonical selected-contract route topology", admission.RouteTopology)
	}
	if admission.RecipientPlanning == nil || admission.RecipientPlanning.Owner != store.RunForkSelectedContractRecipientPlanningOwner {
		t.Fatalf("recipient planning = %#v, want canonical selected-contract recipient planning", admission.RecipientPlanning)
	}
	if !executionBoundaryHas(admission.InvalidPaths, "copy_source_event_deliveries", store.RunForkSelectedContractDispositionInvalid) {
		t.Fatalf("invalid paths = %#v, want source delivery copy invalid", admission.InvalidPaths)
	}
	if !executionBoundaryHas(admission.RequiredConsumers, "fork_local_runtime_container", store.RunForkSelectedContractDispositionPrerequisite) ||
		!executionBoundaryHas(admission.RequiredConsumers, "fork_run_id_runtime_context", store.RunForkSelectedContractDispositionPrerequisite) ||
		!executionBoundaryHas(admission.RequiredConsumers, "fork_local_event_delivery_writes", store.RunForkSelectedContractDispositionPrerequisite) ||
		!executionBoundaryHas(admission.RequiredConsumers, "handler_execution", store.RunForkSelectedContractDispositionPrerequisite) ||
		!executionBoundaryHas(admission.RequiredConsumers, "emitted_follow_up_events", store.RunForkSelectedContractDispositionPrerequisite) {
		t.Fatalf("required consumers = %#v, want current runtime container prerequisites", admission.RequiredConsumers)
	}
	if !executionBoundaryHas(admission.BlockedSiblings, "sessions_turns_audits", store.RunForkSelectedContractDispositionBlockedSibling) {
		t.Fatalf("blocked siblings = %#v, want sessions/turns blocked", admission.BlockedSiblings)
	}
	if !unsupportedBlockerHas(admission.UnsupportedBlockers, store.RunForkBlockerSelectedContractExecutionAdmissionNonMutating) {
		t.Fatalf("unsupported blockers = %#v, want non-mutating admission blocker", admission.UnsupportedBlockers)
	}
	if !unsupportedBlockerHas(admission.UnsupportedBlockers, store.RunForkBlockerSelectedContractRouteAdmissionNonMutating) {
		t.Fatalf("unsupported blockers = %#v, want non-mutating route admission blocker", admission.UnsupportedBlockers)
	}
}

func TestBuildSelectedContractExecutionAdmissionFailsClosedOnMissingBinding(t *testing.T) {
	ctx := context.Background()
	forkRunID := uuid.NewString()
	selection := testContractSelection()
	frontier := testContractFrontierAdmission(selection)
	model := testSelectedContractExecutionModel(t, frontier)
	routeAdmission := testSelectedContractRouteAdmission(frontier)

	_, err := BuildSelectedContractExecutionAdmission(ctx, SelectedContractExecutionAdmissionRequest{
		ForkRunID:         forkRunID,
		BindingReader:     &fakeSelectedContractBindingReader{err: errors.New("selected contract binding not found")},
		SourceLoader:      &fakeSelectedContractSourceLoader{loaded: testLoadedSelectedSource(selection)},
		FrontierAdmission: frontier,
		RouteAdmission:    routeAdmission,
		RouteTopology:     testSelectedContractRouteTopologyFromAdmission(t, frontier, routeAdmission),
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
	routeAdmission := testSelectedContractRouteAdmission(frontier)

	_, err := BuildSelectedContractExecutionAdmission(ctx, SelectedContractExecutionAdmissionRequest{
		ForkRunID:         forkRunID,
		BindingReader:     &fakeSelectedContractBindingReader{binding: binding},
		SourceLoader:      &fakeSelectedContractSourceLoader{loaded: LoadedSelectedContractSource{Selection: binding.ContractSelection}},
		FrontierAdmission: frontier,
		RouteAdmission:    routeAdmission,
		RouteTopology:     testSelectedContractRouteTopologyFromAdmission(t, frontier, routeAdmission),
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
	routeAdmission := testSelectedContractRouteAdmission(frontier)
	mismatched := binding.ContractSelection
	mismatched.WorkflowVersion = "other-version"

	_, err := BuildSelectedContractExecutionAdmission(ctx, SelectedContractExecutionAdmissionRequest{
		ForkRunID:         forkRunID,
		BindingReader:     &fakeSelectedContractBindingReader{binding: binding},
		SourceLoader:      &fakeSelectedContractSourceLoader{loaded: LoadedSelectedContractSource{Selection: binding.ContractSelection, Source: testSelectedSource(mismatched)}},
		FrontierAdmission: frontier,
		RouteAdmission:    routeAdmission,
		RouteTopology:     testSelectedContractRouteTopologyFromAdmission(t, frontier, routeAdmission),
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
	routeAdmission := testSelectedContractRouteAdmission(frontier)
	wrongRoot := binding.ContractSelection
	wrongRoot.ContractsRoot = "/tmp/other-selected-contracts"

	_, err := BuildSelectedContractExecutionAdmission(ctx, SelectedContractExecutionAdmissionRequest{
		ForkRunID:         forkRunID,
		BindingReader:     &fakeSelectedContractBindingReader{binding: binding},
		SourceLoader:      &fakeSelectedContractSourceLoader{loaded: testLoadedSelectedSource(wrongRoot)},
		FrontierAdmission: frontier,
		RouteAdmission:    routeAdmission,
		RouteTopology:     testSelectedContractRouteTopologyFromAdmission(t, frontier, routeAdmission),
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
	routeAdmission := testSelectedContractRouteAdmission(frontier)
	routeTopology := testSelectedContractRouteTopologyFromAdmission(t, frontier, routeAdmission)
	frontier.Owner = "cmd.swarm.local_frontier"

	_, err := BuildSelectedContractExecutionAdmission(ctx, SelectedContractExecutionAdmissionRequest{
		ForkRunID:         forkRunID,
		BindingReader:     &fakeSelectedContractBindingReader{binding: binding},
		SourceLoader:      &fakeSelectedContractSourceLoader{loaded: testLoadedSelectedSource(binding.ContractSelection)},
		FrontierAdmission: frontier,
		RouteAdmission:    routeAdmission,
		RouteTopology:     routeTopology,
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
	routeAdmission := testSelectedContractRouteAdmission(frontier)
	routeTopology := testSelectedContractRouteTopologyFromAdmission(t, frontier, routeAdmission)

	_, err := BuildSelectedContractExecutionAdmission(ctx, SelectedContractExecutionAdmissionRequest{
		ForkRunID:         forkRunID,
		BindingReader:     &fakeSelectedContractBindingReader{binding: binding},
		SourceLoader:      &fakeSelectedContractSourceLoader{loaded: testLoadedSelectedSource(binding.ContractSelection)},
		FrontierAdmission: frontier,
		RouteAdmission:    routeAdmission,
		RouteTopology:     routeTopology,
		ExecutionModel:    model,
	})
	if err == nil || !strings.Contains(err.Error(), "frontier events do not match") {
		t.Fatalf("error = %v, want stale frontier model failure", err)
	}
}

func TestBuildSelectedContractExecutionAdmissionRequiresCanonicalRouteTopology(t *testing.T) {
	ctx := context.Background()
	forkRunID := uuid.NewString()
	binding := testSelectedContractBinding(forkRunID)
	frontier := testContractFrontierAdmission(binding.ContractSelection)
	model := testSelectedContractExecutionModel(t, frontier)
	routeAdmission := testSelectedContractRouteAdmission(frontier)
	routeTopology := testSelectedContractRouteTopologyFromAdmission(t, frontier, routeAdmission)
	routeTopology.Owner = "cmd.swarm.route_helper"

	_, err := BuildSelectedContractExecutionAdmission(ctx, SelectedContractExecutionAdmissionRequest{
		ForkRunID:         forkRunID,
		BindingReader:     &fakeSelectedContractBindingReader{binding: binding},
		SourceLoader:      &fakeSelectedContractSourceLoader{loaded: testLoadedSelectedSource(binding.ContractSelection)},
		FrontierAdmission: frontier,
		RouteAdmission:    routeAdmission,
		RouteTopology:     routeTopology,
		ExecutionModel:    model,
	})
	if err == nil || !strings.Contains(err.Error(), store.RunForkSelectedContractRouteTopologyOwner) {
		t.Fatalf("error = %v, want canonical route topology failure", err)
	}
}

func TestBuildSelectedContractExecutionAdmissionFailsClosedOnStaleRouteTopologyFrontier(t *testing.T) {
	ctx := context.Background()
	forkRunID := uuid.NewString()
	binding := testSelectedContractBinding(forkRunID)
	frontier := testContractFrontierAdmission(binding.ContractSelection)
	routeAdmission := testSelectedContractRouteAdmission(frontier)
	routeTopology := testSelectedContractRouteTopologyFromAdmission(t, frontier, routeAdmission)
	model := testSelectedContractExecutionModel(t, frontier)
	frontier.FrontierEvents[0].EventName = "work.changed"

	_, err := BuildSelectedContractExecutionAdmission(ctx, SelectedContractExecutionAdmissionRequest{
		ForkRunID:         forkRunID,
		BindingReader:     &fakeSelectedContractBindingReader{binding: binding},
		SourceLoader:      &fakeSelectedContractSourceLoader{loaded: testLoadedSelectedSource(binding.ContractSelection)},
		FrontierAdmission: frontier,
		RouteAdmission:    routeAdmission,
		RouteTopology:     routeTopology,
		ExecutionModel:    model,
	})
	if err == nil || !strings.Contains(err.Error(), "frontier fingerprint mismatch") {
		t.Fatalf("error = %v, want stale route topology frontier failure", err)
	}
}

func TestBuildSelectedContractExecutionAdmissionFailsClosedOnStaleRouteTopologyFlowInstances(t *testing.T) {
	ctx := context.Background()
	forkRunID := uuid.NewString()
	binding := testSelectedContractBinding(forkRunID)
	frontier := testContractFrontierAdmission(binding.ContractSelection)
	frontier.FrontierEvents[0].EventName = "review/inst-1/task.started"
	frontier.FrontierEvents[0].SourceClassifications = []string{store.RunForkPendingClassificationPending}
	frontier.FrontierEvents[0].SourceFlowInstances = []string{"review/inst-1"}
	frontier.FrontierEvents[0].SourceSubscriberTypes = []string{"node"}
	frontier.FrontierEvents[0].SourceSubscriberIDs = []string{"source-node"}
	routeAdmission := testSelectedContractRouteAdmission(frontier)
	routeTopology := testSelectedContractRouteTopologyFromAdmission(t, frontier, routeAdmission)
	model := testSelectedContractExecutionModel(t, frontier)
	frontier.FrontierEvents[0].SourceFlowInstances = []string{"review/inst-2"}

	_, err := BuildSelectedContractExecutionAdmission(ctx, SelectedContractExecutionAdmissionRequest{
		ForkRunID:         forkRunID,
		BindingReader:     &fakeSelectedContractBindingReader{binding: binding},
		SourceLoader:      &fakeSelectedContractSourceLoader{loaded: testLoadedSelectedSource(binding.ContractSelection)},
		FrontierAdmission: frontier,
		RouteAdmission:    routeAdmission,
		RouteTopology:     routeTopology,
		ExecutionModel:    model,
	})
	if err == nil || !strings.Contains(err.Error(), "frontier fingerprint mismatch") {
		t.Fatalf("error = %v, want stale route topology flow-instance failure", err)
	}
}

func TestBuildSelectedContractExecutionAdmissionRejectsForgedRouteTopology(t *testing.T) {
	ctx := context.Background()
	forkRunID := uuid.NewString()
	binding := testSelectedContractBinding(forkRunID)
	frontier := testContractFrontierAdmission(binding.ContractSelection)
	frontier.FrontierEvents[0].EventName = "review/inst-1/task.started"
	frontier.FrontierEvents[0].SourceClassifications = []string{store.RunForkPendingClassificationPending}
	frontier.FrontierEvents[0].SourceFlowInstances = []string{"review/inst-1"}
	frontier.FrontierEvents[0].SourceSubscriberTypes = []string{"node"}
	frontier.FrontierEvents[0].SourceSubscriberIDs = []string{"source-node"}
	routeAdmission := testSelectedContractRouteAdmission(frontier)
	routeAdmission.DynamicFlowInstances = []string{"review/inst-1"}
	routeTopology := testSelectedContractRouteTopologyFromAdmission(t, frontier, routeAdmission)
	model, err := BuildSelectedContractExecutionModel(SelectedContractExecutionModelRequest{
		Admission:      frontier,
		RouteAdmission: routeAdmission,
		RouteTopology:  routeTopology,
	})
	if err != nil {
		t.Fatalf("BuildSelectedContractExecutionModel: %v", err)
	}
	routeTopology.DynamicFlowInstances = nil
	routeTopology.DynamicTopologySupported = true
	routeTopology.DynamicTopologyDisposition = store.RunForkSelectedContractDispositionForkLocalTruth
	routeTopology.UnsupportedBlockers = removeUnsupportedBlocker(routeTopology.UnsupportedBlockers, store.RunForkBlockerSelectedContractDynamicRouteTopologyUnproven)

	_, err = BuildSelectedContractExecutionAdmission(ctx, SelectedContractExecutionAdmissionRequest{
		ForkRunID:         forkRunID,
		BindingReader:     &fakeSelectedContractBindingReader{binding: binding},
		SourceLoader:      &fakeSelectedContractSourceLoader{loaded: testLoadedSelectedSource(binding.ContractSelection)},
		FrontierAdmission: frontier,
		RouteAdmission:    routeAdmission,
		RouteTopology:     routeTopology,
		ExecutionModel:    model,
	})
	if err == nil || !strings.Contains(err.Error(), "canonical route-admission evidence") {
		t.Fatalf("error = %v, want forged route topology admission failure", err)
	}
}

func TestBuildSelectedContractExecutionAdmissionRejectsForgedRecipientPlanning(t *testing.T) {
	ctx := context.Background()
	forkRunID := uuid.NewString()
	binding := testSelectedContractBinding(forkRunID)
	frontier := testContractFrontierAdmission(binding.ContractSelection)
	routeAdmission := testSelectedContractRouteAdmission(frontier)
	routeTopology := testSelectedContractRouteTopologyFromAdmission(t, frontier, routeAdmission)
	model := testSelectedContractExecutionModel(t, frontier)
	model.RecipientPlanning.Owner = "cmd.swarm.local_recipient_plan"

	_, err := BuildSelectedContractExecutionAdmission(ctx, SelectedContractExecutionAdmissionRequest{
		ForkRunID:         forkRunID,
		BindingReader:     &fakeSelectedContractBindingReader{binding: binding},
		SourceLoader:      &fakeSelectedContractSourceLoader{loaded: testLoadedSelectedSource(binding.ContractSelection)},
		FrontierAdmission: frontier,
		RouteAdmission:    routeAdmission,
		RouteTopology:     routeTopology,
		ExecutionModel:    model,
	})
	if err == nil || !strings.Contains(err.Error(), store.RunForkSelectedContractRecipientPlanningOwner) {
		t.Fatalf("error = %v, want canonical recipient planning failure", err)
	}
}

func TestBundleCatalogSelectedContractSourceLoaderLoadsPersistedSourceForRequest(t *testing.T) {
	ctx := context.Background()
	repoRoot := runForkExecutionRepoRoot(t)
	bundle := loadRunForkExecutionFixtureBundle(t, filepath.Join("tests", "tier12-runtime-fork", "test-selected-contract-fork-execution"))
	projection, err := runtimecontracts.BuildBundleCatalogProjection(bundle)
	if err != nil {
		t.Fatalf("BuildBundleCatalogProjection: %v", err)
	}
	sourceRunID := uuid.NewString()
	catalogStore := &fakeBundleCatalogSelectedContractSourceStore{
		availability: runbundle.Availability{
			RunID:            sourceRunID,
			Status:           "running",
			BundleHash:       projection.BundleHash,
			BundleSource:     storerunlifecycle.BundleSourcePersisted,
			BundleRowPresent: true,
		},
		record: store.BundleCatalogRuntimeRecord{
			BundleHash:  projection.BundleHash,
			ContentYAML: projection.ContentYAML,
			DataBlob:    projection.DataBlob,
		},
	}
	loader := BundleCatalogSelectedContractSourceLoader{RepoRoot: repoRoot, Store: catalogStore}
	selection := testDBLoadedContractSelection("/stale/db-loaded/source-root")

	loaded, err := loader.LoadRunForkSelectedContractSourceForRequest(ctx, SelectedContractSourceLoadRequest{
		SourceRunID: sourceRunID,
		BundleHash:  projection.BundleHash,
		Selection:   selection,
	})
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSourceForRequest: %v", err)
	}
	defer cleanupLoadedSelectedContractSource(loaded)

	if loaded.BundleHash != projection.BundleHash {
		t.Fatalf("loaded bundle hash = %q, want %q", loaded.BundleHash, projection.BundleHash)
	}
	if loaded.Selection.ContractsRoot != selection.ContractsRoot ||
		loaded.Selection.WorkflowName != "test-selected-contract-fork-execution" ||
		loaded.Selection.WorkflowVersion != "1.0.0" {
		t.Fatalf("loaded selection = %#v", loaded.Selection)
	}
	if loaded.Source == nil || loaded.Module == nil || loaded.Cleanup == nil {
		t.Fatalf("loaded source = %#v, module = %#v, cleanup nil = %v", loaded.Source, loaded.Module, loaded.Cleanup == nil)
	}
	if catalogStore.requestedRunID != sourceRunID || catalogStore.requestedBundleHash != projection.BundleHash {
		t.Fatalf("store requests = run:%q hash:%q", catalogStore.requestedRunID, catalogStore.requestedBundleHash)
	}
}

func TestContractBundleSourceLoaderRejectsIncompatiblePlatformVersion(t *testing.T) {
	ctx := context.Background()
	repoRoot := runForkExecutionRepoRoot(t)
	contractsRoot := writeSelectedContractPlatformVersionFixture(t, ">=0.8.0")
	loader := ContractBundleSourceLoader{RepoRoot: repoRoot}

	_, err := loader.LoadRunForkSelectedContractSource(ctx, store.RunForkContractSelection{
		Mode:            store.RunForkContractSelectionModeSelectedContracts,
		ContractsRoot:   contractsRoot,
		WorkflowName:    "selected-platform-version",
		WorkflowVersion: "1.0.0",
	})
	if err == nil {
		t.Fatal("LoadRunForkSelectedContractSource error = nil, want platform_version compatibility failure")
	}
	for _, want := range []string{
		"selected-contract source admission failed",
		`platform_version range ">=0.8.0" does not include running platform "0.7.0"`,
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("LoadRunForkSelectedContractSource error = %v, want substring %q", err, want)
		}
	}
}

func TestBundleCatalogSelectedContractSourceLoaderUsesRunningPlatformVersionForAdmission(t *testing.T) {
	ctx := context.Background()
	repoRoot := runForkExecutionRepoRoot(t)
	bundle := loadRunForkExecutionFixtureBundle(t, filepath.Join("tests", "tier12-runtime-fork", "test-selected-contract-fork-execution"))
	projection, err := runtimecontracts.BuildBundleCatalogProjection(bundle)
	if err != nil {
		t.Fatalf("BuildBundleCatalogProjection: %v", err)
	}
	catalogStore := &fakeBundleCatalogSelectedContractSourceStore{
		record: store.BundleCatalogRuntimeRecord{
			BundleHash:  projection.BundleHash,
			ContentYAML: projection.ContentYAML,
			DataBlob:    projection.DataBlob,
		},
	}
	loader := BundleCatalogSelectedContractSourceLoader{
		RepoRoot:         repoRoot,
		PlatformSpecPath: writeRunForkExecutionPlatformSpecVersion(t, repoRoot, "0.8.0"),
		Store:            catalogStore,
	}

	_, err = loader.LoadRunForkSelectedContractSourceForRequest(ctx, SelectedContractSourceLoadRequest{
		BundleHash: projection.BundleHash,
		Selection: store.RunForkContractSelection{
			Mode:       store.RunForkContractSelectionModeBundleHash,
			BundleHash: projection.BundleHash,
		},
	})
	if err == nil {
		t.Fatal("LoadRunForkSelectedContractSourceForRequest error = nil, want running platform compatibility failure")
	}
	for _, want := range []string{
		runbundle.CodeBundleDataIntegrityError,
		`platform_version range ">=0.7.0 <0.8.0" does not include running platform "0.8.0"`,
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("LoadRunForkSelectedContractSourceForRequest error = %v, want substring %q", err, want)
		}
	}
}

func TestBundleCatalogSelectedContractSourceLoaderLoadsCrossBundleTargetSelection(t *testing.T) {
	ctx := context.Background()
	repoRoot := runForkExecutionRepoRoot(t)
	bundle := loadRunForkExecutionFixtureBundle(t, filepath.Join("tests", "tier12-runtime-fork", "test-selected-contract-fork-execution"))
	projection, err := runtimecontracts.BuildBundleCatalogProjection(bundle)
	if err != nil {
		t.Fatalf("BuildBundleCatalogProjection: %v", err)
	}
	sourceRunID := uuid.NewString()
	catalogStore := &fakeBundleCatalogSelectedContractSourceStore{
		availability: runbundle.Availability{
			RunID:            sourceRunID,
			Status:           "running",
			BundleHash:       "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			BundleSource:     storerunlifecycle.BundleSourcePersisted,
			BundleRowPresent: true,
		},
		record: store.BundleCatalogRuntimeRecord{
			BundleHash:  projection.BundleHash,
			ContentYAML: projection.ContentYAML,
			DataBlob:    projection.DataBlob,
		},
	}
	loader := BundleCatalogSelectedContractSourceLoader{RepoRoot: repoRoot, Store: catalogStore}
	selection := store.RunForkContractSelection{
		Mode:       store.RunForkContractSelectionModeBundleHash,
		BundleHash: projection.BundleHash,
	}

	loaded, err := loader.LoadRunForkSelectedContractSourceForRequest(ctx, SelectedContractSourceLoadRequest{
		SourceRunID: sourceRunID,
		BundleHash:  projection.BundleHash,
		Selection:   selection,
	})
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSourceForRequest: %v", err)
	}
	defer cleanupLoadedSelectedContractSource(loaded)

	if catalogStore.requestedRunID != "" {
		t.Fatalf("requested source availability run = %q, want no source-run availability lookup for bundle_hash target", catalogStore.requestedRunID)
	}
	if catalogStore.requestedBundleHash != projection.BundleHash {
		t.Fatalf("requested target hash = %q, want %q", catalogStore.requestedBundleHash, projection.BundleHash)
	}
	if loaded.BundleHash != projection.BundleHash ||
		loaded.Selection.Mode != store.RunForkContractSelectionModeBundleHash ||
		loaded.Selection.BundleHash != projection.BundleHash ||
		loaded.Selection.WorkflowName != "test-selected-contract-fork-execution" ||
		loaded.Selection.WorkflowVersion != "1.0.0" {
		t.Fatalf("loaded target source = %#v", loaded)
	}
	if loaded.Selection.ContractsRoot != "" {
		t.Fatalf("loaded selection contracts_root = %q, want no path owner for bundle_hash mode", loaded.Selection.ContractsRoot)
	}
}

func TestSelectedContractSourceLoadersCompileExactEffectiveConnectorResponses(t *testing.T) {
	repoRoot := runForkExecutionRepoRoot(t)
	for _, sourceKind := range []string{"flow_local", "pack_imported"} {
		t.Run(sourceKind, func(t *testing.T) {
			var contractsRoot string
			if sourceKind == "flow_local" {
				contractsRoot = canonicalrouting.CopyStandingTelegramServe(t, "https://example.invalid")
			} else {
				contractsRoot = canonicalrouting.CopyStandingTelegramServe(t, "https://example.invalid")
				convertSelectedTelegramFixtureToPackImport(t, contractsRoot)
			}

			t.Run("disk", func(t *testing.T) {
				loaded, err := (ContractBundleSourceLoader{RepoRoot: repoRoot}).LoadRunForkSelectedContractSource(context.Background(), store.RunForkContractSelection{
					Mode:          store.RunForkContractSelectionModeSelectedContracts,
					ContractsRoot: contractsRoot,
				})
				if err != nil {
					t.Fatalf("LoadRunForkSelectedContractSource: %v", err)
				}
				assertLoadedSelectedConnectorResponse(t, loaded)
			})

			t.Run("catalog", func(t *testing.T) {
				bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, contractsRoot, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
				if err != nil {
					t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
				}
				projection, err := runtimecontracts.BuildBundleCatalogProjection(bundle)
				if err != nil {
					t.Fatalf("BuildBundleCatalogProjection: %v", err)
				}
				loader := BundleCatalogSelectedContractSourceLoader{
					RepoRoot: repoRoot,
					Store: &fakeBundleCatalogSelectedContractSourceStore{record: store.BundleCatalogRuntimeRecord{
						BundleHash: projection.BundleHash, ContentYAML: projection.ContentYAML, DataBlob: projection.DataBlob,
					}},
				}
				loaded, err := loader.LoadRunForkSelectedContractSource(context.Background(), store.RunForkContractSelection{
					Mode: store.RunForkContractSelectionModeBundleHash, BundleHash: projection.BundleHash,
				})
				if err != nil {
					t.Fatalf("LoadRunForkSelectedContractSource: %v", err)
				}
				defer cleanupLoadedSelectedContractSource(loaded)
				assertLoadedSelectedConnectorResponse(t, loaded)
			})
		})
	}
}

func convertSelectedTelegramFixtureToPackImport(t *testing.T, contractsRoot string) {
	t.Helper()
	packagePath := filepath.Join(contractsRoot, "package.yaml")
	body, err := os.ReadFile(packagePath)
	if err != nil {
		t.Fatalf("read selected package fixture: %v", err)
	}
	const marker = "platform_version: \">=0.7.0 <0.8.0\"\n"
	if !strings.Contains(string(body), marker) {
		t.Fatalf("selected package fixture is missing platform marker")
	}
	body = []byte(strings.Replace(string(body), marker, marker+"connector_packs:\n  imports:\n    - {provider: telegram, tool: telegram.send_message}\n", 1))
	if err := os.WriteFile(packagePath, body, 0o644); err != nil {
		t.Fatalf("write selected package fixture: %v", err)
	}
	if err := os.WriteFile(filepath.Join(contractsRoot, "flows", "telegram-chat", "tools.yaml"), []byte("{}\n"), 0o644); err != nil {
		t.Fatalf("remove selected flow-local connector fixture: %v", err)
	}
}

func TestCompileSelectedContractSourceIsolatesEffectiveConnectorPlans(t *testing.T) {
	firstTool := selectedMockConnectorTool()
	secondTool := selectedMockConnectorTool()
	first, firstPlan, err := compileSelectedContractSource(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Tools: map[string]runtimecontracts.ToolSchemaEntry{
		"first.send": firstTool,
	}}))
	if err != nil {
		t.Fatalf("compile first selected source: %v", err)
	}
	second, secondPlan, err := compileSelectedContractSource(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Tools: map[string]runtimecontracts.ToolSchemaEntry{
		"second.send": secondTool,
	}}))
	if err != nil {
		t.Fatalf("compile second selected source: %v", err)
	}
	if _, ok := first.ToolEntries()["second.send"]; ok {
		t.Fatal("first selected source contains second connector")
	}
	if _, ok := second.ToolEntries()["first.send"]; ok {
		t.Fatal("second selected source contains first connector")
	}
	if _, err := firstPlan.Admit("second.send", secondTool); err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("first selected plan admitted second connector: %v", err)
	}
	if _, err := secondPlan.Admit("first.send", firstTool); err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("second selected plan admitted first connector: %v", err)
	}
}

func assertLoadedSelectedConnectorResponse(t *testing.T, loaded LoadedSelectedContractSource) {
	t.Helper()
	tool, ok := loaded.Source.ToolEntries()["telegram.send_message"]
	if !ok {
		t.Fatal("loaded selected source is missing effective telegram.send_message")
	}
	if loaded.MockConnectorResponses == nil {
		t.Fatal("loaded selected source is missing its generated response plan")
	}
	admitted, err := loaded.MockConnectorResponses.Admit("telegram.send_message", tool)
	if err != nil {
		t.Fatalf("Admit telegram.send_message: %v", err)
	}
	response, err := admitted.Materialize()
	if err != nil {
		t.Fatalf("Materialize telegram.send_message: %v", err)
	}
	if len(response) != 0 {
		t.Fatalf("telegram generated response = %#v, want canonical empty object", response)
	}
	ambient, ok := providerconnectors.BuiltinTool("github", "github.create_issue")
	if !ok {
		t.Fatal("built-in github.create_issue fixture missing")
	}
	if _, err := loaded.MockConnectorResponses.Admit("github.create_issue", ambient); err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("selected response plan admitted unimported github.create_issue: %v", err)
	}
}

func selectedMockConnectorTool() runtimecontracts.ToolSchemaEntry {
	return runtimecontracts.ToolSchemaEntry{
		Category:        providerconnectors.Category,
		HandlerType:     "http",
		EffectClass:     string(runtimecontracts.ActivityEffectClassNonIdempotentWrite),
		Credentials:     []string{"provider_token"},
		OutputSchema:    runtimecontracts.ToolInputSchema{Type: "object"},
		ResponseSuccess: &runtimecontracts.HTTPResponseSuccess{Kind: "http_status_2xx"},
		HTTP:            &runtimecontracts.HTTPToolSpec{Method: "POST", URL: "https://example.invalid/send"},
	}
}

func TestBundleCatalogSelectedContractSourceLoaderFailsClosedOnUnavailableStates(t *testing.T) {
	ctx := context.Background()
	sourceRunID := uuid.NewString()
	hash := "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	loader := BundleCatalogSelectedContractSourceLoader{
		RepoRoot: runForkExecutionRepoRoot(t),
		Store: &fakeBundleCatalogSelectedContractSourceStore{
			availability: runbundle.Availability{
				RunID:        sourceRunID,
				Status:       "paused",
				BundleHash:   hash,
				BundleSource: storerunlifecycle.BundleSourceEphemeral,
				ErrorCode:    runbundle.CodeBundleUnavailable,
				Cause:        storerunlifecycle.BundleSourceEphemeral,
			},
		},
	}

	_, err := loader.LoadRunForkSelectedContractSourceForRequest(ctx, SelectedContractSourceLoadRequest{
		SourceRunID: sourceRunID,
		Selection:   testDBLoadedContractSelection("/stale/db-loaded/source-root"),
	})
	if err == nil || !strings.Contains(err.Error(), runbundle.CodeBundleUnavailable) {
		t.Fatalf("error = %v, want %s", err, runbundle.CodeBundleUnavailable)
	}
}

func TestBundleCatalogSelectedContractSourceLoaderFailsClosedOnMissingCatalogBytes(t *testing.T) {
	ctx := context.Background()
	sourceRunID := uuid.NewString()
	hash := "bundle-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	loader := BundleCatalogSelectedContractSourceLoader{
		RepoRoot: runForkExecutionRepoRoot(t),
		Store: &fakeBundleCatalogSelectedContractSourceStore{
			availability: runbundle.Availability{
				RunID:            sourceRunID,
				Status:           "running",
				BundleHash:       hash,
				BundleSource:     storerunlifecycle.BundleSourcePersisted,
				BundleRowPresent: true,
			},
			recordErr: store.ErrBundleNotFound,
		},
	}

	_, err := loader.LoadRunForkSelectedContractSourceForRequest(ctx, SelectedContractSourceLoadRequest{
		SourceRunID: sourceRunID,
		BundleHash:  hash,
		Selection:   testDBLoadedContractSelection("/stale/db-loaded/source-root"),
	})
	if err == nil || !strings.Contains(err.Error(), runbundle.CodeBundleDataIntegrityError) {
		t.Fatalf("error = %v, want %s", err, runbundle.CodeBundleDataIntegrityError)
	}
}

func TestBundleCatalogSelectedContractSourceLoaderFailsClosedOnMissingCrossBundleTarget(t *testing.T) {
	ctx := context.Background()
	targetHash := "bundle-v1:sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	catalogStore := &fakeBundleCatalogSelectedContractSourceStore{recordErr: store.ErrBundleNotFound}
	loader := BundleCatalogSelectedContractSourceLoader{
		RepoRoot: runForkExecutionRepoRoot(t),
		Store:    catalogStore,
	}

	_, err := loader.LoadRunForkSelectedContractSourceForRequest(ctx, SelectedContractSourceLoadRequest{
		SourceRunID: uuid.NewString(),
		BundleHash:  targetHash,
		Selection: store.RunForkContractSelection{
			Mode:       store.RunForkContractSelectionModeBundleHash,
			BundleHash: targetHash,
		},
	})
	if err == nil || !strings.Contains(err.Error(), runbundle.CodeBundleUnavailable) {
		t.Fatalf("error = %v, want %s", err, runbundle.CodeBundleUnavailable)
	}
	if catalogStore.requestedRunID != "" {
		t.Fatalf("requested source availability run = %q, want no source-run availability lookup for bundle_hash target", catalogStore.requestedRunID)
	}
}

func TestBundleCatalogSelectedContractSourceLoaderFailsClosedOnCorruptCrossBundleTarget(t *testing.T) {
	ctx := context.Background()
	targetHash := "bundle-v1:sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	loader := BundleCatalogSelectedContractSourceLoader{
		RepoRoot: runForkExecutionRepoRoot(t),
		Store: &fakeBundleCatalogSelectedContractSourceStore{
			record: store.BundleCatalogRuntimeRecord{
				BundleHash:  targetHash,
				ContentYAML: "projection_version: swarm.bundle.catalog.v1\nfiles: []\ncanonical_inputs: []\n",
			},
		},
	}

	_, err := loader.LoadRunForkSelectedContractSourceForRequest(ctx, SelectedContractSourceLoadRequest{
		SourceRunID: uuid.NewString(),
		BundleHash:  targetHash,
		Selection: store.RunForkContractSelection{
			Mode:       store.RunForkContractSelectionModeBundleHash,
			BundleHash: targetHash,
		},
	})
	if err == nil || !strings.Contains(err.Error(), runbundle.CodeBundleDataIntegrityError) {
		t.Fatalf("error = %v, want %s", err, runbundle.CodeBundleDataIntegrityError)
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

func TestLoadRunForkSelectedContractSourceRejectsExpectedIdentityMismatch(t *testing.T) {
	selection := testContractSelection()
	for _, tc := range []struct {
		name           string
		expectedHash   string
		expectedSource string
		want           string
	}{
		{name: "bundle hash", expectedHash: "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", expectedSource: "ephemeral", want: "bundle_hash mismatch"},
		{name: "bundle source", expectedHash: runForkTestBundleHash, expectedSource: "persisted", want: "bundle_source mismatch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cleaned := false
			loaded := testLoadedSelectedSource(selection)
			loaded.Cleanup = func() error {
				cleaned = true
				return nil
			}
			loader := &fakeSelectedContractSourceLoader{loaded: loaded}
			_, err := loadRunForkSelectedContractSource(context.Background(), loader, SelectedContractSourceLoadRequest{
				ExpectedBundleHash:   tc.expectedHash,
				ExpectedBundleSource: tc.expectedSource,
				Selection:            selection,
			})
			if err == nil || !strings.Contains(err.Error(), runbundle.CodeBundleDataIntegrityError) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %s %s", err, runbundle.CodeBundleDataIntegrityError, tc.want)
			}
			if !cleaned {
				t.Fatal("mismatched selected source was not cleaned up")
			}
		})
	}
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

type fakeBundleCatalogSelectedContractSourceStore struct {
	availability        runbundle.Availability
	availabilityErr     error
	record              store.BundleCatalogRuntimeRecord
	recordErr           error
	requestedRunID      string
	requestedBundleHash string
}

func (s *fakeBundleCatalogSelectedContractSourceStore) LoadRunBundleAvailability(_ context.Context, runID string) (runbundle.Availability, error) {
	s.requestedRunID = runID
	if s.availabilityErr != nil {
		return runbundle.Availability{}, s.availabilityErr
	}
	return s.availability, nil
}

func (s *fakeBundleCatalogSelectedContractSourceStore) LoadBundleCatalogRuntimeRecord(_ context.Context, bundleHash string) (store.BundleCatalogRuntimeRecord, error) {
	s.requestedBundleHash = bundleHash
	if s.recordErr != nil {
		return store.BundleCatalogRuntimeRecord{}, s.recordErr
	}
	return s.record, nil
}

func testDBLoadedContractSelection(contractsRoot string) store.RunForkContractSelection {
	return store.RunForkContractSelection{
		Mode:          "selected_contracts",
		ContractsRoot: contractsRoot,
	}
}

func loadRunForkExecutionFixtureBundle(t *testing.T, relativeRoot string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	repoRoot := runForkExecutionRepoRoot(t)
	platformSpecPath := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, filepath.Join(repoRoot, relativeRoot), platformSpecPath)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides(%s): %v", relativeRoot, err)
	}
	return bundle
}

func writeSelectedContractPlatformVersionFixture(t *testing.T, declaredRange string) string {
	t.Helper()

	root := t.TempDir()
	writeSelectedContractFixtureFile(t, filepath.Join(root, "package.yaml"), `name: selected-platform-version
version: "1.0.0"
platform_version: "`+declaredRange+`"
flows: []
`)
	writeSelectedContractFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: selected-platform-version\n")
	writeSelectedContractFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeSelectedContractFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeSelectedContractFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeSelectedContractFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeSelectedContractFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	return root
}

func writeSelectedContractFixtureFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func writeRunForkExecutionPlatformSpecVersion(t *testing.T, repoRoot, version string) string {
	t.Helper()

	raw, err := os.ReadFile(runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("read default platform spec: %v", err)
	}
	updated := strings.Replace(string(raw), "version: 0.7.0", "version: "+version, 1)
	if updated == string(raw) {
		t.Fatal("default platform spec did not contain expected version line")
	}
	path := filepath.Join(t.TempDir(), "platform-spec.yaml")
	if err := os.WriteFile(path, []byte(updated), 0o644); err != nil {
		t.Fatalf("write running platform spec: %v", err)
	}
	return path
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
		Selection:    selection,
		Source:       testSelectedSource(selection),
		BundleHash:   runForkTestBundleHash,
		BundleSource: "ephemeral",
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

func testSelectedContractRouteAdmission(frontier store.RunForkContractFrontierAdmission) store.RunForkSelectedContractRouteAdmission {
	frontierEventCount, frontierSourceEventIDs, frontierFingerprint := store.RunForkContractFrontierEvidenceBinding(frontier)
	return store.RunForkSelectedContractRouteAdmission{
		Owner:                          store.RunForkSelectedContractRouteAdmissionOwner,
		FutureRouteReconstructionOwner: store.RunForkSelectedContractExecutionOwner + ".route_reconstruction",
		NonMutating:                    true,
		RouteReconstructionSupported:   false,
		ContractSelection:              frontier.ContractSelection,
		FrontierAdmissionOwner:         frontier.Owner,
		FrontierEventCount:             frontierEventCount,
		FrontierSourceEventIDs:         frontierSourceEventIDs,
		FrontierEvidenceFingerprint:    frontierFingerprint,
		RequiredConsumers: []store.RunForkSelectedContractExecutionBoundary{{
			Concept:     "selected_source_route_derivation",
			Disposition: store.RunForkSelectedContractDispositionPrerequisite,
			Owner:       "internal/runtime/bus.DeriveRouteTable",
			Reason:      "test route admission consumes selected-source route derivation",
		}},
		BlockedSiblings: []store.RunForkSelectedContractExecutionBoundary{{
			Concept:     "mutating_route_reconstruction",
			Disposition: store.RunForkSelectedContractDispositionBlockedSibling,
			Owner:       store.RunForkSelectedContractExecutionOwner + ".route_reconstruction",
			Reason:      "test route admission remains non-mutating",
		}},
		InvalidPaths: []store.RunForkSelectedContractExecutionBoundary{{
			Concept:     "copy_source_routing_rules",
			Disposition: store.RunForkSelectedContractDispositionInvalid,
			Reason:      "test route admission rejects source route row copy",
		}},
		UnsupportedBlockers: []store.RunForkUnsupportedBlocker{{
			Code:    store.RunForkBlockerSelectedContractRouteAdmissionNonMutating,
			Message: "selected-contract route admission is non-mutating",
		}},
	}
}

func testSelectedContractExecutionModel(t *testing.T, frontier store.RunForkContractFrontierAdmission) store.RunForkSelectedContractExecution {
	t.Helper()
	routeAdmission := testSelectedContractRouteAdmission(frontier)
	model, err := BuildSelectedContractExecutionModel(SelectedContractExecutionModelRequest{
		Admission:      frontier,
		RouteAdmission: routeAdmission,
		RouteTopology:  testSelectedContractRouteTopologyFromAdmission(t, frontier, routeAdmission),
	})
	if err != nil {
		t.Fatalf("BuildSelectedContractExecutionModel: %v", err)
	}
	return model
}

func testSelectedContractRouteTopology(t *testing.T, frontier store.RunForkContractFrontierAdmission) store.RunForkSelectedContractRouteTopology {
	t.Helper()
	return testSelectedContractRouteTopologyFromAdmission(t, frontier, testSelectedContractRouteAdmission(frontier))
}

func testSelectedContractRouteTopologyFromAdmission(t *testing.T, frontier store.RunForkContractFrontierAdmission, routeAdmission store.RunForkSelectedContractRouteAdmission) store.RunForkSelectedContractRouteTopology {
	t.Helper()
	routeTopology, err := BuildSelectedContractRouteTopology(SelectedContractRouteTopologyRequest{
		Admission:      frontier,
		RouteAdmission: routeAdmission,
	})
	if err != nil {
		t.Fatalf("BuildSelectedContractRouteTopology: %v", err)
	}
	return routeTopology
}
