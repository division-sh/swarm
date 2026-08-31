package runforkexecution

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/providerconnectors"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
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
		ForkRunID:             forkRunID,
		BindingReader:         reader,
		SourceLoader:          sourceLoader,
		FrontierAdmission:     frontier,
		RouteAdmission:        routeAdmission,
		RouteTopology:         routeTopology,
		ExecutionModel:        model,
		DeferredWorkAdmission: selectedContractDeferredWorkAdmissionForTest(t, binding.SourceRunID, binding.ForkEventID, sourceLoader.loaded.Source),
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
	if admission.Owner != runfork.RunForkSelectedContractExecutionAdmissionOwner ||
		admission.FutureExecutionOwner != runfork.RunForkSelectedContractExecutionOwner ||
		!admission.NonMutating ||
		admission.ExecutionSupported {
		t.Fatalf("admission ownership = %#v", admission)
	}
	if admission.ForkRunID != forkRunID ||
		admission.SourceRunID != binding.SourceRunID ||
		admission.ForkEventID != binding.ForkEventID ||
		admission.ContractBindingOwner != runfork.RunForkSelectedContractBindingOwner {
		t.Fatalf("admission binding lineage = %#v", admission)
	}
	if admission.AdmissionOwner != runfork.RunForkContractFrontierAdmissionOwner ||
		admission.ExecutionModelOwner != runfork.RunForkSelectedContractExecutionModelOwner ||
		admission.DeferredWorkAdmissionOwner != runfork.RunForkSelectedContractDeferredWorkAdmissionOwner ||
		admission.AdmissionUse != runfork.RunForkSelectedContractExecutionAdmissionUseDurableBinding {
		t.Fatalf("admission evidence accounting = %#v", admission)
	}
	if admission.SourceWorkflowName != "selected-workflow" || admission.SourceWorkflowVersion != "v2" {
		t.Fatalf("source workflow = %s@%s", admission.SourceWorkflowName, admission.SourceWorkflowVersion)
	}
	if admission.FrontierEventCount != 1 || len(admission.FrontierEvents) != 1 {
		t.Fatalf("frontier events = %#v", admission.FrontierEvents)
	}
	if admission.RouteTopology == nil || admission.RouteTopology.Owner != runfork.RunForkSelectedContractRouteTopologyOwner {
		t.Fatalf("route topology = %#v, want canonical selected-contract route topology", admission.RouteTopology)
	}
	if admission.RecipientPlanning == nil || admission.RecipientPlanning.Owner != runfork.RunForkSelectedContractRecipientPlanningOwner {
		t.Fatalf("recipient planning = %#v, want canonical selected-contract recipient planning", admission.RecipientPlanning)
	}
	if !executionBoundaryHas(admission.InvalidPaths, "copy_source_event_deliveries", runfork.RunForkSelectedContractDispositionInvalid) {
		t.Fatalf("invalid paths = %#v, want source delivery copy invalid", admission.InvalidPaths)
	}
	if !executionBoundaryHas(admission.RequiredConsumers, "fork_local_runtime_container", runfork.RunForkSelectedContractDispositionPrerequisite) ||
		!executionBoundaryHas(admission.RequiredConsumers, "fork_run_id_runtime_context", runfork.RunForkSelectedContractDispositionPrerequisite) ||
		!executionBoundaryHas(admission.RequiredConsumers, "fork_local_event_delivery_writes", runfork.RunForkSelectedContractDispositionPrerequisite) ||
		!executionBoundaryHas(admission.RequiredConsumers, "handler_execution", runfork.RunForkSelectedContractDispositionPrerequisite) ||
		!executionBoundaryHas(admission.RequiredConsumers, "emitted_follow_up_events", runfork.RunForkSelectedContractDispositionPrerequisite) {
		t.Fatalf("required consumers = %#v, want current runtime container prerequisites", admission.RequiredConsumers)
	}
	if !executionBoundaryHas(admission.BlockedSiblings, "sessions_turns_audits", runfork.RunForkSelectedContractDispositionBlockedSibling) {
		t.Fatalf("blocked siblings = %#v, want sessions/turns blocked", admission.BlockedSiblings)
	}
	if !unsupportedBlockerHas(admission.UnsupportedBlockers, runfork.RunForkBlockerSelectedContractExecutionAdmissionNonMutating) {
		t.Fatalf("unsupported blockers = %#v, want non-mutating admission blocker", admission.UnsupportedBlockers)
	}
	if !unsupportedBlockerHas(admission.UnsupportedBlockers, runfork.RunForkBlockerSelectedContractRouteAdmissionNonMutating) {
		t.Fatalf("unsupported blockers = %#v, want non-mutating route admission blocker", admission.UnsupportedBlockers)
	}
}

func TestBuildSelectedContractExecutionAdmissionRequiresDeferredWorkAdmission(t *testing.T) {
	ctx := context.Background()
	forkRunID := uuid.NewString()
	binding := testSelectedContractBinding(forkRunID)
	sourceLoader := &fakeSelectedContractSourceLoader{loaded: testLoadedSelectedSource(binding.ContractSelection)}
	frontier := testContractFrontierAdmission(binding.ContractSelection)
	routeAdmission := testSelectedContractRouteAdmission(frontier)
	routeTopology := testSelectedContractRouteTopologyFromAdmission(t, frontier, routeAdmission)

	_, err := BuildSelectedContractExecutionAdmission(ctx, SelectedContractExecutionAdmissionRequest{
		ForkRunID:         forkRunID,
		BindingReader:     &fakeSelectedContractBindingReader{binding: binding},
		SourceLoader:      sourceLoader,
		FrontierAdmission: frontier,
		RouteAdmission:    routeAdmission,
		RouteTopology:     routeTopology,
		ExecutionModel:    testSelectedContractExecutionModel(t, frontier),
	})
	if err == nil || !strings.Contains(err.Error(), runfork.RunForkSelectedContractDeferredWorkAdmissionOwner) {
		t.Fatalf("error = %v, want deferred-work admission failure", err)
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
	mismatched := runfork.RunForkContractSelection{
		Mode:       runfork.RunForkContractSelectionModeBundleHash,
		BundleHash: runForkTestBundleHash,
	}

	_, err := BuildSelectedContractExecutionAdmission(ctx, SelectedContractExecutionAdmissionRequest{
		ForkRunID:     forkRunID,
		BindingReader: &fakeSelectedContractBindingReader{binding: binding},
		SourceLoader: &fakeSelectedContractSourceLoader{loaded: LoadedSelectedContractSource{
			Selection:          mismatched,
			Source:             testSelectedSource(mismatched),
			SourceArtifactFact: testEphemeralSourceArtifactFact(runForkTestBundleHash),
		}},
		FrontierAdmission: frontier,
		RouteAdmission:    routeAdmission,
		RouteTopology:     testSelectedContractRouteTopologyFromAdmission(t, frontier, routeAdmission),
		ExecutionModel:    model,
	})
	if err == nil || !strings.Contains(err.Error(), "selected source selection does not match durable binding") {
		t.Fatalf("error = %v, want selected source mismatch", err)
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
	if err == nil || !strings.Contains(err.Error(), runfork.RunForkContractFrontierAdmissionOwner) {
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
	if err == nil || !strings.Contains(err.Error(), runfork.RunForkSelectedContractRouteTopologyOwner) {
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
	frontier.FrontierEvents[0].SourceClassifications = []string{runfork.RunForkPendingClassificationPending}
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
	frontier.FrontierEvents[0].SourceClassifications = []string{runfork.RunForkPendingClassificationPending}
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
	routeTopology.DynamicTopologyDisposition = runfork.RunForkSelectedContractDispositionForkLocalTruth
	routeTopology.UnsupportedBlockers = removeUnsupportedBlocker(routeTopology.UnsupportedBlockers, runfork.RunForkBlockerSelectedContractDynamicRouteTopologyUnproven)

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
	if err == nil || !strings.Contains(err.Error(), runfork.RunForkSelectedContractRecipientPlanningOwner) {
		t.Fatalf("error = %v, want canonical recipient planning failure", err)
	}
}

func TestSourceArtifactSelectedContractSourceLoaderLoadsPersistedSourceForRequest(t *testing.T) {
	ctx := context.Background()
	repoRoot := runForkExecutionRepoRoot(t)
	bundle := loadRunForkExecutionFixtureBundle(t, filepath.Join("tests", "tier12-runtime-fork", "test-selected-contract-fork-execution"))
	record := persistedSourceArtifactForTest(t, bundle)
	sourceRunID := uuid.NewString()
	artifactStore := &fakeSourceArtifactSelectedContractSourceStore{
		availability: runbundle.Availability{
			RunID:                 sourceRunID,
			Status:                "running",
			BundleHash:            record.BundleHash,
			SourceArtifactPresent: true,
		},
		record: record,
	}
	loader := SourceArtifactSelectedContractSourceLoader{RepoRoot: repoRoot, Store: artifactStore}
	selection := testDBLoadedContractSelection()

	loaded, err := loader.LoadRunForkSelectedContractSourceForRequest(ctx, SelectedContractSourceLoadRequest{
		SourceRunID: sourceRunID,
		BundleHash:  record.BundleHash,
		Selection:   selection,
	})
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSourceForRequest: %v", err)
	}
	projectionRoot := loaded.RuntimeProjection.PrivateRoot()
	if projectionRoot == "" {
		t.Fatal("loaded selected source omitted its runtime projection")
	}

	if loaded.SourceArtifactFact.BundleHash() != record.BundleHash {
		t.Fatalf("loaded bundle hash = %q, want %q", loaded.SourceArtifactFact.BundleHash(), record.BundleHash)
	}
	if loaded.Selection != selection {
		t.Fatalf("loaded selection = %#v", loaded.Selection)
	}
	if loaded.Source == nil || loaded.Module == nil || loaded.Cleanup == nil {
		t.Fatalf("loaded source = %#v, module = %#v, cleanup nil = %v", loaded.Source, loaded.Module, loaded.Cleanup == nil)
	}
	if artifactStore.requestedRunID != sourceRunID || artifactStore.requestedBundleHash != record.BundleHash {
		t.Fatalf("store requests = run:%q hash:%q", artifactStore.requestedRunID, artifactStore.requestedBundleHash)
	}
	for _, entry := range bundle.SourceArtifact.Entries() {
		projected, err := os.ReadFile(filepath.Join(projectionRoot, filepath.FromSlash(entry.Label())))
		if err != nil || !bytes.Equal(projected, entry.Bytes()) {
			t.Fatalf("selected projection member %s = %q, %v", entry.Label(), projected, err)
		}
	}
	cleanupLoadedSelectedContractSource(loaded)
	if _, err := os.Stat(projectionRoot); !os.IsNotExist(err) {
		t.Fatalf("selected source projection survived cleanup: %v", err)
	}
}

func TestSelectedContractSourceLoadersRejectPresentZeroBeforePublication(t *testing.T) {
	ctx := context.Background()
	repoRoot := runForkExecutionRepoRoot(t)
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier12-runtime-fork", "test-selected-contract-fork-execution")
	project := t.TempDir()
	if err := os.CopyFS(project, os.DirFS(fixtureRoot)); err != nil {
		t.Fatalf("copy selected-contract fixture: %v", err)
	}
	if err := os.WriteFile(filepath.Join(project, "agents.yaml"), []byte("{}\n"), 0o644); err != nil {
		t.Fatalf("write present-zero selected-contract file: %v", err)
	}
	diskSelection := runfork.RunForkContractSelection{
		Mode: runfork.RunForkContractSelectionModeSelectedContracts,
	}
	if _, err := (admittedFixtureSelectedContractSourceLoader{RepoRoot: repoRoot, SourceRoot: project}).LoadRunForkSelectedContractSource(ctx, diskSelection); err == nil ||
		!strings.Contains(err.Error(), "agents.yaml declares nothing - delete the file (absent means empty)") {
		t.Fatalf("disk selected-contract error = %v, want present-zero rejection", err)
	}

	artifactProject := t.TempDir()
	if err := os.CopyFS(artifactProject, os.DirFS(fixtureRoot)); err != nil {
		t.Fatalf("copy source artifact fixture: %v", err)
	}
	if err := os.WriteFile(filepath.Join(artifactProject, "types.yaml"), []byte("{}\n"), 0o644); err != nil {
		t.Fatalf("write present-zero source artifact file: %v", err)
	}
	artifact, err := sourceartifact.AdmitDirectory(artifactProject)
	if err != nil {
		t.Fatalf("admit present-zero source artifact: %v", err)
	}
	record := persistedSourceArtifactForAdmittedTest(t, artifact)
	artifactStore := &fakeSourceArtifactSelectedContractSourceStore{
		record: record,
	}
	artifactSelection := runfork.RunForkContractSelection{
		Mode:       runfork.RunForkContractSelectionModeBundleHash,
		BundleHash: record.BundleHash,
	}
	if _, err := (SourceArtifactSelectedContractSourceLoader{RepoRoot: repoRoot, Store: artifactStore}).LoadRunForkSelectedContractSource(ctx, artifactSelection); err == nil ||
		!strings.Contains(err.Error(), "types.yaml declares nothing - delete the file (absent means empty)") {
		t.Fatalf("source artifact selected-contract error = %v, want present-zero rejection", err)
	}
	if artifactStore.requestedBundleHash != record.BundleHash {
		t.Fatalf("source artifact selected-contract request hash = %q, want %q", artifactStore.requestedBundleHash, record.BundleHash)
	}
}

func TestSourceArtifactSelectedContractSourceLoaderPreservesImportedPackGenerationForFork(t *testing.T) {
	ctx := context.Background()
	repoRoot := runForkExecutionRepoRoot(t)
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier12-runtime-fork", "test-selected-contract-fork-execution")
	project := t.TempDir()
	if err := os.CopyFS(project, os.DirFS(fixtureRoot)); err != nil {
		t.Fatalf("copy selected-contract fixture: %v", err)
	}
	base := packfixture.EmbeddedBase(t)
	baseGenerations, err := packartifact.NewPlatformPackBaseGenerationOwner(base)
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := packartifact.ImportEmbeddedPack(project, "provider.telegram", base); err != nil || !changed {
		t.Fatalf("import Telegram pack changed=%t: %v", changed, err)
	}
	manifestPath := filepath.Join(project, "packs", "provider.telegram", packartifact.TriggerManifestFileName)
	body, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	edited := []byte(strings.Replace(string(body), "telegram update object is required", "fork project telegram update object is required", 1))
	if string(edited) == string(body) {
		t.Fatal("Telegram fork edit found no canonical field")
	}
	if err := os.WriteFile(manifestPath, edited, 0o644); err != nil {
		t.Fatal(err)
	}
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOptions(
		repoRoot,
		project,
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
		runtimecontracts.WorkflowContractLoadOptions{
			PlatformPackBase: base, AdmitPackInventory: packadmission.AdmitInventory,
		},
	)
	if err != nil {
		t.Fatalf("load imported project-pack bundle: %v", err)
	}
	record := persistedSourceArtifactForTest(t, bundle)
	telegram, ok := base.Lookup("provider.telegram")
	if !ok {
		t.Fatal("embedded Telegram pack is missing")
	}
	successorBody := []byte(strings.Replace(string(telegram.ManifestBody()), "telegram update object is required", "successor development telegram update object is required", 1))
	successor, _ := packfixture.DevelopmentBase(t, map[string][]byte{"provider.telegram": successorBody})
	if err := baseGenerations.Select(successor); err != nil {
		t.Fatalf("select successor development generation: %v", err)
	}
	sourceRunID := uuid.NewString()
	artifactStore := &fakeSourceArtifactSelectedContractSourceStore{
		availability: runbundle.Availability{
			RunID: sourceRunID, Status: "running", BundleHash: record.BundleHash,
			SourceArtifactPresent: true,
		},
		record: record,
	}
	loader := SourceArtifactSelectedContractSourceLoader{
		RepoRoot: repoRoot, PlatformSpecPath: runtimecontracts.DefaultPlatformSpecFile(repoRoot),
		PlatformPackBases: baseGenerations, Store: artifactStore,
	}
	loaded, err := loader.LoadRunForkSelectedContractSourceForRequest(ctx, SelectedContractSourceLoadRequest{
		SourceRunID: sourceRunID,
		BundleHash:  record.BundleHash,
		Selection: runfork.RunForkContractSelection{
			Mode: runfork.RunForkContractSelectionModeBundleHash, BundleHash: record.BundleHash,
		},
	})
	if err != nil {
		t.Fatalf("load imported project-pack fork source: %v", err)
	}
	defer cleanupLoadedSelectedContractSource(loaded)
	loadedBundle, ok := semanticview.Bundle(loaded.Source)
	if !ok || loadedBundle == nil || loadedBundle.PackInventory == nil {
		t.Fatalf("fork source bundle = %#v, present=%t", loadedBundle, ok)
	}
	entry, ok := loadedBundle.PackInventory.Lookup("provider.telegram")
	if !ok || entry.Source() != packartifact.ProvenanceProject || !entry.Modified() || !entry.ShadowsBase() ||
		loadedBundle.PackInventory.BaseDigest() != successor.Digest() || string(entry.ManifestBody()) != string(edited) {
		t.Fatalf("fork imported pack = %#v present=%t base=%s", entry, ok, loadedBundle.PackInventory.BaseDigest())
	}
}

func TestSourceArtifactSelectedContractSourceLoaderUsesRunningPlatformVersionForAdmission(t *testing.T) {
	ctx := context.Background()
	repoRoot := runForkExecutionRepoRoot(t)
	bundle := loadRunForkExecutionFixtureBundle(t, filepath.Join("tests", "tier12-runtime-fork", "test-selected-contract-fork-execution"))
	record := persistedSourceArtifactForTest(t, bundle)
	artifactStore := &fakeSourceArtifactSelectedContractSourceStore{
		record: record,
	}
	loader := SourceArtifactSelectedContractSourceLoader{
		RepoRoot:         repoRoot,
		PlatformSpecPath: writeRunForkExecutionPlatformSpecVersion(t, repoRoot, "0.8.0"),
		Store:            artifactStore,
	}

	_, err := loader.LoadRunForkSelectedContractSourceForRequest(ctx, SelectedContractSourceLoadRequest{
		BundleHash: record.BundleHash,
		Selection: runfork.RunForkContractSelection{
			Mode:       runfork.RunForkContractSelectionModeBundleHash,
			BundleHash: record.BundleHash,
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

func TestSourceArtifactSelectedContractSourceLoaderLoadsCrossBundleTargetSelection(t *testing.T) {
	ctx := context.Background()
	repoRoot := runForkExecutionRepoRoot(t)
	bundle := loadRunForkExecutionFixtureBundle(t, filepath.Join("tests", "tier12-runtime-fork", "test-selected-contract-fork-execution"))
	record := persistedSourceArtifactForTest(t, bundle)
	sourceRunID := uuid.NewString()
	artifactStore := &fakeSourceArtifactSelectedContractSourceStore{
		availability: runbundle.Availability{
			RunID:                 sourceRunID,
			Status:                "running",
			BundleHash:            "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			SourceArtifactPresent: true,
		},
		record: record,
	}
	loader := SourceArtifactSelectedContractSourceLoader{RepoRoot: repoRoot, Store: artifactStore}
	selection := runfork.RunForkContractSelection{
		Mode:       runfork.RunForkContractSelectionModeBundleHash,
		BundleHash: record.BundleHash,
	}

	loaded, err := loader.LoadRunForkSelectedContractSourceForRequest(ctx, SelectedContractSourceLoadRequest{
		SourceRunID: sourceRunID,
		BundleHash:  record.BundleHash,
		Selection:   selection,
	})
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSourceForRequest: %v", err)
	}
	defer cleanupLoadedSelectedContractSource(loaded)

	if artifactStore.requestedRunID != "" {
		t.Fatalf("requested source availability run = %q, want no source-run availability lookup for bundle_hash target", artifactStore.requestedRunID)
	}
	if artifactStore.requestedBundleHash != record.BundleHash {
		t.Fatalf("requested target hash = %q, want %q", artifactStore.requestedBundleHash, record.BundleHash)
	}
	if loaded.SourceArtifactFact.BundleHash() != record.BundleHash ||
		loaded.Selection.Mode != runfork.RunForkContractSelectionModeBundleHash ||
		loaded.Selection.BundleHash != record.BundleHash {
		t.Fatalf("loaded target source = %#v", loaded)
	}
}

func TestAdmittedSelectedContractSourceLoaderBindsExactPersistedBundleSelection(t *testing.T) {
	repoRoot := runForkExecutionRepoRoot(t)
	sourceRoot := filepath.Join(repoRoot, "tests", "tier12-runtime-fork", "test-selected-contract-fork-execution")
	selection := runfork.RunForkContractSelection{
		Mode: runfork.RunForkContractSelectionModeSelectedContracts,
	}
	loaded, err := (admittedFixtureSelectedContractSourceLoader{RepoRoot: repoRoot, SourceRoot: sourceRoot}).LoadRunForkSelectedContractSource(context.Background(), selection)
	if err != nil {
		t.Fatal(err)
	}
	persisted, err := runtimecorrelation.NewSourceArtifactFact(loaded.SourceArtifactFact.BundleHash())
	if err != nil {
		t.Fatal(err)
	}
	identity, err := scenarioexecution.NewEffectiveSourceIdentity(persisted, "sha256:"+strings.Repeat("9", 64))
	if err != nil {
		t.Fatal(err)
	}
	loader, err := NewAdmittedSelectedContractSourceLoader(loaded.Selection, loaded.Module, persisted, identity)
	if err != nil {
		t.Fatal(err)
	}

	target := runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeBundleHash, BundleHash: persisted.BundleHash()}
	admitted, err := loader.LoadRunForkSelectedContractSourceForRequest(context.Background(), SelectedContractSourceLoadRequest{Selection: target})
	if err != nil {
		t.Fatal(err)
	}
	if admitted.Selection.Mode != runfork.RunForkContractSelectionModeBundleHash ||
		admitted.Selection.BundleHash != persisted.BundleHash() {
		t.Fatalf("admitted target selection = %#v", admitted.Selection)
	}

	target.BundleHash = "bundle-v2:sha256:" + strings.Repeat("a", 64)
	if _, err := loader.LoadRunForkSelectedContractSourceForRequest(context.Background(), SelectedContractSourceLoadRequest{Selection: target}); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("mismatched bundle selection error = %v", err)
	}
}

func TestSelectedContractSourceLoadersCompileExactEffectiveConnectorResponses(t *testing.T) {
	repoRoot := runForkExecutionRepoRoot(t)
	for _, sourceKind := range []string{"flow_local", "pack_imported"} {
		t.Run(sourceKind, func(t *testing.T) {
			contractsRoot := copySelectedForkConnectorFixture(t, repoRoot)
			if sourceKind == "pack_imported" {
				convertSelectedTelegramFixtureToPackImport(t, contractsRoot)
			}

			t.Run("disk", func(t *testing.T) {
				loaded, err := (admittedFixtureSelectedContractSourceLoader{RepoRoot: repoRoot, SourceRoot: contractsRoot}).LoadRunForkSelectedContractSource(context.Background(), runfork.RunForkContractSelection{
					Mode: runfork.RunForkContractSelectionModeSelectedContracts,
				})
				if err != nil {
					t.Fatalf("LoadRunForkSelectedContractSource: %v", err)
				}
				assertLoadedSelectedConnectorResponse(t, loaded)
			})

			t.Run("source artifact", func(t *testing.T) {
				bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, contractsRoot, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
				if err != nil {
					t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
				}
				record := persistedSourceArtifactForTest(t, bundle)
				loader := SourceArtifactSelectedContractSourceLoader{
					RepoRoot: repoRoot,
					Store:    &fakeSourceArtifactSelectedContractSourceStore{record: record},
				}
				loaded, err := loader.LoadRunForkSelectedContractSource(context.Background(), runfork.RunForkContractSelection{
					Mode: runfork.RunForkContractSelectionModeBundleHash, BundleHash: record.BundleHash,
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

func copySelectedForkConnectorFixture(t *testing.T, repoRoot string) string {
	t.Helper()
	sourceRoot := filepath.Join(repoRoot, "internal/runtime/runforkexecution/testdata/selected_fork_flow_scoped_mcp")
	root := t.TempDir()
	if err := os.CopyFS(root, os.DirFS(sourceRoot)); err != nil {
		t.Fatalf("copy selected fork connector fixture: %v", err)
	}
	tool := `telegram.send_message:
  category: provider_connector
  description: send Telegram messages
  handler_type: http
  effect_class: non_idempotent_write
  credentials: [telegram_bot_token]
  input_schema:
    type: object
    properties:
      chat_id: {type: string}
      text: {type: string}
    required: [chat_id, text]
  output_schema: {type: object}
  response_success: {kind: http_status_2xx}
  http:
    method: POST
    url: https://example.invalid/bot{{credentials.telegram_bot_token}}/sendMessage
    body:
      chat_id: "{{input.chat_id}}"
      text: "{{input.text}}"
`
	if err := os.WriteFile(filepath.Join(root, "worker", "tools.yaml"), []byte(tool), 0o644); err != nil {
		t.Fatalf("write selected connector fixture: %v", err)
	}
	return root
}

func convertSelectedTelegramFixtureToPackImport(t *testing.T, contractsRoot string) {
	t.Helper()
	schemaPath := filepath.Join(contractsRoot, "worker", "schema.yaml")
	body, err := os.ReadFile(schemaPath)
	if err != nil {
		t.Fatalf("read selected worker schema fixture: %v", err)
	}
	body = append(body, []byte("imports:\n  connector_packs:\n    - {provider: telegram, tool: telegram.send_message}\n")...)
	if err := os.WriteFile(schemaPath, body, 0o644); err != nil {
		t.Fatalf("write selected worker schema fixture: %v", err)
	}
	if err := os.Remove(filepath.Join(contractsRoot, "worker", "tools.yaml")); err != nil {
		t.Fatalf("remove selected flow-local connector fixture: %v", err)
	}
}

func TestCompileSelectedContractSourceIsolatesEffectiveConnectorPlans(t *testing.T) {
	firstTool := selectedMockConnectorTool()
	secondTool := selectedMockConnectorTool()
	firstFact := testEphemeralSourceArtifactFact("bundle-v2:sha256:" + strings.Repeat("1", 64))
	first, firstPlan, firstIdentity, err := compileSelectedContractSource(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Tools: map[string]runtimecontracts.ToolSchemaEntry{
		"first.send": firstTool,
	}}), firstFact)
	if err != nil {
		t.Fatalf("compile first selected source: %v", err)
	}
	secondFact := testEphemeralSourceArtifactFact("bundle-v2:sha256:" + strings.Repeat("2", 64))
	second, secondPlan, secondIdentity, err := compileSelectedContractSource(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Tools: map[string]runtimecontracts.ToolSchemaEntry{
		"second.send": secondTool,
	}}), secondFact)
	if err != nil {
		t.Fatalf("compile second selected source: %v", err)
	}
	if !firstIdentity.SourceArtifactFact().Matches(firstFact) || !secondIdentity.SourceArtifactFact().Matches(secondFact) || firstIdentity.Equal(secondIdentity) {
		t.Fatalf("selected effective identities were not bound to their exact sources: first=%s second=%s", firstIdentity.Digest(), secondIdentity.Digest())
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
	materialized, err := admitted.Materialize()
	if err != nil {
		t.Fatalf("Materialize telegram.send_message: %v", err)
	}
	response, ok := materialized.(map[string]any)
	if !ok {
		t.Fatalf("telegram generated response = %T, want object", materialized)
	}
	if len(response) != 0 {
		t.Fatalf("telegram generated response = %#v, want canonical empty object", response)
	}
	ambient := packfixture.ConnectorTool(t, "github", "github.create_issue").Tool
	if _, err := loaded.MockConnectorResponses.Admit("github.create_issue", ambient); err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("selected response plan admitted unimported github.create_issue: %v", err)
	}
}

func selectedMockConnectorTool() runtimecontracts.ToolSchemaEntry {
	return runtimecontracts.MustToolSchemaEntry(runtimecontracts.WithToolCategory(providerconnectors.Category), runtimecontracts.WithToolHandler(runtimecontracts.MustToolHandlerKind("http")), runtimecontracts.WithToolEffect(runtimecontracts.NormalizeActivityEffectClass(string(runtimecontracts.ActivityEffectClassNonIdempotentWrite))), runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("object")),
		runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("object"))), runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: "POST", URL: "https://example.invalid/send"}), runtimecontracts.WithToolResponseSuccess(runtimecontracts.HTTPResponseSuccess{Kind: "http_status_2xx"}), runtimecontracts.WithToolCredentials("provider_token"))

}

func TestSourceArtifactSelectedContractSourceLoaderFailsClosedOnUnavailableStates(t *testing.T) {
	ctx := context.Background()
	sourceRunID := uuid.NewString()
	hash := "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	loader := SourceArtifactSelectedContractSourceLoader{
		RepoRoot: runForkExecutionRepoRoot(t),
		Store: &fakeSourceArtifactSelectedContractSourceStore{
			availability: runbundle.Availability{
				RunID:      sourceRunID,
				Status:     "paused",
				BundleHash: hash,
				ErrorCode:  runbundle.CodeBundleUnavailable,
				Cause:      "missing_source_artifact",
			},
		},
	}

	_, err := loader.LoadRunForkSelectedContractSourceForRequest(ctx, SelectedContractSourceLoadRequest{
		SourceRunID: sourceRunID,
		Selection:   testDBLoadedContractSelection(),
	})
	if err == nil || !strings.Contains(err.Error(), runbundle.CodeBundleUnavailable) {
		t.Fatalf("error = %v, want %s", err, runbundle.CodeBundleUnavailable)
	}
}

func TestSourceArtifactSelectedContractSourceLoaderFailsClosedOnMissingArtifact(t *testing.T) {
	ctx := context.Background()
	sourceRunID := uuid.NewString()
	hash := "bundle-v2:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	loader := SourceArtifactSelectedContractSourceLoader{
		RepoRoot: runForkExecutionRepoRoot(t),
		Store: &fakeSourceArtifactSelectedContractSourceStore{
			availability: runbundle.Availability{
				RunID:                 sourceRunID,
				Status:                "running",
				BundleHash:            hash,
				SourceArtifactPresent: true,
			},
			recordErr: sourceartifact.ErrNotFound,
		},
	}

	_, err := loader.LoadRunForkSelectedContractSourceForRequest(ctx, SelectedContractSourceLoadRequest{
		SourceRunID: sourceRunID,
		BundleHash:  hash,
		Selection:   testDBLoadedContractSelection(),
	})
	if err == nil || !strings.Contains(err.Error(), runbundle.CodeBundleDataIntegrityError) {
		t.Fatalf("error = %v, want %s", err, runbundle.CodeBundleDataIntegrityError)
	}
}

func TestSourceArtifactSelectedContractSourceLoaderFailsClosedOnMissingCrossBundleTarget(t *testing.T) {
	ctx := context.Background()
	targetHash := "bundle-v2:sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	artifactStore := &fakeSourceArtifactSelectedContractSourceStore{recordErr: sourceartifact.ErrNotFound}
	loader := SourceArtifactSelectedContractSourceLoader{
		RepoRoot: runForkExecutionRepoRoot(t),
		Store:    artifactStore,
	}

	_, err := loader.LoadRunForkSelectedContractSourceForRequest(ctx, SelectedContractSourceLoadRequest{
		SourceRunID: uuid.NewString(),
		BundleHash:  targetHash,
		Selection: runfork.RunForkContractSelection{
			Mode:       runfork.RunForkContractSelectionModeBundleHash,
			BundleHash: targetHash,
		},
	})
	if err == nil || !strings.Contains(err.Error(), runbundle.CodeBundleUnavailable) {
		t.Fatalf("error = %v, want %s", err, runbundle.CodeBundleUnavailable)
	}
	if artifactStore.requestedRunID != "" {
		t.Fatalf("requested source availability run = %q, want no source-run availability lookup for bundle_hash target", artifactStore.requestedRunID)
	}
}

func TestSourceArtifactSelectedContractSourceLoaderFailsClosedOnCorruptCrossBundleTarget(t *testing.T) {
	ctx := context.Background()
	targetHash := "bundle-v2:sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	loader := SourceArtifactSelectedContractSourceLoader{
		RepoRoot: runForkExecutionRepoRoot(t),
		Store: &fakeSourceArtifactSelectedContractSourceStore{
			record: sourceartifact.Persisted{
				BundleHash:  targetHash,
				SourceBlob:  []byte("not a source artifact"),
				MemberCount: 1,
				TotalBytes:  21,
				CreatedAt:   time.Now().UTC(),
			},
		},
	}

	_, err := loader.LoadRunForkSelectedContractSourceForRequest(ctx, SelectedContractSourceLoadRequest{
		SourceRunID: uuid.NewString(),
		BundleHash:  targetHash,
		Selection: runfork.RunForkContractSelection{
			Mode:       runfork.RunForkContractSelectionModeBundleHash,
			BundleHash: targetHash,
		},
	})
	if err == nil || !strings.Contains(err.Error(), runbundle.CodeBundleDataIntegrityError) {
		t.Fatalf("error = %v, want %s", err, runbundle.CodeBundleDataIntegrityError)
	}
}

type fakeSelectedContractBindingReader struct {
	binding            runfork.RunForkSelectedContractBinding
	err                error
	requestedForkRunID string
}

type fakeSelectedContractSourceLoader struct {
	loaded             LoadedSelectedContractSource
	err                error
	requestedSelection runfork.RunForkContractSelection
}

func TestLoadRunForkSelectedContractSourceRejectsExpectedIdentityMismatch(t *testing.T) {
	selection := testContractSelection()
	for _, tc := range []struct {
		name         string
		expectedFact runtimecorrelation.SourceArtifactFact
		want         string
	}{
		{name: "bundle hash", expectedFact: testEphemeralSourceArtifactFact("bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), want: "bundle_hash mismatch"},
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
				SourceArtifactFact: tc.expectedFact,
				Selection:          selection,
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

func (l *fakeSelectedContractSourceLoader) LoadRunForkSelectedContractSource(_ context.Context, selection runfork.RunForkContractSelection) (LoadedSelectedContractSource, error) {
	l.requestedSelection = selection
	if l.err != nil {
		return LoadedSelectedContractSource{}, l.err
	}
	return l.loaded, nil
}

func (l *fakeSelectedContractSourceLoader) LoadRunForkSelectedContractSourceForRequest(ctx context.Context, req SelectedContractSourceLoadRequest) (LoadedSelectedContractSource, error) {
	return l.LoadRunForkSelectedContractSource(ctx, req.Selection)
}

func (r *fakeSelectedContractBindingReader) RequireRunForkSelectedContractBinding(_ context.Context, forkRunID string) (runfork.RunForkSelectedContractBinding, error) {
	r.requestedForkRunID = forkRunID
	if r.err != nil {
		return runfork.RunForkSelectedContractBinding{}, r.err
	}
	return r.binding, nil
}

type fakeSourceArtifactSelectedContractSourceStore struct {
	availability        runbundle.Availability
	availabilityErr     error
	record              sourceartifact.Persisted
	recordErr           error
	requestedRunID      string
	requestedBundleHash string
}

func (s *fakeSourceArtifactSelectedContractSourceStore) LoadRunBundleAvailability(_ context.Context, runID string) (runbundle.Availability, error) {
	s.requestedRunID = runID
	if s.availabilityErr != nil {
		return runbundle.Availability{}, s.availabilityErr
	}
	return s.availability, nil
}

func (s *fakeSourceArtifactSelectedContractSourceStore) GetSourceArtifact(_ context.Context, bundleHash string) (sourceartifact.Persisted, error) {
	s.requestedBundleHash = bundleHash
	if s.recordErr != nil {
		return sourceartifact.Persisted{}, s.recordErr
	}
	return s.record, nil
}

func persistedSourceArtifactForTest(t testing.TB, bundle *runtimecontracts.WorkflowContractBundle) sourceartifact.Persisted {
	t.Helper()
	if bundle == nil || bundle.SourceArtifact == nil {
		t.Fatal("loaded source artifact is required")
	}
	return persistedSourceArtifactForAdmittedTest(t, bundle.SourceArtifact)
}

func persistedSourceArtifactForAdmittedTest(t testing.TB, artifact *sourceartifact.AdmittedSourceArtifact) sourceartifact.Persisted {
	t.Helper()
	record, err := sourceartifact.PersistedFromArtifact(artifact, time.Now().UTC())
	if err != nil {
		t.Fatalf("persist source artifact fixture: %v", err)
	}
	return record
}

func testDBLoadedContractSelection() runfork.RunForkContractSelection {
	return runfork.RunForkContractSelection{
		Mode: "selected_contracts",
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

func testSelectedContractBinding(forkRunID string) runfork.RunForkSelectedContractBinding {
	return runfork.RunForkSelectedContractBinding{
		Owner:       runfork.RunForkSelectedContractBindingOwner,
		ForkRunID:   forkRunID,
		SourceRunID: uuid.NewString(),
		ForkEventID: uuid.NewString(),
		ContractSelection: runfork.RunForkContractSelection{
			Mode: "selected_contracts",
		},
		CreatedAt: time.Unix(1700000900, 0).UTC(),
	}
}

func testContractSelection() runfork.RunForkContractSelection {
	return testSelectedContractBinding(uuid.NewString()).ContractSelection
}

func testSelectedSource(_ runfork.RunForkContractSelection) semanticview.Source {
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name:    "selected-workflow",
			Version: "v2",
		},
	})
}

func testLoadedSelectedSource(selection runfork.RunForkContractSelection) LoadedSelectedContractSource {
	fact := testEphemeralSourceArtifactFact(runForkTestBundleHash)
	return LoadedSelectedContractSource{
		Selection:               selection,
		Source:                  testSelectedSource(selection),
		SourceArtifactFact:      fact,
		EffectiveSourceIdentity: testEffectiveSourceIdentity(fact),
	}
}

func testEffectiveSourceIdentity(fact runtimecorrelation.SourceArtifactFact) scenarioexecution.EffectiveSourceIdentity {
	identity, err := scenarioexecution.NewEffectiveSourceIdentity(fact, "sha256:"+strings.Repeat("a", 64))
	if err != nil {
		panic(err)
	}
	return identity
}

func testEphemeralSourceArtifactFact(bundleHash string) runtimecorrelation.SourceArtifactFact {
	fact, err := runtimecorrelation.NewSourceArtifactFact(bundleHash)
	if err != nil {
		panic(err)
	}
	return fact
}

func testPersistedSourceArtifactFact(bundleHash string) runtimecorrelation.SourceArtifactFact {
	fact, err := runtimecorrelation.NewSourceArtifactFact(bundleHash)
	if err != nil {
		panic(err)
	}
	return fact
}

func selectedContractDeferredWorkAdmissionForTest(t testing.TB, sourceRunID, forkEventID string, source semanticview.Source) selectedContractDeferredWorkAdmission {
	t.Helper()
	admission, err := admitSelectedContractDeferredWork(runfork.RunForkPlan{
		SourceRunID: sourceRunID,
		ForkPoint:   runfork.RunForkPoint{EventID: forkEventID},
	}, source)
	if err != nil {
		t.Fatalf("admit selected-contract deferred work: %v", err)
	}
	return admission
}

func testContractFrontierAdmission(selection runfork.RunForkContractSelection) runfork.RunForkContractFrontierAdmission {
	return runfork.RunForkContractFrontierAdmission{
		Owner:                        runfork.RunForkContractFrontierAdmissionOwner,
		ContractSelection:            selection,
		NonMutating:                  true,
		HistoricalExecutionSupported: false,
		FrontierEventCount:           1,
		FrontierEvents: []runfork.RunForkContractFrontierEvent{{
			SourceEventID:           uuid.NewString(),
			EventName:               "work.begin",
			RuntimeEventOwners:      []string{mustRunForkNode("flow-a", "alpha-intake").Key()},
			WorkflowNodeSubscribers: []string{mustRunForkNode("flow-b", "beta-intake").Key()},
			DerivedRecipients: []runfork.RunForkContractFrontierRecipient{
				testNodeFrontierRecipient("alpha-intake", "flow-a/alpha-intake", "selected_contracts"),
			},
		}},
	}
}

func testSelectedContractRouteAdmission(frontier runfork.RunForkContractFrontierAdmission) runfork.RunForkSelectedContractRouteAdmission {
	frontierEventCount, frontierSourceEventIDs, frontierFingerprint := runfork.RunForkContractFrontierEvidenceBinding(frontier)
	return runfork.RunForkSelectedContractRouteAdmission{
		Owner:                          runfork.RunForkSelectedContractRouteAdmissionOwner,
		FutureRouteReconstructionOwner: runfork.RunForkSelectedContractExecutionOwner + ".route_reconstruction",
		NonMutating:                    true,
		RouteReconstructionSupported:   false,
		ContractSelection:              frontier.ContractSelection,
		FrontierAdmissionOwner:         frontier.Owner,
		FrontierEventCount:             frontierEventCount,
		FrontierSourceEventIDs:         frontierSourceEventIDs,
		FrontierEvidenceFingerprint:    frontierFingerprint,
		RequiredConsumers: []runfork.RunForkSelectedContractExecutionBoundary{{
			Concept:     "selected_source_route_derivation",
			Disposition: runfork.RunForkSelectedContractDispositionPrerequisite,
			Owner:       "internal/runtime/bus.DeriveRouteTable",
			Reason:      "test route admission consumes selected-source route derivation",
		}},
		BlockedSiblings: []runfork.RunForkSelectedContractExecutionBoundary{{
			Concept:     "mutating_route_reconstruction",
			Disposition: runfork.RunForkSelectedContractDispositionBlockedSibling,
			Owner:       runfork.RunForkSelectedContractExecutionOwner + ".route_reconstruction",
			Reason:      "test route admission remains non-mutating",
		}},
		InvalidPaths: []runfork.RunForkSelectedContractExecutionBoundary{{
			Concept:     "copy_source_routing_rules",
			Disposition: runfork.RunForkSelectedContractDispositionInvalid,
			Reason:      "test route admission rejects source route row copy",
		}},
		UnsupportedBlockers: []runfork.RunForkUnsupportedBlocker{{
			Code:    runfork.RunForkBlockerSelectedContractRouteAdmissionNonMutating,
			Message: "selected-contract route admission is non-mutating",
		}},
	}
}

func testSelectedContractExecutionModel(t *testing.T, frontier runfork.RunForkContractFrontierAdmission) runfork.RunForkSelectedContractExecution {
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

func testSelectedContractRouteTopologyFromAdmission(t *testing.T, frontier runfork.RunForkContractFrontierAdmission, routeAdmission runfork.RunForkSelectedContractRouteAdmission) runfork.RunForkSelectedContractRouteTopology {
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
