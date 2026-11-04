package runtimepersistence

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	agenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/google/uuid"
)

func TestServedLiveRunDeclarationHeadBindingBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, db, _ := newP16RaceStore(t, backend)
			fixture := newReceiverConfigActivationFixtureForStore(t, selected.(agentFixtureFlowStore), false, map[string]string{
				"schema.yaml":          "outputs:\n  - records.ready\n",
				"events.yaml":          "records.ready:\n  body: text\n",
				"nested/schema.yaml":   "stages: []\n",
				"manifest.yaml":        "version: 1.0.0\nplatform_version: '*'\n",
				"nested/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
			}, nil, ownStoreTestAgentManager, nil)
			catalog, err := contracts.BuildDurableDataCatalog(fixture.bundle)
			if err != nil {
				t.Fatal(err)
			}
			store := selected.(interface {
				EnsureSourceArtifactWithData(context.Context, *sourceartifact.AdmittedSourceArtifact, durabledata.Catalog) (sourceartifact.EnsureResult, error)
				ExecuteDataSourceOperation(context.Context, durabledata.SourceCommand) (durabledata.SourceOperationResult, error)
				LoadRunResourceAccess(context.Context, string, []durabledata.DeclarationRef) ([]durabledata.ResourceAccessItem, error)
				LoadPinnedSource(context.Context, string, string, durabledata.DeclarationRef) (durabledata.PinnedSource, error)
				ReconcileFlowRun(context.Context, runlifecycle.LiveRunScope, runtimecorrelation.SourceArtifactFact) (runlifecycle.FlowRunReconciliation, error)
				PauseRunControlOutcome(context.Context, runcontrol.TransitionRequest) (runcontrol.StoreTransition, error)
				ContinueRunControlOutcome(context.Context, runcontrol.TransitionRequest) (runcontrol.StoreTransition, error)
				StopRunControlOutcome(context.Context, runcontrol.TransitionRequest) (runcontrol.StoreTransition, error)
			})
			forkWriter := selectedAdmissionLifecycleWriter(selected).(interface {
				InsertRunForkRunTx(context.Context, *mutationprotocol.Attempt, string, string, runfork.RunForkPoint, time.Time, runtimecorrelation.SourceArtifactFact, sourceartifact.RootAdmission) error
				SelectLiveRunTx(context.Context, *mutationprotocol.Attempt, string, time.Time) (string, error)
			})
			if _, err := store.EnsureSourceArtifactWithData(fixture.ctx, fixture.bundle.SourceArtifact, catalog); err != nil {
				t.Fatal(err)
			}
			ref, err := durabledata.ParseDeclarationRef(".", "records.ready")
			if err != nil {
				t.Fatal(err)
			}
			fact := mustStoreTestSourceArtifactFact(fixture.bundle.SourceArtifact.BundleHash())
			first, err := store.ReconcileFlowRun(fixture.ctx, runlifecycle.LiveRunScope{FlowPath: "."}, fact)
			if err != nil || !first.Created {
				t.Fatalf("create served run before data import: %+v err=%v", first, err)
			}
			read := func(runID string, want durabledata.VersionID) {
				t.Helper()
				items, err := store.LoadRunResourceAccess(fixture.ctx, runID, []durabledata.DeclarationRef{ref})
				if err != nil || len(items) != 1 || items[0].VersionID != want {
					t.Fatalf("run %s binding: items=%+v err=%v want=%s", runID, items, err, want)
				}
				pinned, err := store.LoadPinnedSource(fixture.ctx, runID, catalog.BundleHash, ref)
				if err != nil || pinned.VersionID != want {
					t.Fatalf("run %s pinned read: source=%+v err=%v want=%s", runID, pinned, err, want)
				}
			}
			if _, err := store.LoadRunResourceAccess(fixture.ctx, first.RunID, []durabledata.DeclarationRef{ref}); err == nil || !strings.Contains(err.Error(), "has no exact pin") {
				t.Fatalf("absent declaration head invented a run binding: %v", err)
			}
			importData := func(head durabledata.ExpectedHead, body string) durabledata.SourceOperationResult {
				t.Helper()
				result, err := store.ExecuteDataSourceOperation(fixture.ctx, durabledata.SourceCommand{
					Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: catalog.BundleHash,
					Declaration: ref, ExpectedHead: head, InputFormat: "jsonl", Input: []byte("{\"body\":\"" + body + "\"}\n"),
				})
				if err != nil || result.Outcome != "accepted" {
					t.Fatalf("import %s: %+v err=%v", body, result, err)
				}
				return result
			}
			v1 := importData(durabledata.AbsentHead(), "first")
			read(first.RunID, v1.Candidate.VersionID)
			second, err := store.ReconcileFlowRun(fixture.ctx, runlifecycle.LiveRunScope{FlowPath: "nested"}, fact)
			if err != nil || !second.Created {
				t.Fatalf("create served run after data import: %+v err=%v", second, err)
			}
			read(second.RunID, v1.Candidate.VersionID)
			v2 := importData(durabledata.VersionHead(v1.Candidate.VersionID), "second")
			read(first.RunID, v2.Candidate.VersionID)
			read(second.RunID, v2.Candidate.VersionID)
			pause := runcontrol.TransitionRequest{RunID: first.RunID, Reason: "r64-paused-head", ControlledBy: "test", Now: time.Now().UTC()}
			if outcome, err := store.PauseRunControlOutcome(fixture.ctx, pause); err != nil || !outcome.Acknowledged {
				t.Fatalf("pause served run: %+v err=%v", outcome, err)
			}
			v3 := importData(durabledata.VersionHead(v2.Candidate.VersionID), "third")
			read(first.RunID, v2.Candidate.VersionID)
			read(second.RunID, v3.Candidate.VersionID)
			if outcome, err := store.ContinueRunControlOutcome(fixture.ctx, pause); err != nil || !outcome.Acknowledged {
				t.Fatalf("continue served run: %+v err=%v", outcome, err)
			}
			read(first.RunID, v3.Candidate.VersionID)
			if outcome, err := store.PauseRunControlOutcome(fixture.ctx, pause); err != nil || !outcome.Acknowledged {
				t.Fatalf("pause before terminal retirement: %+v err=%v", outcome, err)
			}
			if outcome, err := store.StopRunControlOutcome(fixture.ctx, pause); err != nil || !outcome.Acknowledged {
				t.Fatalf("stop served run: %+v err=%v", outcome, err)
			}
			v4 := importData(durabledata.VersionHead(v3.Candidate.VersionID), "fourth")
			readLiveFork := func(runID string, want durabledata.VersionID) {
				t.Helper()
				items, err := store.LoadRunResourceAccess(fixture.ctx, runID, []durabledata.DeclarationRef{ref})
				if err != nil || len(items) != 1 || items[0].VersionID != want {
					t.Fatalf("live fork actor read: items=%+v err=%v want=%s", items, err, want)
				}
				pinned, err := store.LoadPinnedSource(fixture.ctx, runID, catalog.BundleHash, ref)
				if err != nil || pinned.VersionID != v4.Candidate.VersionID {
					t.Fatalf("live fork rewrote immutable source: source=%+v err=%v want=%s", pinned, err, v4.Candidate.VersionID)
				}
			}
			read(first.RunID, v3.Candidate.VersionID)
			read(second.RunID, v4.Candidate.VersionID)
			conflict, err := store.ExecuteDataSourceOperation(fixture.ctx, durabledata.SourceCommand{
				Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: catalog.BundleHash,
				Declaration: ref, ExpectedHead: durabledata.VersionHead(v2.Candidate.VersionID),
				InputFormat: "jsonl", Input: []byte("{\"body\":\"stale\"}\n"),
			})
			if err != nil || conflict.Outcome != "head_conflict" {
				t.Fatalf("stale import should refuse without moving bindings: %+v err=%v", conflict, err)
			}
			read(first.RunID, v3.Candidate.VersionID)
			read(second.RunID, v4.Candidate.VersionID)
			if outcome, err := store.PauseRunControlOutcome(fixture.ctx, runcontrol.TransitionRequest{RunID: second.RunID, Now: time.Now().UTC()}); err != nil || !outcome.Acknowledged {
				t.Fatalf("pause fork source: %+v err=%v", outcome, err)
			}
			v5 := importData(durabledata.VersionHead(v4.Candidate.VersionID), "fifth")
			read(second.RunID, v4.Candidate.VersionID)
			root, present, err := runtimecorrelation.RootAdmissionFromContext(fixture.ctx)
			if err != nil || !present {
				t.Fatalf("fork requires captured root admission: %v", err)
			}
			forkRunID := uuid.NewString()
			if err := runSelectedFixtureMutation(fixture.ctx, selected, "r64 materialize fork", func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
				return forkWriter.InsertRunForkRunTx(ctx, attempt, forkRunID, second.RunID,
					runfork.RunForkPoint{Kind: runfork.RunForkPointStateRevision, Revision: 1}, time.Now().UTC(), fact, root)
			}); err != nil {
				t.Fatalf("materialize selected fork run: %v", err)
			}
			pins, err := MaterializeDataForkPinsForTest(fixture.ctx, selected, second.RunID, forkRunID, catalog.BundleHash, nil, false)
			if err != nil || len(pins) != 1 || pins[0].VersionID != v4.Candidate.VersionID {
				t.Fatalf("historical fork cut: pins=%+v err=%v", pins, err)
			}
			read(forkRunID, v4.Candidate.VersionID)
			if err := runSelectedFixtureMutation(fixture.ctx, selected, "r64 select fork live", func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
				_, err := forkWriter.SelectLiveRunTx(ctx, attempt, forkRunID, time.Now().UTC())
				return err
			}); err != nil {
				t.Fatalf("select fork live: %v", err)
			}
			readLiveFork(forkRunID, v5.Candidate.VersionID)
			v6 := importData(durabledata.VersionHead(v5.Candidate.VersionID), "sixth")
			readLiveFork(forkRunID, v6.Candidate.VersionID)
			if outcome, err := store.PauseRunControlOutcome(fixture.ctx, runcontrol.TransitionRequest{RunID: forkRunID, Now: time.Now().UTC()}); err != nil || !outcome.Acknowledged {
				t.Fatalf("pause live fork: %+v err=%v", outcome, err)
			}
			v7 := importData(durabledata.VersionHead(v6.Candidate.VersionID), "seventh")
			read(forkRunID, v4.Candidate.VersionID)
			pins, err = MaterializeDataForkPinsForTest(fixture.ctx, selected, second.RunID, forkRunID, catalog.BundleHash, nil, true)
			if err != nil || len(pins) != 1 || pins[0].VersionID != v4.Candidate.VersionID {
				t.Fatalf("historical fork replay changed after imports: pins=%+v err=%v", pins, err)
			}
			if outcome, err := store.ContinueRunControlOutcome(fixture.ctx, runcontrol.TransitionRequest{RunID: forkRunID, Now: time.Now().UTC()}); err != nil || !outcome.Acknowledged {
				t.Fatalf("continue selected fork: %+v err=%v", outcome, err)
			}
			readLiveFork(forkRunID, v7.Candidate.VersionID)
			if outcome, err := store.PauseRunControlOutcome(fixture.ctx, runcontrol.TransitionRequest{RunID: forkRunID, Now: time.Now().UTC()}); err != nil || !outcome.Acknowledged {
				t.Fatalf("pause before fork stop: %+v err=%v", outcome, err)
			}
			if outcome, err := store.StopRunControlOutcome(fixture.ctx, runcontrol.TransitionRequest{RunID: forkRunID, Now: time.Now().UTC()}); err != nil || !outcome.Acknowledged {
				t.Fatalf("stop selected fork: %+v err=%v", outcome, err)
			}
			v8 := importData(durabledata.VersionHead(v7.Candidate.VersionID), "eighth")
			read(forkRunID, v4.Candidate.VersionID)
			read(second.RunID, v4.Candidate.VersionID)
			if outcome, err := store.ContinueRunControlOutcome(fixture.ctx, runcontrol.TransitionRequest{RunID: second.RunID, Now: time.Now().UTC()}); err != nil || !outcome.Acknowledged {
				t.Fatalf("resume compatible served run: %+v err=%v", outcome, err)
			}
			read(second.RunID, v8.Candidate.VersionID)
			if err := fixture.grant.Retire(fixture.ctx); err != nil {
				t.Fatalf("retire first fixture grant: %v", err)
			}
			process, err := agentfixture.ProcessCapability(t, fixture.ctx, selected.(agentfixture.Store))
			if err != nil {
				t.Fatal(err)
			}
			retireStoreTestWorkFixture(t)
			if err := process.Release(fixture.ctx); err != nil {
				t.Fatalf("release first fixture process: %v", err)
			}

			otherSelected := newStopCommitStoreOwner(t, backend, db)
			otherFiles := map[string]string{
				"schema.yaml":             "outputs:\n  - records.ready\n",
				"events.yaml":             "records.ready:\n  body: integer\n",
				"nested/schema.yaml":      "stages: []\n",
				"different/schema.yaml":   "stages: []\n",
				"manifest.yaml":           "version: 1.0.0\nplatform_version: '*'\n",
				"nested/manifest.yaml":    "version: 1.0.0\nplatform_version: '*'\n",
				"different/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
			}
			otherBundle := loadLifecyclePersistenceFixtureForTest(t, otherFiles)
			otherRooted, err := otherBundle.RootedSource()
			if err != nil {
				t.Fatal(err)
			}
			otherCoordinate, err := agenttopology.NewSourceCoordinate(otherRooted.Admission())
			if err != nil {
				t.Fatal(err)
			}
			otherPlan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{otherCoordinate}, nil)
			if err != nil {
				t.Fatal(err)
			}
			otherCtx := testAuthorActivityContextForBundle(otherBundle.SourceArtifact.BundleHash())
			otherProcess, err := agentfixture.ProcessCapability(t, otherCtx, otherSelected.(agentfixture.Store))
			if err != nil {
				t.Fatal(err)
			}
			previousPlan, exists, err := otherProcess.CurrentSourceSet(otherCtx)
			if err != nil || !exists {
				t.Fatalf("read first fixture source plan: exists=%v err=%v", exists, err)
			}
			if _, err := otherProcess.RestoreSourceSet(otherCtx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), ExpectedRevision: previousPlan.Revision, Plan: otherPlan}); err != nil {
				t.Fatalf("replace first fixture source plan: %v", err)
			}
			other := newReceiverConfigActivationFixtureForStore(t, otherSelected.(agentFixtureFlowStore), false, otherFiles, nil, ownStoreTestAgentManager, nil)
			otherStore := otherSelected.(interface {
				EnsureSourceArtifactWithData(context.Context, *sourceartifact.AdmittedSourceArtifact, durabledata.Catalog) (sourceartifact.EnsureResult, error)
				ExecuteDataSourceOperation(context.Context, durabledata.SourceCommand) (durabledata.SourceOperationResult, error)
				LoadRunResourceAccess(context.Context, string, []durabledata.DeclarationRef) ([]durabledata.ResourceAccessItem, error)
				LoadPinnedSource(context.Context, string, string, durabledata.DeclarationRef) (durabledata.PinnedSource, error)
				ReconcileFlowRun(context.Context, runlifecycle.LiveRunScope, runtimecorrelation.SourceArtifactFact) (runlifecycle.FlowRunReconciliation, error)
				PauseRunControlOutcome(context.Context, runcontrol.TransitionRequest) (runcontrol.StoreTransition, error)
			})
			otherCatalog, err := contracts.BuildDurableDataCatalog(other.bundle)
			if err != nil || len(otherCatalog.Declarations) != 1 || otherCatalog.Declarations[0].SchemaDigest == catalog.Declarations[0].SchemaDigest {
				t.Fatalf("two admitted bundles require incompatible schemas: %+v err=%v", otherCatalog, err)
			}
			if _, err := otherStore.EnsureSourceArtifactWithData(other.ctx, other.bundle.SourceArtifact, otherCatalog); err != nil {
				t.Fatal(err)
			}
			otherFact := mustStoreTestSourceArtifactFact(other.bundle.SourceArtifact.BundleHash())
			otherRun, err := otherStore.ReconcileFlowRun(other.ctx, runlifecycle.LiveRunScope{FlowPath: "different"}, otherFact)
			if err != nil || !otherRun.Created {
				t.Fatalf("create compatible second-bundle run: %+v err=%v", otherRun, err)
			}
			if _, err := otherStore.LoadRunResourceAccess(other.ctx, otherRun.RunID, []durabledata.DeclarationRef{ref}); err == nil || !strings.Contains(err.Error(), "has no exact pin") {
				t.Fatalf("incompatible existing head bound to second bundle: %v", err)
			}
			otherVersion, err := otherStore.ExecuteDataSourceOperation(other.ctx, durabledata.SourceCommand{
				Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: otherCatalog.BundleHash,
				Declaration: ref, ExpectedHead: durabledata.VersionHead(v8.Candidate.VersionID), InputFormat: "jsonl", Input: []byte("{\"body\":7}\n"),
			})
			if err != nil || otherVersion.Outcome != "accepted" {
				t.Fatalf("import incompatible head: %+v err=%v", otherVersion, err)
			}
			otherItems, err := otherStore.LoadRunResourceAccess(other.ctx, otherRun.RunID, []durabledata.DeclarationRef{ref})
			if err != nil || len(otherItems) != 1 || otherItems[0].VersionID != otherVersion.Candidate.VersionID {
				t.Fatalf("compatible second-bundle run did not advance: items=%+v err=%v", otherItems, err)
			}
			for _, retained := range []struct {
				runID string
				want  durabledata.VersionID
			}{{second.RunID, v8.Candidate.VersionID}, {forkRunID, v4.Candidate.VersionID}} {
				items, err := otherStore.LoadRunResourceAccess(other.ctx, retained.runID, []durabledata.DeclarationRef{ref})
				if err != nil || len(items) != 1 || items[0].VersionID != retained.want {
					t.Fatalf("incompatible import changed run %s binding: items=%+v err=%v want=%s", retained.runID, items, err, retained.want)
				}
				pinned, err := otherStore.LoadPinnedSource(other.ctx, retained.runID, catalog.BundleHash, ref)
				if err != nil || pinned.VersionID != retained.want {
					t.Fatalf("incompatible import changed run %s fork source: source=%+v err=%v want=%s", retained.runID, pinned, err, retained.want)
				}
			}
			if outcome, err := otherStore.PauseRunControlOutcome(other.ctx, runcontrol.TransitionRequest{RunID: otherRun.RunID, Now: time.Now().UTC()}); err != nil || !outcome.Acknowledged {
				t.Fatalf("pause second-bundle run before fork: %+v err=%v", outcome, err)
			}
			otherRoot, present, err := runtimecorrelation.RootAdmissionFromContext(other.ctx)
			if err != nil || !present {
				t.Fatalf("second bundle requires captured root admission: %v", err)
			}
			otherFork := uuid.NewString()
			otherForkWriter := selectedAdmissionLifecycleWriter(otherSelected).(interface {
				InsertRunForkRunTx(context.Context, *mutationprotocol.Attempt, string, string, runfork.RunForkPoint, time.Time, runtimecorrelation.SourceArtifactFact, sourceartifact.RootAdmission) error
			})
			if err := runSelectedFixtureMutation(other.ctx, otherSelected, "r64 materialize second bundle fork", func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
				return otherForkWriter.InsertRunForkRunTx(ctx, attempt, otherFork, otherRun.RunID,
					runfork.RunForkPoint{Kind: runfork.RunForkPointStateRevision, Revision: 1}, time.Now().UTC(), otherFact, otherRoot)
			}); err != nil {
				t.Fatalf("fork retained compatible bundle: %v", err)
			}
			otherPins, err := MaterializeDataForkPinsForTest(other.ctx, otherSelected, otherRun.RunID, otherFork, otherCatalog.BundleHash, nil, false)
			if err != nil || len(otherPins) != 1 || otherPins[0].VersionID != otherVersion.Candidate.VersionID {
				t.Fatalf("second-bundle fork cut: pins=%+v err=%v", otherPins, err)
			}
		})
	}
}
