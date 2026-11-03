package runtimepersistence

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/google/uuid"
)

func TestServedLiveRunDeclarationHeadBindingBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, _, _ := newP16RaceStore(t, backend)
			fixture := newReceiverConfigActivationFixtureForStore(t, selected.(agentFixtureFlowStore), false, map[string]string{
				"schema.yaml":          "pins:\n  outputs:\n    - records.ready\n",
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
				ReconcileFlowRun(context.Context, runlifecycle.LiveRunScope, runtimecorrelation.SourceArtifactFact) (runlifecycle.FlowRunReconciliation, error)
				PauseRunControlOutcome(context.Context, runcontrol.TransitionRequest) (runcontrol.StoreTransition, error)
				ContinueRunControlOutcome(context.Context, runcontrol.TransitionRequest) (runcontrol.StoreTransition, error)
				StopRunControlOutcome(context.Context, runcontrol.TransitionRequest) (runcontrol.StoreTransition, error)
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
		})
	}
}
