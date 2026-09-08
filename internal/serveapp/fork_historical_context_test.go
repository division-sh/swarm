package serveapp

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/runforkreadiness"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/google/uuid"
)

// Settlement supersedes the pending delivery at R through real delivery writers.
// Corrupting only R must not be hidden by the valid latest/live delivery.
func TestRunForkHistoricalIdentityPublicExecutionBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			var selected *selectedStoreOwner
			previous := projectRuntimePersistenceForServe
			projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
				selected = owner
				return previous(owner)
			}
			t.Cleanup(func() { projectRuntimePersistenceForServe = previous })
			root := canonicalrouting.CopyForkReceiverBusinessMutationOwnership(t, false)
			rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, root)
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "start.seeded", "bundle_hash": rt.BundleHash,
				"payload": map[string]any{"token": "receiver-proof"}, "idempotency_key": "historical-context-seed",
			})
			waitForkReceiverSourceCompletion(t, rt, seed.RunID)
			requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "start.requested", "run_id": seed.RunID, "source_event_id": seed.EventID,
				"payload": map[string]any{"token": "receiver-proof"}, "idempotency_key": "historical-context-request",
			})
			waitForkReceiverSourceCompletion(t, rt, seed.RunID)
			var frontier, deliveryID string
			if err := rt.DB.QueryRow(`SELECT e.event_id, d.delivery_id FROM events e JOIN event_deliveries d ON d.event_id=e.event_id WHERE e.run_id=$1 AND e.event_name='producer/work.ready'`, seed.RunID).Scan(&frontier, &deliveryID); err != nil {
				t.Fatal(err)
			}
			sourceRows := readForkReceiverRows(t, rt, seed.RunID)
			sourceRoute := requireForkReceiverBusinessMutation(t, rt, seed.RunID, frontier, sourceRows["consumer"].ID)
			sibling := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
				"event_name": "start.seeded", "bundle_hash": rt.BundleHash,
				"payload": map[string]any{"token": "receiver-proof"}, "idempotency_key": "historical-context-sibling",
			})
			waitForkReceiverSourceCompletion(t, rt, sibling.RunID)
			family, ok := selected.RunFork()
			if !ok {
				t.Fatal("missing real selected fork owner")
			}
			ctx := servedControlProofAuthorActivityContext(t, rt)
			plan, err := family.Plan(ctx, runfork.RunForkPlanRequest{SourceRunID: seed.RunID, At: frontier})
			if err != nil {
				t.Fatal(err)
			}
			defaultPlan, err := family.Plan(ctx, runfork.RunForkPlanRequest{SourceRunID: seed.RunID})
			if err != nil || defaultPlan.ForkPoint.EventID != frontier || defaultPlan.ForkPoint.Revision != plan.ForkPoint.Revision {
				t.Fatalf("default selector must select this actual final publication: %#v, %v", defaultPlan.ForkPoint, err)
			}
			// Exercise the real direct store using an already-admitted request,
			// rather than allowing request construction to be the only guard.
			direct, ok := family.Availability().(forkHistoricalSelectedStore)
			if !ok {
				t.Fatal("selected availability projection does not expose the actual store fixture")
			}
			loader := runforkexecution.SourceArtifactSelectedContractSourceLoader{RepoRoot: repoRootForTest(), PlatformSpecPath: filepath.Join(repoRootForTest(), defaultPlatformSpecPath), Store: selected.SourceArtifactStore()}
			request, loaded := admitForkHistoricalSelectedRequest(t, ctx, direct, loader, plan, rt.BundleHash)
			var revision, latestRevision int64
			var original, latest []byte
			if err := rt.DB.QueryRow(`SELECT revision, fact FROM run_fork_fact_revisions WHERE run_id=$1 AND family='event_deliveries' AND fact_key=$2 AND revision<=$3 ORDER BY revision DESC LIMIT 1`, seed.RunID, deliveryID, plan.ForkPoint.Revision).Scan(&revision, &original); err != nil {
				t.Fatal(err)
			}
			if err := rt.DB.QueryRow(`SELECT revision, fact FROM run_fork_fact_revisions WHERE run_id=$1 AND family='event_deliveries' AND fact_key=$2 ORDER BY revision DESC LIMIT 1`, seed.RunID, deliveryID).Scan(&latestRevision, &latest); err != nil {
				t.Fatal(err)
			}
			var oldBody, latestBody map[string]any
			if err := json.Unmarshal(original, &oldBody); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(latest, &latestBody); err != nil {
				t.Fatal(err)
			}
			if latestRevision <= plan.ForkPoint.Revision || oldBody["status"] != "pending" || latestBody["status"] != "delivered" || latestBody["run_id"] != seed.RunID {
				t.Fatalf("need real pending R and later lawful settlement: R=%d old=%v latest=%d/%v", plan.ForkPoint.Revision, oldBody, latestRevision, latestBody)
			}
			oldBody["run_id"] = uuid.NewString()
			corrupt, err := json.Marshal(oldBody)
			if err != nil {
				t.Fatal(err)
			}
			writeFact := func(raw []byte) {
				t.Helper()
				result, err := rt.DB.Exec(`UPDATE run_fork_fact_revisions SET fact=$1 WHERE run_id=$2 AND family='event_deliveries' AND fact_key=$3 AND revision=$4`, string(raw), seed.RunID, deliveryID, revision)
				if err != nil {
					t.Fatal(err)
				}
				if n, err := result.RowsAffected(); err != nil || n != 1 {
					t.Fatalf("fault injection changed %d rows: %v", n, err)
				}
			}
			writeFact(corrupt)
			before := snapshotForkReceiverApplication(t, rt)
			requireUnchanged := func() {
				t.Helper()
				if after := snapshotForkReceiverApplication(t, rt); !reflect.DeepEqual(before, after) {
					for table := range before {
						if !reflect.DeepEqual(before[table], after[table]) {
							t.Errorf("refusal changed complete table %s", table)
						}
					}
					t.Fatal("historical refusal changed application rows (including API journal)")
				}
			}
			requireHistoricalRefusal := func(err error) {
				t.Helper()
				if err == nil || !strings.Contains(err.Error(), "event_deliveries owning run disagrees with ledger run") {
					t.Fatalf("expected contextual historical delivery refusal, got %v", err)
				}
				requireUnchanged()
			}
			for _, at := range []string{frontier, ""} {
				_, err := family.Plan(ctx, runfork.RunForkPlanRequest{SourceRunID: seed.RunID, At: at})
				requireHistoricalRefusal(err)
				_, err = family.Materialize(ctx, runfork.RunForkMaterializeRequest{SourceRunID: seed.RunID, At: at})
				requireHistoricalRefusal(err)
				result, err := family.Execute(ctx, runforkexecution.SelectedContractExecutionRequest{
					SourceRunID: seed.RunID, At: at, AllowSourceFreeze: true, ExpectedBundleHash: rt.BundleHash,
					SourceLoader:      runforkexecution.SourceArtifactSelectedContractSourceLoader{RepoRoot: repoRootForTest(), PlatformSpecPath: filepath.Join(repoRootForTest(), defaultPlatformSpecPath), Store: selected.SourceArtifactStore()},
					ContractSelection: runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeBundleHash, BundleHash: rt.BundleHash},
					AgentRuntime:      runforkexecution.SelectedContractAgentRuntimeOptions{ExecutionPosture: rt.Runtime.ExecutionPosture},
				})
				requireHistoricalRefusal(err)
				if result.Materialization.ForkRunID != "" || result.ExecutedEventCount != 0 {
					t.Fatalf("corrupt R reached child execution: %#v", result)
				}
				params := map[string]any{"source_run_id": seed.RunID, "confirm_source_freeze": true, "idempotency_key": "historical-refusal-" + at}
				if at != "" {
					params["fork_event_id"] = at
				}
				for attempt := 0; attempt < 2; attempt++ {
					rpcErr := requireServedJSONRPCError(t, rt.Endpoint, "run.fork", params)
					// HTTP sanitizes backend errors. The direct entry above proves the
					// exact owner refusal; this cell proves the real public write boundary.
					if rpcErr.Code != -32603 {
						t.Fatalf("unexpected public refusal: %+v", rpcErr)
					}
					// A failed operation releases its API idempotency reservation;
					// unlike successful execution it leaves no committed response row.
					requireUnchanged()
				}
			}
			_, err = direct.MaterializeRunForkForSelectedContractExecution(ctx, request)
			requireHistoricalRefusal(err)
			writeFact(original)
			var stillLatest []byte
			if err := rt.DB.QueryRow(`SELECT fact FROM run_fork_fact_revisions WHERE run_id=$1 AND family='event_deliveries' AND fact_key=$2 AND revision=$3`, seed.RunID, deliveryID, latestRevision).Scan(&stillLatest); err != nil || string(stillLatest) != string(latest) {
				t.Fatalf("latest fact was changed: %v", err)
			}
			staged, err := direct.MaterializeRunForkForSelectedContractExecution(ctx, request)
			if err != nil || staged.ForkRunID == "" || staged.SelectedContractBinding == nil || staged.ForkRunStatus != runfork.RunForkMaterializedStatus {
				t.Fatalf("lawful selected staging: %#v, %v", staged, err)
			}
			writeFact(corrupt)
			// The staged child, binding, readiness and revision rows are lawful.
			// Each refusal must preserve them exactly, not pretend no child exists.
			before = snapshotForkReceiverApplication(t, rt)
			_, err = direct.MaterializeRunForkForSelectedContractExecution(ctx, request)
			requireHistoricalRefusal(err)
			_, eventIDs, _, err := runfork.RunForkContractFrontierEvidenceBinding(request.FrontierAdmission)
			if err != nil {
				t.Fatal(err)
			}
			activation, err := direct.ActivateRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionActivateRequest{
				ExecutionSource: loaded.Source, ForkRunID: staged.ForkRunID, AllowSourceFreeze: true,
				AllowedSourceEventIDs: eventIDs, FrontierAdmission: request.FrontierAdmission,
				RouteTopology: request.RouteTopology, RecipientPlanning: request.RecipientPlanning,
			})
			requireHistoricalRefusal(err)
			if activation.Activated {
				t.Fatal("direct selected activation accepted corrupt R")
			}
			recovered, err := family.Activate(ctx, runforkexecution.SelectedContractActivationGateRequest{
				ForkRunID: staged.ForkRunID, AllowSourceFreeze: true, SourceLoader: loader,
				AgentRuntime: runforkexecution.SelectedContractAgentRuntimeOptions{ExecutionPosture: rt.Runtime.ExecutionPosture},
			})
			requireHistoricalRefusal(err)
			if recovered.Activated || recovered.ExecutedEventCount != 0 || len(recovered.ForkEvents) != 0 {
				t.Fatalf("recovered activation reached execution: %#v", recovered)
			}
			writeFact(original)
			// Source settlement (including pipeline receipt/handoff) and fault
			// restoration precede the healthy baseline. Source freeze/lineage and
			// the successful API response are lawful writes, not business effects.
			waitForkReceiverSourceCompletion(t, rt, seed.RunID)
			sourceBefore := readServedForkRecipientSourceDomain(t, rt, seed.RunID)
			siblingBefore := readServedForkRecipientSourceDomain(t, rt, sibling.RunID)
			sourceCompanions := readForkReceiverCompanions(t, rt, seed.RunID)
			siblingCompanions := readForkReceiverCompanions(t, rt, sibling.RunID)
			sourceNotices := readForkReceiverNoticeDomain(t, rt, seed.RunID)
			siblingNotices := readForkReceiverNoticeDomain(t, rt, sibling.RunID)
			var result apiv1.RunForkExecutionResult
			params := map[string]any{"source_run_id": seed.RunID, "fork_event_id": frontier, "confirm_source_freeze": true, "idempotency_key": "historical-restored-control"}
			requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &result)
			if result.ForkRunID == "" || result.ExecutedEventCount != 1 {
				t.Fatalf("restored historical reader did not execute actual static receiver: %#v", result)
			}
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, result.ForkRunID)
			childEvent := activityidentity.ForkLineageEventID(result.ForkRunID, frontier)
			childRoute := requireForkReceiverBusinessMutation(t, rt, result.ForkRunID, childEvent, sourceRows["consumer"].ID)
			if !reflect.DeepEqual(sourceRoute.ConnectClaim, childRoute.ConnectClaim) {
				t.Fatal("admitted historical route lost exact compiled connect evidence")
			}
			if !reflect.DeepEqual(sourceBefore, readServedForkRecipientSourceDomain(t, rt, seed.RunID)) || !reflect.DeepEqual(siblingBefore, readServedForkRecipientSourceDomain(t, rt, sibling.RunID)) ||
				!reflect.DeepEqual(sourceCompanions, readForkReceiverCompanions(t, rt, seed.RunID)) || !reflect.DeepEqual(siblingCompanions, readForkReceiverCompanions(t, rt, sibling.RunID)) ||
				!reflect.DeepEqual(sourceNotices, readForkReceiverNoticeDomain(t, rt, seed.RunID)) || !reflect.DeepEqual(siblingNotices, readForkReceiverNoticeDomain(t, rt, sibling.RunID)) {
				t.Fatal("healthy fork changed complete source/sibling business rows, companions, or notices")
			}
			completed := snapshotForkReceiverApplication(t, rt)
			for attempt := 0; attempt < 2; attempt++ {
				var replay apiv1.RunForkExecutionResult
				requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &replay)
				if !reflect.DeepEqual(result, replay) || !reflect.DeepEqual(completed, snapshotForkReceiverApplication(t, rt)) {
					t.Fatalf("successful API replay %d changed response or application rows", attempt)
				}
			}
		})
	}
}

type forkHistoricalSelectedStore interface {
	runforkexecution.SelectedContractForkLifecycle
	runforkexecution.SelectedContractReplayPersistence
}

func admitForkHistoricalSelectedRequest(t *testing.T, ctx context.Context, store forkHistoricalSelectedStore, loader runforkexecution.SourceArtifactSelectedContractSourceLoader, plan runfork.RunForkPlan, hash string) (runforkreadiness.MaterializeRequest, runforkexecution.LoadedSelectedContractSource) {
	t.Helper()
	selection := runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts}
	loaded, err := loader.LoadRunForkSelectedContractSourceForRequest(ctx, runforkexecution.SelectedContractSourceLoadRequest{SourceRunID: plan.SourceRunID, BundleHash: hash, Selection: selection})
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Cleanup != nil {
		t.Cleanup(func() {
			if err := loaded.Cleanup(); err != nil {
				t.Error(err)
			}
		})
	}
	frontier, err := runforkadmission.AdmitContractFrontier(runforkadmission.ContractFrontierRequest{Plan: plan, Source: loaded.Source, ContractSelection: selection})
	if err != nil {
		t.Fatal(err)
	}
	routes, err := runforkadmission.AdmitSelectedContractRouteHistory(runforkadmission.SelectedContractRouteHistoryRequest{Plan: plan, Source: loaded.Source, ContractSelection: selection, FrontierAdmission: frontier})
	if err != nil {
		t.Fatal(err)
	}
	topology, err := runforkexecution.BuildSelectedContractRouteTopology(runforkexecution.SelectedContractRouteTopologyRequest{Admission: frontier, RouteAdmission: routes})
	if err != nil {
		t.Fatal(err)
	}
	model, err := runforkexecution.BuildSelectedContractExecutionModel(runforkexecution.SelectedContractExecutionModelRequest{Admission: frontier, RouteAdmission: routes, RouteTopology: topology})
	if err != nil {
		t.Fatal(err)
	}
	_, eventIDs, _, err := runfork.RunForkContractFrontierEvidenceBinding(frontier)
	if err != nil {
		t.Fatal(err)
	}
	modes, err := store.LoadRunForkSelectedContractSourceEventModes(ctx, plan.SourceRunID, eventIDs)
	if err != nil || len(modes) != len(eventIDs) {
		t.Fatalf("source mode census: %v, %v", modes, err)
	}
	sourceModes := make(map[string]executionmode.Mode, len(modes))
	for i, id := range eventIDs {
		sourceModes[id] = modes[i]
	}
	readiness, err := runforkreadiness.Admit(runforkreadiness.AdmissionRequest{
		Binding: runforkreadiness.Binding{Plan: plan, ContractSelection: selection, SourceArtifactFact: loaded.SourceArtifactFact,
			EffectiveSourceIdentity: loaded.EffectiveSourceIdentity, FrontierAdmission: frontier, RecipientPlanning: *model.RecipientPlanning, SourceModes: sourceModes}, Source: loaded.Source,
	})
	if err != nil {
		t.Fatal(err)
	}
	return runforkreadiness.MaterializeRequest{
		SourceRunID: plan.SourceRunID, At: plan.ForkPoint.EventID, ContractSelection: selection,
		SourceArtifactFact: loaded.SourceArtifactFact, EffectiveSourceIdentity: loaded.EffectiveSourceIdentity,
		FrontierAdmission: frontier, RouteTopology: topology, RecipientPlanning: *model.RecipientPlanning, Readiness: readiness,
	}, loaded
}
