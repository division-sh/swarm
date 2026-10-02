package runtimepersistence_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecanonicaljson "github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

// This exercises the durable source/plan/publication owners, not the manager's
// declaration-to-plan builder. Builder and served revision proofs are separate.
func TestDynamicFlowCreationSourceRevisionPublicationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, order := range []string{"publication_first", "revision_first", "concurrent"} {
			t.Run(backend+"/"+order, func(t *testing.T) {
				f := newDynamicFlowCreationAtomicityFixture(t, backend)
				load := func() runtimepipeline.DynamicFlowRuntimeReadiness {
					t.Helper()
					item, found, err := f.workflow.LoadDynamicFlowRuntimeReadiness(f.ctx, f.runID, f.plan.Identity.Route())
					if err != nil || !found {
						t.Fatalf("load readiness: found=%v err=%v", found, err)
					}
					return item
				}
				original := load()
				artifact := sourceartifactfixture.New("schema.yaml", []byte("name: revised-creation-proof\n"))
				source := sourceartifactfixture.FactFor(artifact)
				sourceartifactfixture.RequireArtifact(t, f.ctx, f.selected, artifact)
				revise := func() error {
					_, err := f.selected.ReviseRunSource(f.ctx, runtimerunlifecycle.SourceRevisionRequest{RunID: f.runID, Source: source})
					return err
				}
				var publicationErr, revisionErr error
				switch order {
				case "publication_first":
					publicationErr = f.commit()
					revisionErr = revise()
				case "revision_first":
					revisionErr = revise()
					publicationErr = f.commit()
				case "concurrent":
					start := make(chan struct{})
					published, revised := make(chan error, 1), make(chan error, 1)
					go func() { <-start; published <- f.commit() }()
					go func() { <-start; revised <- revise() }()
					close(start)
					publicationErr, revisionErr = <-published, <-revised
				}
				if revisionErr != nil {
					t.Fatalf("source revision: %v", revisionErr)
				}
				if publicationErr != nil &&
					!errors.Is(publicationErr, runtimemanager.ErrRunExecutionNotOwned) &&
					!strings.Contains(publicationErr.Error(), "readiness source does not match persisted run") &&
					!strings.Contains(publicationErr.Error(), "mutation log bundle source fact does not match active run") {
					t.Fatalf("publication lost for an unrelated reason: %v", publicationErr)
				}
				if order == "publication_first" && publicationErr != nil {
					t.Fatalf("publication before revision: %v", publicationErr)
				}
				if order == "revision_first" && publicationErr == nil {
					t.Fatal("stale source published after revision")
				}
				wait := func(bus *runtimebus.EventBus) {
					t.Helper()
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					if err := bus.WaitForQuiescence(ctx); err != nil {
						t.Fatalf("join accepted publication work: %v", err)
					}
				}
				wait(f.bus)
				current := load()
				if !current.OwningRunSource.Matches(source) || !reflect.DeepEqual(current.Plan, original.Plan) {
					t.Fatal("source revision implicitly rewrote the frozen creation plan")
				}
				oldPublished := !current.CreationEventEmittedAt.IsZero()
				if oldPublished != (publicationErr == nil) {
					t.Fatalf("publication result disagrees with durable mark: err=%v readiness=%#v", publicationErr, current)
				}
				desired := current.Plan
				desired.BundleHash = source.BundleHash()
				if !oldPublished {
					creation := *current.Plan.CreationEvent
					creation.EventID = uuid.NewString()
					creation.Payload = []byte(`{"name":"beta"}`)
					desired.CreationEvent = &creation
				}
				ctx := runtimecorrelation.WithSourceArtifactFact(f.ctx, source)
				ctx = runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope("11111111-1111-1111-1111-111111111111", source.BundleHash()))
				reconcile := func(observed runtimepipeline.DynamicFlowRuntimeReadiness, expected runtimepipeline.DynamicFlowRuntimeReadinessPlan) error {
					_, err := f.workflow.ReconcileDynamicFlowRuntimeReadinessPlans(ctx, []runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliation{{Observed: observed, Expected: expected}}, time.Now().UTC())
					return err
				}
				if err := reconcile(original, desired); !runtimepipeline.IsDynamicFlowRuntimeReadinessObservationConflict(err) {
					t.Fatalf("pre-revision observation was not fenced: %v", err)
				}
				if got := load(); !reflect.DeepEqual(current, got) {
					t.Fatal("stale observation mutated readiness")
				}
				if err := reconcile(current, desired); err != nil {
					t.Fatalf("explicit pending-plan reconciliation: %v", err)
				}
				replaced := load()
				gotPlan, err := runtimecanonicaljson.Bytes(replaced.Plan)
				if err != nil {
					t.Fatal(err)
				}
				wantPlan, err := runtimecanonicaljson.Bytes(desired)
				if err != nil {
					t.Fatal(err)
				}
				if string(gotPlan) != string(wantPlan) || replaced.Phase != current.Phase || replaced.AttemptState != "superseded" || replaced.AttemptOrdinal != current.AttemptOrdinal {
					t.Fatalf("replacement changed installed progress before exact cleanup or retained admission: got=%s want=%s readiness=%+v", gotPlan, wantPlan, replaced)
				}
				if err := f.selected.VerifyDynamicFlowRuntimeActivationAttempt(ctx, f.attempt); err == nil {
					t.Fatal("superseded source retained executable attempt authority")
				}
				if err := f.selected.RetireDynamicFlowRuntimeActivationAttempt(ctx, f.attempt); err != nil {
					t.Fatalf("settle superseded creation attempt: %v", err)
				}
				settled := load()
				if settled.Phase != replaced.Phase || settled.AttemptState != "retired" || settled.AttemptOrdinal != replaced.AttemptOrdinal {
					t.Fatalf("retirement changed predecessor progress or failed to settle it: %+v", settled)
				}
				sourceSet, err := runtimeagenttopology.NewSourceSetPlan([]runtimeagenttopology.SourceCoordinate{{BundleHash: source.BundleHash()}}, nil)
				if err != nil {
					t.Fatal(err)
				}
				previousSet, found, err := f.process.CurrentSourceSet(ctx)
				if err != nil || !found {
					t.Fatalf("load predecessor source set: found=%v err=%v", found, err)
				}
				if _, err := f.process.RestoreSourceSet(ctx, runtimeagenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), ExpectedRevision: previousSet.Revision, Plan: sourceSet}); err != nil {
					t.Fatalf("restore revised source set: %v", err)
				}
				grant, err := f.process.IssueGenerationGrant(ctx, runtimestartupownership.GrantRequest{BundleHash: source.BundleHash(), RuntimeInstanceID: "11111111-1111-1111-1111-111111111111", RuntimeGeneration: 2, SourceSetRevision: sourceSet.Revision})
				if err != nil {
					t.Fatalf("issue revised generation grant: %v", err)
				}
				if _, err := grant.MarkProbesSettled(ctx, nil); err != nil {
					t.Fatal(err)
				}
				if _, err := grant.AdmitExecution(ctx); err != nil {
					t.Fatal(err)
				}
				binding, err := grant.ProcessExecutionBinding()
				if err != nil {
					t.Fatal(err)
				}
				admitted, err := f.selected.BeginDynamicFlowRuntimeActivation(ctx, runtimepipeline.NewDynamicFlowRuntimeActivationRequest(desired, settled.AttemptOrdinal, settled.AttemptState, binding))
				if err != nil || !admitted.Acknowledged {
					t.Fatalf("admit revised activation: %+v err=%v", admitted, err)
				}
				f.attempt = admitted.Attempt
				successor := load()
				if successor.Phase != runtimepipeline.FlowAttachmentPlanned || successor.AttemptState != "accepted" || successor.AttemptOrdinal != settled.AttemptOrdinal+1 || f.attempt.Ordinal() != successor.AttemptOrdinal {
					t.Fatalf("admission did not mint one fresh planned successor: %+v", successor)
				}
				if committed, err := completeFlowAttachmentFixture(ctx, f.workflow, f.attempt, time.Now().UTC()); err != nil || !committed.Acknowledged {
					t.Fatalf("revised topology: %v", err)
				}
				if oldPublished {
					immutable := load()
					invalid := desired
					creation := *desired.CreationEvent
					creation.EventID = uuid.NewString()
					invalid.CreationEvent = &creation
					if err := reconcile(immutable, invalid); err == nil || !strings.Contains(err.Error(), "cannot revise emitted") {
						t.Fatalf("emitted occurrence replacement: %v", err)
					}
					if got := load(); !reflect.DeepEqual(immutable, got) {
						t.Fatal("emitted-plan refusal changed state")
					}
				} else {
					if err := f.commit(); err == nil {
						t.Fatal("old callback adopted revised readiness")
					}
					bundle := semanticview.Wrap(dynamicFlowCreationAtomicityBundle(t))
					descriptors, err := runtimepkg.AuthorActivityEventDescriptors(bundle)
					if err != nil {
						t.Fatal(err)
					}
					lease, err := f.selected.RegisterAuthorActivityEventCatalog(runtimeauthoractivity.BundleScope("11111111-1111-1111-1111-111111111111", source.BundleHash()), descriptors)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(lease.Release)
					rehydrated, err := newStoreTestEventBus(t, f.selected, runtimebus.EventBusOptions{
						RuntimeInstanceID: "11111111-1111-1111-1111-111111111111",
						ContractBundle:    bundle, SourceArtifactFact: source,
					})
					if err != nil {
						t.Fatal(err)
					}
					f.bus, f.ctx, f.plan = rehydrated, ctx, desired
					f.event, err = dynamicFlowCreationAtomicityEvent(desired)
					if err != nil {
						t.Fatal(err)
					}
					if err := f.commit(); err != nil {
						t.Fatalf("recovered revised publication: %v", err)
					}
					wait(f.bus)
					if err := f.commit(); err != nil {
						t.Fatalf("exact publication duplicate: %v", err)
					}
					wait(f.bus)
				}
				f.assertOccurrenceCounts(t, 1)
				query := `SELECT event_id, payload FROM events WHERE run_id = ? AND event_name = ?`
				if !f.sqlite {
					query = `SELECT event_id::text, payload FROM events WHERE run_id = $1::uuid AND event_name = $2`
				}
				rows, err := f.db.QueryContext(ctx, query, f.runID, desired.CreationEvent.EventType)
				if err != nil {
					t.Fatal(err)
				}
				defer rows.Close()
				count := 0
				for rows.Next() {
					count++
					var id string
					var payload []byte
					if err := rows.Scan(&id, &payload); err != nil {
						t.Fatal(err)
					}
					var got, want any
					if err := json.Unmarshal(payload, &got); err != nil {
						t.Fatal(err)
					}
					if err := json.Unmarshal(desired.CreationEvent.Payload, &want); err != nil {
						t.Fatal(err)
					}
					if id != desired.CreationEvent.EventID || !reflect.DeepEqual(got, want) {
						t.Fatalf("mixed occurrence: id=%s payload=%s wanted=%#v", id, payload, desired.CreationEvent)
					}
				}
				if err := rows.Err(); err != nil {
					t.Fatal(err)
				}
				if count != 1 {
					t.Fatalf("creation publications=%d, want exactly one", count)
				}
			})
		}
	}
}
