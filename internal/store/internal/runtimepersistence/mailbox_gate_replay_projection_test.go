package runtimepersistence

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/tests/support/store/testutil/agentfixture"
	"github.com/google/uuid"
)

// Native selected-owner projection; this is not channel/provider execution.
func TestMailboxGateReplayProjectionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, map[string]string{
				"schema.yaml": "# fixture: native gate replay\nstages:\n  awaiting_review:\n    gate:\n      decision: root_review\n      outcomes:\n        approve:\n          advances_to: done\n          emit: test.node_emitted\n  done: {final: true}\n",
				"events.yaml": "test.node_emitted:\n",
				"state.yaml":  "result: text\n",
			}, nil)
			selected := f.store.(workflowTestSelectedStore)
			persistence := pipeline.NewWorkflowPersistence(f.store.(pipeline.WorkflowPersistenceOwner))
			publisher := f.newRuntimeEventBus(t, bus.EventBusOptions{})
			opts := completeWorkflowTestCoordinatorOptions(persistence, selected)
			opts.Module = runForkGateWorkflowModule{source: semanticview.Wrap(f.bundle)}
			opts.SourceArtifactFact = mustStoreTestSourceArtifactFact(f.bundle.SourceArtifact.BundleHash())
			opts.WorkOwner = storeTestWorkOwner(t)
			opts.DeliveryRuntime = publisher
			coordinator := pipeline.NewPipelineCoordinatorWithOptions(publisher, opts)
			publisher.SetInterceptors(coordinator)
			capability, err := agentfixture.ProcessCapability(t, f.ctx, f.store.(agentfixture.Store))
			if err != nil {
				t.Fatal(err)
			}
			process, present := worklifetime.ProcessFromContext(f.ctx)
			if !present {
				t.Fatal("native replay requires its original work process")
			}
			lease, err := process.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			leaseSettled := false
			t.Cleanup(func() {
				if !leaseSettled {
					if err := lease.Done(); err != nil {
						t.Error(err)
					}
				}
			})
			probeCtx, err := startupownership.WithProcessMutationAuthority(worklifetime.WithProcess(lease.Context(), process), capability)
			if err != nil {
				t.Fatal(err)
			}
			ctx := f.ctx
			observation, err := BindMailboxGateObservationForTest(f.store)
			if err != nil {
				t.Fatal(err)
			}
			for _, invalid := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), struct{}{}} {
				if _, err := BindMailboxGateObservationForTest(invalid); err == nil {
					t.Fatalf("unrelated owner admitted: %T", invalid)
				}
			}
			wait := func() {
				stable := 0
				for deadline := time.Now().Add(60 * time.Second); time.Now().Before(deadline); {
					summary, err := observation.Deliveries(ctx, correlation.RunIDFromContext(ctx))
					if err != nil {
						t.Fatal(err)
					}
					if err := summary.Validate(); err != nil || summary.RunID != correlation.RunIDFromContext(ctx) {
						t.Fatalf("invalid exact summary: %+v %v", summary, err)
					}
					if summary.DeadLetter != 0 {
						t.Fatalf("publication delivery failed: %+v", summary)
					}
					incomplete, err := observation.IncompleteHandoffs(ctx, summary.RunID)
					if err != nil {
						t.Fatal(err)
					}
					if summary.Pending+summary.InProgress == 0 && incomplete == 0 {
						stable++
						if stable == 4 {
							return
						}
					} else {
						stable = 0
					}
					time.Sleep(25 * time.Millisecond)
				}
				t.Fatal("native publication did not settle under the original bound")
			}
			registerTestAuthorActivityCatalogForContext(t, f.store.(testAuthorActivityCatalogRegistrar), ctx)
			runID := correlation.RunIDFromContext(ctx)
			card := nativeMailboxGateCard(t, &f, coordinator, runID)
			other := nativeMailboxGateCard(t, &f, coordinator, uuid.NewString())
			now := time.Now().UTC()
			principal, err := f.store.(interface {
				EnsureOperatorPrincipal(context.Context, time.Time) (operatorchannel.Principal, error)
			}).EnsureOperatorPrincipal(ctx, now)
			if err != nil {
				t.Fatal(err)
			}
			key := uuid.NewString()
			params := map[string]any{"card_id": card.CardID, "verdict": "approve", "observed_content_hash": card.CardContentHash, "idempotency_key": key}
			raw, err := canonicaljson.Bytes(map[string]any{"method": "mailbox.decide", "params": params})
			if err != nil {
				t.Fatal(err)
			}
			req := apiidempotency.Request{Method: "mailbox.decide", Actor: apiidempotency.PrincipalActor(principal.ID), ResourceID: card.CardID, RequestHash: fmt.Sprintf("sha256:%x", sha256.Sum256(raw)), IdempotencyKey: key, Now: now, TTL: 24 * time.Hour}
			mutationFor := func(request apiidempotency.Request) pipeline.DecisionCardMutation {
				return pipeline.NewDecisionCardDecision(decisioncard.DecideRequest{CardID: request.ResourceID, PrincipalID: request.Actor.ID, Verdict: "approve", ObservedContentHash: card.CardContentHash, Fields: semanticvalue.EmptyObject(), DecisionEventID: uuid.NewString(), Now: request.Now})
			}
			response, replayed, err := coordinator.CommitDecisionCardMutation(ctx, req, mutationFor(req))
			if err != nil || replayed || len(response) == 0 {
				t.Fatalf("genuine stage-gate answer: replay=%t response=%s err=%v", replayed, response, err)
			}
			wait()
			domain := MailboxGateDomain{Method: req.Method, Actor: req.Actor.ID, Key: req.IdempotencyKey, Hash: req.RequestHash}
			before, err := observation.Observe(ctx, runID, card.CardID, domain)
			if err != nil {
				t.Fatal(err)
			}
			var stored []byte
			matches := 0
			for _, row := range before.API {
				if row.Method == req.Method && row.Actor == req.Actor.ID && row.Key == req.IdempotencyKey && row.ResourceID == req.ResourceID {
					matches++
					stored = []byte(row.Response)
				}
			}
			if len(before.API) != 1 || matches != 1 || !bytes.Equal(response, stored) {
				t.Fatalf("actual singular completion: matches=%d response=%s stored=%s", matches, response, stored)
			}
			probe := func(ctx context.Context, request apiidempotency.Request) (json.RawMessage, bool, error) {
				return persistence.ReplayDecisionCardMutation(ctx, request, mutationFor(request))
			}
			got, replayed, err := probe(probeCtx, req)
			if err != nil || !replayed || !bytes.Equal(got, stored) {
				t.Fatalf("selected-owner replay: replay=%t response=%s err=%v", replayed, got, err)
			}
			for _, tc := range []struct {
				name     string
				change   func(*apiidempotency.Request)
				conflict bool
			}{
				{"hash", func(r *apiidempotency.Request) { r.RequestHash = "different-semantic-body" }, true},
				{"resource", func(r *apiidempotency.Request) { r.ResourceID = other.CardID }, true},
				{"actor", func(r *apiidempotency.Request) { r.Actor = apiidempotency.PrincipalActor(uuid.NewString()) }, false},
				{"bearer", func(r *apiidempotency.Request) { r.Actor = apiidempotency.BearerActor(principal.ID) }, false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					bad := req
					tc.change(&bad)
					got, replayed, err := probe(probeCtx, bad)
					if err == nil || replayed || len(got) != 0 || tc.conflict && !errors.Is(err, apiidempotency.ErrConflict) {
						t.Fatalf("domain refusal: replay=%t response=%s err=%v", replayed, got, err)
					}
				})
			}
			badMethod := req
			badMethod.Method = "mailbox.defer"
			if got, replayed, err := persistence.ReplayDecisionCardMutation(probeCtx, badMethod, mutationFor(req)); err == nil || replayed || len(got) != 0 {
				t.Fatal("method did not remain bound to actual mutation")
			}
			canceled, cancel := context.WithCancel(probeCtx)
			cancel()
			foreign := mustStoreTestSourceArtifactFact("bundle-v2:sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
			for _, badCtx := range []context.Context{context.Background(), canceled, correlation.WithSourceArtifactFact(probeCtx, foreign), correlation.WithRootAdmission(probeCtx, sourceartifact.RootAdmission{})} {
				if got, replayed, err := probe(badCtx, req); err == nil || replayed || len(got) != 0 {
					t.Fatal("missing, foreign or canceled authority returned completion")
				}
			}
			fresh := req
			fresh.IdempotencyKey = uuid.NewString()
			if got, replayed, err := probe(probeCtx, fresh); err != nil || replayed || len(got) != 0 {
				t.Fatalf("fresh key must miss: replay=%t response=%s err=%v", replayed, got, err)
			}
			const absent = "bundle-v2:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
			hash := f.bundle.SourceArtifact.BundleHash()
			if err := SwapMailboxRunSourceForTest(ctx, f.store, runID, hash, absent); err != nil {
				t.Fatal(err)
			}
			got, replayed, missingErr := probe(probeCtx, req)
			restoreErr := SwapMailboxRunSourceForTest(ctx, f.store, runID, absent, hash)
			if restoreErr != nil || missingErr == nil || replayed || len(got) != 0 {
				t.Fatalf("actual replay missing-source refusal: read=%v restore=%v replay=%t", missingErr, restoreErr, replayed)
			}
			var originalBlob []byte
			if err := f.db.QueryRowContext(ctx, `SELECT source_blob FROM source_artifacts WHERE bundle_hash=$1`, hash).Scan(&originalBlob); err != nil {
				t.Fatal(err)
			}
			restored := false
			restoreBlob := func() error {
				restoreCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
				defer cancel()
				result, err := f.db.ExecContext(restoreCtx, `UPDATE source_artifacts SET source_blob=$1 WHERE bundle_hash=$2`, originalBlob, hash)
				if err != nil {
					return err
				}
				n, err := result.RowsAffected()
				if err != nil {
					return err
				}
				if n != 1 {
					return fmt.Errorf("restore rows=%d", n)
				}
				var actual []byte
				if err := f.db.QueryRowContext(restoreCtx, `SELECT source_blob FROM source_artifacts WHERE bundle_hash=$1`, hash).Scan(&actual); err != nil {
					return err
				}
				if !bytes.Equal(actual, originalBlob) {
					return fmt.Errorf("exact source bytes did not restore")
				}
				restored = true
				return nil
			}
			t.Cleanup(func() {
				if !restored {
					if err := restoreBlob(); err != nil {
						t.Error(err)
					}
				}
			})
			if result, err := f.db.ExecContext(ctx, `UPDATE source_artifacts SET source_blob=$1 WHERE bundle_hash=$2`, []byte("not an artifact"), hash); err != nil {
				t.Fatal(err)
			} else if n, err := result.RowsAffected(); err != nil || n != 1 {
				t.Fatalf("corrupt rows=%d err=%v", n, err)
			}
			got, replayed, corruptErr := probe(probeCtx, req)
			if restoreErr := restoreBlob(); restoreErr != nil || corruptErr == nil || replayed || len(got) != 0 {
				t.Fatalf("actual replay corrupt-source refusal: read=%v restore=%v replay=%t", corruptErr, restoreErr, replayed)
			}
			if after, err := observation.Observe(ctx, runID, card.CardID, domain); err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("replay/refusals changed same-operation footprint: before=%v after=%v", before, after)
			}
			expired := req
			expired.Now = req.Now.Add(25 * time.Hour)
			footprint := func() []string {
				wait()
				var out []string
				for _, query := range []string{
					`SELECT * FROM decision_cards WHERE run_id=$1 ORDER BY card_id`,
					`SELECT d.* FROM decision_card_input_drafts d JOIN decision_cards c ON c.card_id=d.card_id WHERE c.run_id=$1 ORDER BY d.input_draft_id`,
					`SELECT d.* FROM decision_card_changes d JOIN decision_cards c ON c.card_id=d.card_id WHERE c.run_id=$1 ORDER BY d.change_id`,
					`SELECT * FROM human_task_continuations WHERE run_id=$1 ORDER BY card_id`,
					`SELECT * FROM proposed_effect_continuations WHERE run_id=$1 ORDER BY card_id`,
					`SELECT * FROM events WHERE run_id=$1 AND event_class NOT IN ('runtime_diagnostic','diagnostic_direct') ORDER BY event_id`,
					`SELECT d.* FROM event_deliveries d JOIN events e ON e.event_id=d.event_id WHERE e.run_id=$1 ORDER BY d.delivery_id`,
					`SELECT * FROM entity_state WHERE run_id=$1 ORDER BY entity_id,flow_instance`,
					`SELECT * FROM flow_instances WHERE run_id=$1 ORDER BY instance_path`,
				} {
					rows, err := f.db.QueryContext(ctx, query, runID)
					if err != nil {
						t.Fatal(err)
					}
					columns, err := rows.Columns()
					if err != nil {
						rows.Close()
						t.Fatal(err)
					}
					for rows.Next() {
						values := make([]any, len(columns))
						pointers := make([]any, len(columns))
						for i := range values {
							pointers[i] = &values[i]
						}
						if err := rows.Scan(pointers...); err != nil {
							rows.Close()
							t.Fatal(err)
						}
						raw, err := json.Marshal(values)
						if err != nil {
							rows.Close()
							t.Fatal(err)
						}
						out = append(out, query+":"+string(raw))
					}
					if err := errors.Join(rows.Err(), rows.Close()); err != nil {
						t.Fatal(err)
					}
				}
				return out
			}
			beforeEffects := footprint()
			if got, replayed, err := probe(probeCtx, expired); err != nil || replayed || len(got) != 0 {
				t.Fatalf("actual expired replay must miss: replay=%t response=%s err=%v", replayed, got, err)
			}
			if after := footprint(); !reflect.DeepEqual(beforeEffects, after) {
				t.Fatal("TTL cleanup mutated domain footprint")
			}
			var count int
			if err := f.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM api_idempotency WHERE method=$1 AND actor_kind=$2 AND actor_id=$3 AND idempotency_key=$4`, req.Method, req.Actor.Kind, req.Actor.ID, req.IdempotencyKey).Scan(&count); err != nil || count != 0 {
				t.Fatalf("exact expired domain remains: count=%d err=%v", count, err)
			}
			if err := f.manager.Shutdown(); err != nil {
				t.Fatal(err)
			}
			if err := lease.Done(); err != nil {
				t.Fatal(err)
			}
			leaseSettled = true
			retireStoreTestWorkFixture(t)
			if err := capability.Release(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := capability.ProveCurrent(context.Background()); err == nil {
				t.Fatal("original capability remains current")
			}
			if got, replayed, err := probe(probeCtx, req); err == nil || replayed || len(got) != 0 {
				t.Fatal("retired original receiver returned completion bytes")
			}
		})
	}
}

func nativeMailboxGateCard(t *testing.T, f *receiverConfigActivationFixture, pc *pipeline.PipelineCoordinator, runID string) decisioncard.Card {
	t.Helper()
	ctx := correlation.WithRunID(f.ctx, runID)
	rooted, err := f.bundle.RootedSource()
	if err != nil {
		t.Fatal(err)
	}
	ctx = requireRunFixtureForTest(t, ctx, f.store, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID, Artifact: f.bundle.SourceArtifact, BundleHash: f.bundle.SourceArtifact.BundleHash(), RootAdmission: rooted.Admission()})
	source := semanticview.Wrap(f.bundle)
	identity := flowidentity.Stored(source, ".", runID, runID, runID, "")
	plan, err := f.manager.PrepareFlowInstanceActivation(ctx, pipeline.FlowInstanceActivationRequest{ContractBundle: source, Instance: identity, OccurredAt: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	committed, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(ctx, plan)
	if err != nil || !committed.Acknowledged || !committed.Created {
		t.Fatalf("genuine root gate construction: %+v err=%v", committed, err)
	}
	if err := f.manager.FinalizeCommittedFlowInstanceActivation(ctx, committed); err != nil {
		t.Fatal(err)
	}
	instance, found, err := pc.Load(ctx, flowidentity.RunScopedFlowInstance{RunID: runID, Route: identity.Route()})
	if err != nil || !found {
		t.Fatalf("gate instance found=%t err=%v", found, err)
	}
	carrier, err := engine.StateCarrierFromPersisted(instance.Fields, instance.Bookkeeping, instance.StateBuckets)
	if err != nil {
		t.Fatal(err)
	}
	activations, err := gateruntime.List(carrier.StateBuckets)
	if err != nil || len(activations) != 1 {
		t.Fatalf("genuine gate activations=%+v err=%v", activations, err)
	}
	card, err := f.store.(workflowTestSelectedStore).GetDecisionCard(ctx, activations[0].CardID)
	if err != nil || card.Anchor.Kind() != decisioncard.AnchorKindStageGate || card.RunID != runID {
		t.Fatalf("genuine pending gate=%+v err=%v", card, err)
	}
	return card
}
