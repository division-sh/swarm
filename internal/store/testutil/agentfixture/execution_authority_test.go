package agentfixture_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentintent"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

type executionFixture struct {
	ctx   context.Context
	store agentfixture.Store
	fact  correlation.SourceArtifactFact
	owner startupownership.ProcessCapability
	plan  agenttopology.SourceSetPlan
	grant startupownership.LiveGenerationGrant
}

func executionFixtureStore(t *testing.T, backend string) agentfixture.Store {
	t.Helper()
	if backend == "postgres" {
		return storetest.StartPostgresRuntimeStore(t)
	}
	return storetest.StartSQLiteRuntimeStore(t)
}

func prepareExecutionFixture(t *testing.T, backend string) executionFixture {
	t.Helper()
	selected := executionFixtureStore(t, backend)
	fact := sourceartifactfixture.Require(t, t.Context(), selected)
	runtimeID, runID := uuid.NewString(), uuid.NewString()
	ctx := correlation.WithRuntimeInstanceID(correlation.WithRunID(correlation.WithSourceArtifactFact(t.Context(), fact), runID), runtimeID)
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(runtimeID, fact.BundleHash()))
	name, err := agentidentity.DeclaredName("admitted-worker", "execution-authority-proof")
	if err != nil {
		t.Fatal(err)
	}
	route, err := agentidentity.PresentRoute("global", "global", "global")
	if err != nil {
		t.Fatal(err)
	}
	identity, err := agentidentity.New(runID, name, route)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := agentintent.Resolve(agentintent.SourceInline, "inline", "agents.yaml#agents.admitted-worker.intent", "Preserve the prepared execution fixture.")
	if err != nil {
		t.Fatal(err)
	}
	prompt, err := agentintent.IntentOnlyPrompt(intent)
	if err != nil {
		t.Fatal(err)
	}
	record := manager.PersistedAgent{Config: actors.AgentConfig{ID: identity.AgentID(), Identity: identity,
		Role: "worker", Type: "sonnet", Model: "regular", ResolvedLLMBackend: "anthropic",
		FlowID: "global", FlowPath: "global", ExecutionMode: effects.ExecutionModeLive,
		Intent: intent, Prompt: prompt, Config: []byte(`{}`)},
		Status: "active", HiredBy: "execution-authority-proof", StartedAt: time.Now().UTC()}
	if err := agentfixture.UpsertStaticForSource(t, ctx, selected, record, fact); err != nil {
		t.Fatal(err)
	}
	owner, err := agentfixture.ProcessCapability(t, ctx, selected)
	if err != nil {
		t.Fatal(err)
	}
	plan, found, err := owner.CurrentSourceSet(ctx)
	if err != nil || !found || len(plan.Agents) != 1 {
		t.Fatalf("prepared exact actor/source plan: found=%t plan=%+v err=%v", found, plan, err)
	}
	grant, err := agentfixture.AdmitGeneration(t, ctx, selected, plan, agenttopology.SourceCoordinate{BundleHash: fact.BundleHash()})
	if err != nil {
		t.Fatal(err)
	}
	return executionFixture{ctx: ctx, store: selected, fact: fact, owner: owner, plan: plan, grant: grant}
}

func executionSnapshot(t *testing.T, f executionFixture) map[string]private.SelectedForkStorageTableSnapshot {
	t.Helper()
	snapshot, err := private.ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func TestAdmitExecutionAuthorityUsesIssuedEvidenceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, state := range []string{"prepared", "probe_settled", "admitted"} {
			t.Run(backend+"/"+state, func(t *testing.T) {
				f := prepareExecutionFixture(t, backend)
				if state != "prepared" {
					if _, err := f.grant.MarkProbesSettled(f.ctx, nil); err != nil {
						t.Fatal(err)
					}
				}
				if state == "admitted" {
					if _, err := f.grant.AdmitExecution(f.ctx); err != nil {
						t.Fatal(err)
					}
				}
				issued, err := f.grant.Evidence()
				if err != nil {
					t.Fatal(err)
				}
				agents, err := f.store.LoadAgents(f.ctx)
				if err != nil || len(agents) != 1 {
					t.Fatalf("original prepared actor: agents=%+v err=%v", agents, err)
				}
				authority, err := agentfixture.AdmitExecutionAuthority(t, f.ctx, f.store, f.fact)
				if err != nil || authority.Validate() != nil {
					t.Fatalf("admit prepared fixture: %+v err=%v", authority, err)
				}
				admitted, err := f.grant.Evidence()
				if err != nil || admitted.State != startupownership.GrantAdmitted || admitted.GrantID != issued.GrantID ||
					admitted.RuntimeGeneration != issued.RuntimeGeneration || !reflect.DeepEqual(admitted.ProbeSurfaceIDs, issued.ProbeSurfaceIDs) {
					t.Fatalf("helper replaced evidence or erased settled probes: before=%+v after=%+v err=%v", issued, admitted, err)
				}
				if authority.Kind() != deliverylifecycle.ExecutionAuthorityNormalRuntime || authority.SourceArtifact() != f.fact ||
					authority.ExecutionID() != admitted.GrantID || authority.Generation() != admitted.RuntimeGeneration {
					t.Fatalf("authority did not consume issued admission: %+v evidence=%+v", authority, admitted)
				}
				if err := f.grant.ProveCurrent(f.ctx); err != nil {
					t.Fatal(err)
				}
				ownership, err := f.grant.InspectRunExecutionOwnership(f.ctx, correlation.RunIDFromContext(f.ctx))
				if err != nil || ownership != manager.RunExecutionOwned {
					t.Fatalf("issued grant does not own prepared run: ownership=%v err=%v", ownership, err)
				}
				retained, err := agentfixture.ProcessCapability(t, f.ctx, f.store)
				if err != nil || retained != f.owner {
					t.Fatalf("helper replaced retained process: same=%t err=%v", retained == f.owner, err)
				}
				current, found, err := retained.CurrentSourceSet(f.ctx)
				if err != nil || !found || !reflect.DeepEqual(current, f.plan) {
					t.Fatalf("helper changed complete source plan: %+v found=%t err=%v", current, found, err)
				}
				currentAgents, err := f.store.LoadAgents(f.ctx)
				if err != nil || !reflect.DeepEqual(currentAgents, agents) {
					t.Fatalf("helper changed actor/config/binding: %+v err=%v", currentAgents, err)
				}
				before := executionSnapshot(t, f)
				again, err := agentfixture.AdmitExecutionAuthority(t, f.ctx, f.store, f.fact)
				last, evidenceErr := f.grant.Evidence()
				if err != nil || evidenceErr != nil || !again.Equal(authority) || !reflect.DeepEqual(admitted, last) || !reflect.DeepEqual(before, executionSnapshot(t, f)) {
					t.Fatalf("repeat call reissued or mutated admission: authority=%+v evidence=%+v err=%v evidenceErr=%v", again, last, err, evidenceErr)
				}
			})
		}
	}
}

func TestAdmitExecutionAuthorityRefusesUnpreparedOrForeignBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cut := range []string{"missing_source_plan", "foreign_coordinate", "foreign_caller_source", "foreign_store", "retired_grant", "released_process", "successor_process", "stale_plan"} {
			t.Run(backend+"/"+cut, func(t *testing.T) {
				if cut == "missing_source_plan" {
					selected := executionFixtureStore(t, backend)
					fact := sourceartifactfixture.Require(t, t.Context(), selected)
					ctx := correlation.WithSourceArtifactFact(t.Context(), fact)
					owner, err := agentfixture.ProcessCapability(t, ctx, selected)
					if err != nil {
						t.Fatal(err)
					}
					f := executionFixture{ctx: ctx, store: selected}
					before := executionSnapshot(t, f)
					authority, err := agentfixture.AdmitExecutionAuthority(t, ctx, selected, fact)
					_, found, readErr := owner.CurrentSourceSet(ctx)
					if err == nil || authority.Validate() == nil || readErr != nil || found || !reflect.DeepEqual(before, executionSnapshot(t, f)) {
						t.Fatalf("missing plan became execution setup: authority=%+v found=%t err=%v readErr=%v", authority, found, err, readErr)
					}
					return
				}
				f := prepareExecutionFixture(t, backend)
				ctx, target, fact := f.ctx, f.store, f.fact
				var other *executionFixture
				var successor startupownership.ProcessCapability
				switch cut {
				case "foreign_coordinate", "foreign_caller_source":
					fact = sourceartifactfixture.FactFor(sourceartifactfixture.New("global/schema.yaml", []byte("name: foreign\n")))
					if cut == "foreign_coordinate" {
						ctx = correlation.WithSourceArtifactFact(ctx, fact)
					}
				case "foreign_store":
					next := prepareExecutionFixture(t, backend)
					other, target = &next, next.store
				case "retired_grant":
					if err := f.grant.Retire(ctx); err != nil {
						t.Fatal(err)
					}
				case "released_process", "successor_process":
					if err := f.owner.Release(ctx); err != nil {
						t.Fatal(err)
					}
					if cut == "successor_process" {
						var err error
						successor, err = target.AcquireProcessCapability(ctx, startupownership.AcquireRequest{
							OwnerID: "fixture-successor", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString(),
						})
						if err != nil {
							t.Fatal(err)
						}
						t.Cleanup(func() { _ = successor.Release(context.Background()) })
					}
				case "stale_plan":
					added := sourceartifactfixture.RequireArtifact(t, ctx, target, sourceartifactfixture.New("global/schema.yaml", []byte("name: next\n")))
					plan, err := agenttopology.NewSourceSetPlan(append(append([]agenttopology.SourceCoordinate(nil), f.plan.Sources...), agenttopology.SourceCoordinate{BundleHash: added.BundleHash()}), f.plan.Agents)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := f.owner.RestoreSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), ExpectedRevision: f.plan.Revision, Plan: plan}); err != nil {
						t.Fatal(err)
					}
				}
				before := executionSnapshot(t, f)
				var foreignBefore map[string]private.SelectedForkStorageTableSnapshot
				if other != nil {
					foreignBefore = executionSnapshot(t, *other)
				}
				authority, err := agentfixture.AdmitExecutionAuthority(t, ctx, target, fact)
				if err == nil || authority.Validate() == nil || !reflect.DeepEqual(before, executionSnapshot(t, f)) {
					t.Fatalf("invalid fixture gained execution or changed storage: authority=%+v err=%v", authority, err)
				}
				if other != nil && !reflect.DeepEqual(foreignBefore, executionSnapshot(t, *other)) {
					t.Fatal("foreign store was mutated by donated caller coordinates")
				}
				if successor != nil {
					if err := successor.ProveCurrent(ctx); err != nil {
						t.Fatalf("old fixture admission disturbed successor possession: %v", err)
					}
				}
			})
		}
	}
}
