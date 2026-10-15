package runtimepersistence

import (
	"context"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/google/uuid"
)

func TestSelectedAgentClaimReadsOriginalPreparationWithoutOrdinaryCaptureBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx, selected := newAgentFixtureAuthorityStore(t, backend)
			ordinary := newAgentProfileMembershipActor(t, admitAgentClaimProfileFixture(t, ctx, selected))
			process, err := agentfixture.ProcessCapability(t, ordinary.ctx, selected)
			if err != nil {
				t.Fatal(err)
			}
			owner := selected.(selectedCompletionAuthorityStore)
			f := newSelectedProviderCompletionFixtureWithProcess(t, owner, receiverClaimTestDB(t, selected), backend == "sqlite", process)
			ctx = correlation.WithRunID(testAuthorActivityContextForBundle(f.request.DeclarationPlan.BundleHash), f.forkRun)
			issued, err := owner.IssueRunForkSelectedContractRuntimeExecution(ctx, f.request)
			if err != nil {
				t.Fatal(err)
			}
			execution, err := owner.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, "s6-profile-evidence", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			admitSelectedProviderFixture(t, ctx, f, issued, execution)
			identity := f.providerActor.Config.Identity
			authority, err := deliverylifecycle.NewSelectedExecutionAuthority(mustStoreTestSourceArtifactFact(f.request.DeclarationPlan.BundleHash), execution.ID, identity.RunID, execution.SelectedFork.Generation)
			if err != nil {
				t.Fatal(err)
			}
			event := eventtest.ExistingRunRootIngress(uuid.NewString(), "test.selected_profile", "operator", "", []byte(`{}`), 0, identity.RunID, events.EventEnvelope{}, time.Now().UTC())
			route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(identity.AgentID()), AgentIdentity: identity}
			commitSelectedReceiverClaimEvent(t, ctx, selected, event, route, authority)
			result, err := selected.ClaimDelivery(ctx, authority, event, route)
			claimed, acquired := result.Acquired()
			if err != nil || !acquired {
				t.Fatalf("native selected first claim: %+v err=%v", result, err)
			}
			reader, ok := selected.(interface {
				ReadSelectedAgentPreparation(context.Context, deliverylifecycle.Snapshot) (runfork.SelectedForkPreparedActor, error)
			})
			if !ok {
				t.Fatal("canonical selected preparation profile reader is missing")
			}
			before, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
			if err != nil {
				t.Fatal(err)
			}
			attempts, err := ReadExternalAttemptStorageForTest(ctx, selected)
			if err != nil || len(attempts) != 0 {
				t.Fatalf("native pre-attempt cut has external attempts: rows=%d err=%v", len(attempts), err)
			}
			actor, err := reader.ReadSelectedAgentPreparation(ctx, claimed.Snapshot)
			plan, planErr := identity.Plan()
			if err != nil || planErr != nil || actor.Plan != plan || actor.Backend != selection.BackendAnthropic || actor != f.request.Preparation.Actors[0] {
				t.Fatalf("original prepared profile: %+v err=%v planErr=%v", actor, err, planErr)
			}
			state, found, err := selected.LoadAgentLifecycleState(ctx, identity)
			if err != nil || !found || state.ConfigRevision != actor.ConfigurationRevision {
				t.Fatalf("prepared configuration differs from native lifecycle: %+v found=%v err=%v", state, found, err)
			}
			rows, err := DecodeSelectedForkSnapshotRowsForTest(before["event_delivery_attempts"])
			if err != nil {
				t.Fatal(err)
			}
			matched := 0
			for _, row := range rows {
				id, err := row["delivery_id"].Text()
				if err != nil {
					t.Fatal(err)
				}
				if id == claimed.Snapshot.DeliveryID {
					matched++
					if cell := row["agent_claim_profile"]; cell.Type != "<nil>" || string(cell.Value) != "null" {
						t.Fatalf("selected claim duplicated ordinary profile evidence: %+v", cell)
					}
				}
			}
			if matched != 1 {
				t.Fatalf("native selected attempt rows=%d, want 1", matched)
			}
			if _, err := ReadFirstAgentClaimProfileForTest(ctx, selected, claimed.Snapshot); err == nil {
				t.Fatal("selected history gained ordinary first-claim evidence")
			}
			for _, name := range []string{"missing_row", "event", "run", "route", "source", "generation", "execution", "fork", "normal", "unclaimed"} {
				t.Run(name, func(t *testing.T) {
					bad := claimed.Snapshot
					switch name {
					case "missing_row":
						bad.DeliveryID = uuid.NewString()
					case "event":
						bad.EventID = uuid.NewString()
					case "run":
						bad.RunID = uuid.NewString()
					case "route":
						bad.Route.Target = events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: "global", FlowInstance: "sibling"})
					case "source", "generation", "execution", "fork", "normal":
						source, generation, executionID, forkID := authority.SourceArtifact(), authority.Generation(), authority.ExecutionID(), authority.ForkRunID()
						switch name {
						case "source":
							source = mustStoreTestSourceArtifactFact("bundle-v2:sha256:" + strings.Repeat("a", 64))
							if source.Matches(authority.SourceArtifact()) {
								t.Fatal("foreign source control is not independent")
							}
						case "generation":
							generation++
						case "execution":
							executionID = uuid.NewString()
						case "fork":
							forkID = uuid.NewString()
						}
						var err error
						if name == "normal" {
							bad.Authority, err = deliverylifecycle.NewNormalExecutionAuthority(source, executionID, generation)
						} else {
							bad.Authority, err = deliverylifecycle.NewSelectedExecutionAuthority(source, executionID, forkID, generation)
						}
						if err != nil {
							t.Fatal(err)
						}
					case "unclaimed":
						bad.ClaimVersion = 0
					}
					if returned, err := reader.ReadSelectedAgentPreparation(ctx, bad); err == nil || returned != (runfork.SelectedForkPreparedActor{}) {
						t.Fatalf("inexact selected preparation escaped: %+v err=%v", returned, err)
					}
				})
			}
			after, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("prepared profile observation mutated native state: err=%v", err)
			}
			_, foreignOwner := newAgentFixtureAuthorityStore(t, backend)
			for _, fault := range []SelectedAgentPreparationFault{SelectedAgentPreparationMissing, SelectedAgentPreparationCorrupt, SelectedAgentPreparationForeign} {
				t.Run("fault_"+string(fault), func(t *testing.T) {
					proveSelectedAgentPreparationFault(t, ctx, selected, foreignOwner, claimed.Snapshot, reader.ReadSelectedAgentPreparation, fault)
				})
			}
			proveMixedAgentProfileMembershipWitness(t, ctx, ordinary, f, claimed, foreignOwner)
			terminal, err := selected.Snapshot(ctx, claimed.Snapshot.DeliveryID)
			if err != nil {
				t.Fatal(err)
			}
			proveSelectedProfileFencedMembership(t, ctx, ordinary, f, execution, terminal, foreignOwner)
			beforeRetained, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
			if err != nil {
				t.Fatal(err)
			}
			retained, err := reader.ReadSelectedAgentPreparation(ctx, terminal)
			if err != nil || !reflect.DeepEqual(actor, retained) {
				t.Fatalf("retirement replaced immutable preparation: %+v err=%v", retained, err)
			}
			if retry, err := selected.ClaimDelivery(ctx, authority, event, route); err != nil || retry.Disposition != deliverylifecycle.ClaimWrongAuthority {
				t.Fatalf("historical profile reopened selected work: %+v err=%v", retry, err)
			}
			afterRetained, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
			if err != nil || !reflect.DeepEqual(beforeRetained, afterRetained) {
				t.Fatalf("retained observation/refusal mutated native state: err=%v", err)
			}
			proveMixedOwnedRecoveryImage(t, ordinary, f, foreignOwner)
			proveAgentRecoveryNonAgentAndTerminalExclusion(t, ordinary)
			proveAgentRecoveryExactNativeGenerationJoin(t, ordinary)
		})
	}
}

func TestSelectedAgentPreparationCoherentMembershipRefusalBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, mismatch := range []string{"foreign_actor", "configuration_revision"} {
			t.Run(backend+"/"+mismatch, func(t *testing.T) {
				ctx, selected := newAgentFixtureAuthorityStore(t, backend)
				owner := selected.(selectedCompletionAuthorityStore)
				f := newSelectedProviderCompletionFixture(t, owner, receiverClaimTestDB(t, selected), backend == "sqlite")
				if mismatch == "foreign_actor" {
					f.request.Preparation.Actors[0].Plan.Name.AgentID = "s6-foreign-prepared-actor"
				} else {
					f.request.Preparation.Actors[0].ConfigurationRevision = strings.Repeat("a", 64)
					if f.request.Preparation.Actors[0].ConfigurationRevision == f.request.DeclarationPlan.Agents[0].ConfigRevision {
						t.Fatal("configuration revision control is not independent")
					}
				}
				ctx = correlation.WithRunID(testAuthorActivityContextForBundle(f.request.DeclarationPlan.BundleHash), f.forkRun)
				issued, err := owner.IssueRunForkSelectedContractRuntimeExecution(ctx, f.request)
				if err != nil {
					t.Fatalf("native coherent preparation issuance: %v", err)
				}
				execution, err := owner.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, "s6-coherent-membership", time.Minute)
				if err != nil {
					t.Fatal(err)
				}
				admitSelectedProviderFixture(t, ctx, f, issued, execution)
				identity := f.providerActor.Config.Identity
				authority, err := deliverylifecycle.NewSelectedExecutionAuthority(mustStoreTestSourceArtifactFact(f.request.DeclarationPlan.BundleHash), execution.ID, f.forkRun, execution.SelectedFork.Generation)
				if err != nil {
					t.Fatal(err)
				}
				event := eventtest.ExistingRunRootIngress(uuid.NewString(), "test.selected_profile", "operator", "", []byte(`{}`), 0, f.forkRun, events.EventEnvelope{}, time.Now().UTC())
				route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(identity.AgentID()), AgentIdentity: identity}
				commitSelectedReceiverClaimEvent(t, ctx, selected, event, route, authority)
				result, err := selected.ClaimDelivery(ctx, authority, event, route)
				claimed, acquired := result.Acquired()
				if err != nil || !acquired {
					t.Fatalf("native first claim did not reach membership reader: %+v err=%v", result, err)
				}
				before, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
				if err != nil {
					t.Fatal(err)
				}
				reader := selected.(interface {
					ReadSelectedAgentPreparation(context.Context, deliverylifecycle.Snapshot) (runfork.SelectedForkPreparedActor, error)
				})
				actor, err := reader.ReadSelectedAgentPreparation(ctx, claimed.Snapshot)
				want := "selected agent is absent from immutable preparation"
				if mismatch == "configuration_revision" {
					want = "selected prepared actor differs from immutable declaration"
				}
				if err == nil || err.Error() != want || actor != (runfork.SelectedForkPreparedActor{}) {
					t.Fatalf("coherent mismatch did not reach exact membership gate: %+v err=%v want=%s", actor, err, want)
				}
				after, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("coherent membership refusal mutated storage: %v", err)
				}
			})
		}
	}
}

func proveSelectedAgentPreparationFault(t *testing.T, ctx context.Context, selected, foreignOwner agentFixtureFlowStore, snapshot deliverylifecycle.Snapshot, read func(context.Context, deliverylifecycle.Snapshot) (runfork.SelectedForkPreparedActor, error), fault SelectedAgentPreparationFault) {
	t.Helper()
	storage := func() map[string]SelectedForkStorageTableSnapshot {
		t.Helper()
		value, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	before := storage()
	if image, err := inspectNativeAgentRecoveryImage(t, agentClaimProfileFixture{ctx: ctx, store: selected}); err != nil || len(image.Members) != 1 {
		t.Fatalf("healthy selected preparation image: %+v err=%v", image, err)
	}
	foreignBefore, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, foreignOwner)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"canceled_install", "foreign_install", "stale_generation", "unnamed_variant"} {
		t.Run(name, func(t *testing.T) {
			cutCtx, destination, bad, variant := ctx, selected, snapshot, fault
			switch name {
			case "canceled_install":
				var cancel context.CancelFunc
				cutCtx, cancel = context.WithCancel(ctx)
				cancel()
			case "foreign_install":
				destination = foreignOwner
			case "stale_generation":
				bad.Authority, err = deliverylifecycle.NewSelectedExecutionAuthority(snapshot.Authority.SourceArtifact(), snapshot.Authority.ExecutionID(), snapshot.Authority.ForkRunID(), snapshot.Authority.Generation()+1)
				if err != nil {
					t.Fatal(err)
				}
			case "unnamed_variant":
				variant = "not-an-authorized-fault"
			}
			if refused, err := ApplySelectedAgentPreparationFaultForTest(cutCtx, destination, bad, variant); err == nil || refused != nil {
				t.Fatalf("refused fault escaped: %v err=%v", refused, err)
			}
			if !reflect.DeepEqual(before, storage()) {
				t.Fatal("refused install changed complete native storage")
			}
		})
	}
	receipt, err := ApplySelectedAgentPreparationFaultForTest(ctx, selected, snapshot, fault)
	if err != nil || receipt == nil {
		t.Fatalf("install exact selected preparation fault: receipt=%v err=%v", receipt, err)
	}
	restored := false
	t.Cleanup(func() {
		if !restored {
			if err := RestoreSelectedAgentPreparationFaultForTest(ctx, selected, receipt); err != nil {
				t.Errorf("join selected preparation fault restoration: %v", err)
			}
		}
	})
	injected := storage()
	delta := make(map[string]SelectedForkStorageTableSnapshot, len(before))
	for table, evidence := range before {
		evidence.Rows = slices.Clone(evidence.Rows)
		delta[table] = evidence
	}
	assertForkReceiverFaultOnlyColumns(t, delta, injected, []forkReceiverFaultEdit{{
		table: "run_fork_selected_contract_runtime_executions", key: "execution_id", value: snapshot.Authority.ExecutionID(), columns: []string{"preparation_binding"},
	}})
	// The unchanged fingerprint makes this an integrity/shape refusal. It is
	// deliberately not credited as a downstream actor-membership negative.
	if actor, err := read(ctx, snapshot); err == nil || actor != (runfork.SelectedForkPreparedActor{}) {
		t.Fatalf("corrupt selected preparation escaped: %+v err=%v", actor, err)
	}
	assertNativeAgentRecoveryImageRefusal(t, agentClaimProfileFixture{ctx: ctx, store: selected})
	if next, err := ApplySelectedAgentPreparationFaultForTest(ctx, selected, snapshot, fault); err == nil || next != nil {
		t.Fatalf("changed original evidence accepted another fault: %v err=%v", next, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := RestoreSelectedAgentPreparationFaultForTest(canceled, selected, receipt); err == nil {
		t.Fatal("cancelled restoration mutated selected preparation")
	}
	if err := RestoreSelectedAgentPreparationFaultForTest(ctx, foreignOwner, receipt); err == nil {
		t.Fatal("foreign native owner accepted restoration")
	}
	foreignAfter, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, foreignOwner)
	if err != nil || !reflect.DeepEqual(foreignBefore, foreignAfter) || !reflect.DeepEqual(injected, storage()) {
		t.Fatalf("refused observation/fault/restoration changed complete storage: err=%v", err)
	}
	if err := RestoreSelectedAgentPreparationFaultForTest(ctx, selected, receipt); err != nil {
		t.Fatal(err)
	}
	restored = true
	if !reflect.DeepEqual(before, storage()) {
		t.Fatal("exact restoration did not preserve original physical bytes")
	}
	second, err := ApplySelectedAgentPreparationFaultForTest(ctx, selected, snapshot, fault)
	if err != nil || second == nil {
		t.Fatalf("install independent named replacement: %v err=%v", second, err)
	}
	secondRestored := false
	t.Cleanup(func() {
		if !secondRestored {
			if err := RestoreSelectedAgentPreparationFaultForTest(ctx, selected, second); err != nil {
				t.Errorf("join second selected preparation restoration: %v", err)
			}
		}
	})
	changed := storage()
	if reflect.DeepEqual(injected, changed) {
		t.Fatal("fault reinstall did not have an independent captured replacement")
	}
	if err := RestoreSelectedAgentPreparationFaultForTest(ctx, selected, receipt); err == nil || !reflect.DeepEqual(changed, storage()) {
		t.Fatal("stale receipt overwrote a changed replacement")
	}
	if err := RestoreSelectedAgentPreparationFaultForTest(ctx, selected, second); err != nil {
		t.Fatal(err)
	}
	secondRestored = true
	if !reflect.DeepEqual(before, storage()) {
		t.Fatal("second restoration lost physical conservation")
	}
}
