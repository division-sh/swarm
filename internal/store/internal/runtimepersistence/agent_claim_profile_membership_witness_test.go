package runtimepersistence

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

// The reset inventory is authorized only as this test's membership witness,
// never as a dispatcher dependency, slot count or possession capability.
func TestAgentClaimProfileRecoveryMembershipWitnessSharesNativeCutBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			m := newAgentProfileMembershipFixture(t, backend)
			foreign := newAgentClaimProfileFixture(t, backend)
			proveAgentProfileMembershipWitness(t, m, foreign.store)
		})
	}
}

type agentProfileMembershipEvidence struct {
	Snapshot  deliverylifecycle.Snapshot
	Profile   deliverylifecycle.AgentClaimProfile
	Lifecycle manager.AgentLifecycleState
	Restart   runlifecycle.StandingRestartDisposition
}

func proveAgentProfileMembershipWitness(t *testing.T, m agentProfileMembershipFixture, foreign agentFixtureFlowStore) {
	t.Helper()
	f := m.current
	inspector := f.store.(interface {
		InspectSnapshot(context.Context, func(context.Context) error) error
	})
	var held context.Context
	var original map[string]agentProfileMembershipEvidence
	var latest map[string]agentProfileMembershipEvidence
	var written map[string]SelectedForkStorageTableSnapshot
	if err := inspector.InspectSnapshot(f.ctx, func(ctx context.Context) error {
		held = ctx
		read := f
		read.ctx = ctx
		before := profileFixtureSnapshot(t, read)
		original = readAgentProfileMembershipWitness(t, read)
		assertAgentProfileMembershipWitnessFixture(t, m, original)
		assertAgentProfileMembershipWitnessRefusal(t, ctx, foreign, "different selected store")
		added := advanceAgentProfileMembershipWitness(t, m)
		latest = readAgentProfileMembershipWitness(t, f)
		written = profileFixtureSnapshot(t, f)
		assertAgentProfileMembershipWriterCut(t, m, added, original, latest)
		if again := readAgentProfileMembershipWitness(t, read); !reflect.DeepEqual(original, again) {
			t.Fatal("test-only membership witness crossed the held native transaction")
		}
		if after := profileFixtureSnapshot(t, read); !reflect.DeepEqual(before, after) {
			t.Fatal("held membership/source/claim cut changed after outside native commits")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	assertAgentProfileMembershipWitnessRefusal(t, held, f.store, "inspection snapshot is closed")
	if current := readAgentProfileMembershipWitness(t, f); !reflect.DeepEqual(latest, current) || reflect.DeepEqual(original, current) {
		t.Fatal("closed inspection hid the writer's actual changed membership")
	}
	assertAgentProfileProcessNoAttempts(t, f)
	if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(written, after) {
		t.Fatal("membership reads or context refusals changed writer's complete storage")
	}
	t.Log("test-only active refs enumerate all fixture members in one selected cut across a real remove/add commit; exact native claim/profile/source/lifecycle reads reuse that cut; no general iterator-error or occupancy claim")
}

func readAgentProfileMembershipWitness(t *testing.T, f agentClaimProfileFixture) map[string]agentProfileMembershipEvidence {
	t.Helper()
	inventory, err := f.store.(destructivereset.InventoryReader).ReadResetInventory(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	image := make(map[string]agentProfileMembershipEvidence)
	for _, ref := range inventory.ActiveDeliveries {
		if _, duplicate := image[ref.DeliveryID]; duplicate {
			t.Fatalf("membership witness duplicated exact native ref %+v", ref)
		}
		actual, err := f.store.Snapshot(f.ctx, ref.DeliveryID)
		if err != nil || actual.DeliveryID != ref.DeliveryID || actual.RunID != ref.RunID || string(actual.Status) != ref.Status ||
			actual.SubscriberClass != deliverylifecycle.SubscriberAgent {
			t.Fatalf("exact native membership ref/read mismatch: %+v snapshot=%+v err=%v", ref, actual, err)
		}
		image[ref.DeliveryID] = readAgentProfileMembershipEvidence(t, f, actual)
	}
	return image
}

func readAgentProfileMembershipEvidence(t *testing.T, f agentClaimProfileFixture, actual deliverylifecycle.Snapshot) agentProfileMembershipEvidence {
	t.Helper()
	profile, err := ReadFirstAgentClaimProfileForTest(f.ctx, f.store, actual)
	if actual.ClaimVersion == 0 {
		if actual.Status != deliverylifecycle.StatusPending || err == nil || profile != (deliverylifecycle.AgentClaimProfile{}) {
			t.Fatalf("enumerated pending/v0 invented capture: %+v err=%v", actual, err)
		}
	} else if err != nil || !profile.MatchesDelivery(actual) {
		t.Fatalf("enumerated exact immutable first profile: %+v snapshot=%+v err=%v", profile, actual, err)
	}
	owned, err := f.store.(interface {
		ObserveOrdinaryRunSource(context.Context, correlation.SourceArtifactFact, string) (bool, error)
	}).ObserveOrdinaryRunSource(f.ctx, actual.Authority.SourceArtifact(), actual.RunID)
	if err != nil || !owned || actual.Authority.SourceArtifact() != f.source {
		t.Fatalf("enumerated exact native run/source binding: owned=%t snapshot=%+v err=%v", owned, actual, err)
	}
	state, found, err := f.store.LoadAgentLifecycleState(f.ctx, actual.Route.AgentIdentity)
	if err != nil || !found || state.Identity != actual.Route.AgentIdentity || state.ProcessBinding.BundleHash != f.source.BundleHash() {
		t.Fatalf("enumerated canonical lifecycle/source evidence: %+v found=%t err=%v", state, found, err)
	}
	if actual.ClaimVersion > 0 && actual.Authority.Equal(f.authority) &&
		(state.ConfigRevision != profile.ConfigurationRevision || state.RuntimeEpoch != profile.AgentRuntimeEpoch || state.Generation != profile.AgentGeneration) {
		t.Fatalf("current fixture capture lost exact native configuration/generation: %+v profile=%+v", state, profile)
	}
	standing, err := f.store.(runlifecycle.StandingRestartDispositionReader).StandingRunRestartDisposition(f.ctx, actual.RunID)
	if err != nil || standing.Validate() != nil || !standing.UsesGenericRecovery() {
		t.Fatalf("enumerated ordinary run changed native restart classification: %+v err=%v", standing, err)
	}
	return agentProfileMembershipEvidence{Snapshot: actual, Profile: profile, Lifecycle: state, Restart: standing}
}

func assertAgentProfileMembershipWitnessFixture(t *testing.T, m agentProfileMembershipFixture, image map[string]agentProfileMembershipEvidence) {
	t.Helper()
	if len(image) != len(m.continuations)+1 {
		t.Fatalf("native witness fixture membership=%d want=%d", len(image), len(m.continuations)+1)
	}
	for id := range m.continuations {
		if _, found := image[id]; !found {
			t.Fatalf("complete native witness lost successor member %s", id)
		}
	}
	retained, found := image[m.retained.Snapshot.DeliveryID]
	if !found || retained.Profile != m.original || !retained.Snapshot.ClaimReclaimable {
		t.Fatalf("complete native witness lost original reclaimable evidence: %+v", retained)
	}
	if _, found := image[m.terminalID]; found {
		t.Fatal("active witness included the native terminal-history control")
	}
}

func advanceAgentProfileMembershipWitness(t *testing.T, m agentProfileMembershipFixture) events.Event {
	t.Helper()
	f := m.current
	ctx, cancel := context.WithTimeout(f.ctx, 10*time.Second)
	defer cancel()
	if settled, err := f.store.SettleSuccess(ctx, m.busy.Claim, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection()); err != nil || settled.Status != deliverylifecycle.StatusDelivered {
		t.Fatalf("outside membership writer exact settlement: %+v err=%v", settled, err)
	}
	claimed, err := f.store.ClaimDelivery(ctx, f.authority, m.pending, f.route)
	if _, acquired := claimed.Acquired(); err != nil || !claimed.Acknowledged || !acquired || claimed.Snapshot.ClaimVersion != 1 {
		t.Fatalf("outside membership writer exact first claim: %+v err=%v", claimed, err)
	}
	f.ctx = ctx
	return publishAgentProfileRecoveryEvent(t, f)
}

func assertAgentProfileMembershipWriterCut(t *testing.T, m agentProfileMembershipFixture, published events.Event, before, after map[string]agentProfileMembershipEvidence) {
	t.Helper()
	if len(before) != len(after) {
		t.Fatalf("counterexample needs same cardinality but changed exact membership: %d vs %d", len(before), len(after))
	}
	if _, found := after[m.busy.Snapshot.DeliveryID]; found {
		t.Fatal("outside writer did not remove exact settled member")
	}
	newID, err := deliverylifecycle.DeliveryID(published.ID(), m.current.route)
	if err != nil {
		t.Fatal(err)
	}
	added, found := after[newID]
	if !found || added.Snapshot.EventID != published.ID() || added.Snapshot.Status != deliverylifecycle.StatusPending ||
		added.Snapshot.ClaimVersion != 0 || added.Profile != (deliverylifecycle.AgentClaimProfile{}) {
		t.Fatalf("outside writer added wrong exact native member: %+v event=%s", added, published.ID())
	}
	pendingID, err := deliverylifecycle.DeliveryID(m.pending.ID(), m.current.route)
	if err != nil {
		t.Fatal(err)
	}
	for id, member := range before {
		if id == m.busy.Snapshot.DeliveryID {
			continue
		}
		current, found := after[id]
		if id == pendingID {
			if !found || current.Snapshot.Status != deliverylifecycle.StatusInProgress || current.Snapshot.ClaimVersion != 1 || current.Profile.Validate() != nil {
				t.Fatalf("outside writer lost exact pending-to-first-claim result: %+v", current)
			}
		} else if !found || !reflect.DeepEqual(member, current) {
			t.Fatalf("outside writer changed an unrelated exact membership record: before=%+v after=%+v", member, current)
		}
	}
}

func assertAgentProfileMembershipWitnessRefusal(t *testing.T, ctx context.Context, selected agentFixtureFlowStore, reason string) {
	t.Helper()
	inventory, err := selected.(destructivereset.InventoryReader).ReadResetInventory(ctx)
	if err == nil || !strings.Contains(err.Error(), reason) || !reflect.DeepEqual(inventory, destructivereset.Inventory{}) {
		t.Fatalf("membership witness escaped its native context: %+v want=%q err=%v", inventory, reason, err)
	}
}
