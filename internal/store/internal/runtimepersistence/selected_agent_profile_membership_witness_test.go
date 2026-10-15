package runtimepersistence

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

// This discriminator composes existing evidence readers, not slot accounting
// or selected-execution permission. Reset inventory is test-only membership.
type mixedAgentProfileMembership struct {
	Snapshot  deliverylifecycle.Snapshot
	Ordinary  deliverylifecycle.AgentClaimProfile
	Selected  runfork.SelectedForkPreparedActor
	Lifecycle manager.AgentLifecycleState
	Run       bus.RunLifecycleSnapshot
}

func proveMixedAgentProfileMembershipWitness(t *testing.T, selectedCtx context.Context, ordinary agentClaimProfileFixture, selected selectedCompletionFixture,
	first deliverylifecycle.ClaimedObligation, foreign agentFixtureFlowStore) {
	t.Helper()
	busy := claimAgentProfileRecoveryEvent(t, ordinary)
	pending := publishAgentProfileRecoveryEvent(t, ordinary)
	retry := claimAgentProfileRecoveryEvent(t, ordinary)
	retry.Snapshot = prepareStandingProfileInspectionClaim(t, ordinary, retry, deliverylifecycle.ClaimDeferred)
	terminal := claimAgentProfileRecoveryEvent(t, ordinary)
	if _, err := ordinary.store.SettleSuccess(ordinary.ctx, terminal.Claim, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection()); err != nil {
		t.Fatal(err)
	}
	inspector := ordinary.store.(interface {
		InspectSnapshot(context.Context, func(context.Context) error) error
	})
	var held context.Context
	var written map[string]SelectedForkStorageTableSnapshot
	var latest map[string]mixedAgentProfileMembership
	if err := inspector.InspectSnapshot(ordinary.ctx, func(ctx context.Context) error {
		held = ctx
		read := ordinary
		read.ctx = ctx
		before := profileFixtureSnapshot(t, read)
		original := readMixedAgentProfileMembership(t, read)
		assertMixedAgentProfileMembershipFixture(t, ordinary, selected, first, busy, pending, retry.Snapshot, terminal.Snapshot.DeliveryID, original)
		assertAgentProfileMembershipWitnessRefusal(t, ctx, foreign, "different selected store")
		assertMixedAgentPreparationContextRefusal(t, ctx, foreign, first.Snapshot, "different selected store")
		added := advanceMixedAgentProfileMembership(t, selectedCtx, ordinary, first, busy, pending)
		latest = readMixedAgentProfileMembership(t, ordinary)
		written = profileFixtureSnapshot(t, ordinary)
		assertMixedAgentProfileMembershipWriter(t, first, busy, pending, added, ordinary.route, original, latest)
		if again := readMixedAgentProfileMembership(t, read); !reflect.DeepEqual(original, again) {
			t.Fatal("mixed ordinary/selected evidence crossed the held inspection cut")
		}
		if after := profileFixtureSnapshot(t, read); !reflect.DeepEqual(before, after) {
			t.Fatal("mixed membership/source/generation inspection crossed outside commits")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	assertAgentProfileMembershipWitnessRefusal(t, held, ordinary.store, "inspection snapshot is closed")
	assertMixedAgentPreparationContextRefusal(t, held, ordinary.store, first.Snapshot, "inspection snapshot is closed")
	if current := readMixedAgentProfileMembership(t, ordinary); !reflect.DeepEqual(latest, current) {
		t.Fatal("mixed inspection hid the actual post-commit membership")
	}
	assertAgentProfileProcessNoAttempts(t, ordinary)
	if after := profileFixtureSnapshot(t, ordinary); !reflect.DeepEqual(written, after) {
		t.Fatal("mixed evidence reads or context refusals changed complete application storage")
	}
	t.Log("one native cut retains complete ordinary/selected active refs, distinct first-profile owners and native actor/run evidence across real settlements and first claim; no occupancy, live possession or launch claim")
}

func readMixedAgentProfileMembership(t *testing.T, f agentClaimProfileFixture) map[string]mixedAgentProfileMembership {
	t.Helper()
	inventory, err := f.store.(destructivereset.InventoryReader).ReadResetInventory(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	image := make(map[string]mixedAgentProfileMembership)
	for _, ref := range inventory.ActiveDeliveries {
		if _, duplicate := image[ref.DeliveryID]; duplicate {
			t.Fatalf("mixed membership duplicated native ref %+v", ref)
		}
		actual, err := f.store.Snapshot(f.ctx, ref.DeliveryID)
		if err != nil || actual.DeliveryID != ref.DeliveryID || actual.RunID != ref.RunID || string(actual.Status) != ref.Status ||
			actual.SubscriberClass != deliverylifecycle.SubscriberAgent {
			t.Fatalf("mixed exact native ref/read mismatch: %+v snapshot=%+v err=%v", ref, actual, err)
		}
		member := mixedAgentProfileMembership{Snapshot: actual}
		switch actual.Authority.Kind() {
		case deliverylifecycle.ExecutionAuthorityNormalRuntime:
			evidence := readAgentProfileMembershipEvidence(t, f, actual)
			member.Ordinary, member.Lifecycle = evidence.Profile, evidence.Lifecycle
		case deliverylifecycle.ExecutionAuthoritySelectedContractFork:
			member.Selected, member.Lifecycle = readMixedSelectedAgentProfile(t, f, actual)
		default:
			t.Fatalf("unclassified mixed agent authority %+v", actual)
		}
		member.Run, err = f.store.(bus.RunLifecycleReadPersistence).LoadRunLifecycleSnapshot(f.ctx, actual.RunID)
		if err != nil || member.Run.RunID != actual.RunID {
			t.Fatalf("mixed exact native run: %+v snapshot=%+v err=%v", member.Run, actual, err)
		}
		image[ref.DeliveryID] = member
	}
	return image
}

func readMixedSelectedAgentProfile(t *testing.T, f agentClaimProfileFixture, actual deliverylifecycle.Snapshot) (runfork.SelectedForkPreparedActor, manager.AgentLifecycleState) {
	t.Helper()
	reader := f.store.(interface {
		ReadSelectedAgentPreparation(context.Context, deliverylifecycle.Snapshot) (runfork.SelectedForkPreparedActor, error)
	})
	actor, err := reader.ReadSelectedAgentPreparation(f.ctx, actual)
	plan, planErr := actual.Route.AgentIdentity.Plan()
	if err != nil || planErr != nil || actor.Plan != plan || actual.Authority.ForkRunID() != actual.RunID {
		t.Fatalf("mixed exact immutable selected preparation: %+v snapshot=%+v err=%v planErr=%v", actor, actual, err, planErr)
	}
	if ordinary, err := ReadFirstAgentClaimProfileForTest(f.ctx, f.store, actual); err == nil || ordinary != (deliverylifecycle.AgentClaimProfile{}) {
		t.Fatalf("mixed selected member invented ordinary capture: %+v err=%v", ordinary, err)
	}
	state, found, err := f.store.LoadAgentLifecycleState(f.ctx, actual.Route.AgentIdentity)
	if err != nil || !found || state.Identity != actual.Route.AgentIdentity || state.ConfigRevision != actor.ConfigurationRevision ||
		state.ProcessBinding.BundleHash != actual.Authority.SourceArtifact().BundleHash() {
		t.Fatalf("mixed selected native lifecycle/source: %+v found=%t actor=%+v err=%v", state, found, actor, err)
	}
	return actor, state
}

func assertMixedAgentProfileMembershipFixture(t *testing.T, ordinary agentClaimProfileFixture, selected selectedCompletionFixture,
	first, busy deliverylifecycle.ClaimedObligation, pending events.Event, retry deliverylifecycle.Snapshot, terminalID string,
	image map[string]mixedAgentProfileMembership) {
	t.Helper()
	pendingID, err := deliverylifecycle.DeliveryID(pending.ID(), ordinary.route)
	if err != nil {
		t.Fatal(err)
	}
	if len(image) != 4 {
		t.Fatalf("mixed native membership=%d want=4", len(image))
	}
	for id, want := range map[string]deliverylifecycle.Status{
		first.Snapshot.DeliveryID: deliverylifecycle.StatusInProgress, busy.Snapshot.DeliveryID: deliverylifecycle.StatusInProgress,
		pendingID: deliverylifecycle.StatusPending, retry.DeliveryID: deliverylifecycle.StatusFailed,
	} {
		if member, found := image[id]; !found || member.Snapshot.Status != want {
			t.Fatalf("mixed witness lost exact native member %s: %+v want=%s", id, member, want)
		}
	}
	if _, found := image[terminalID]; found {
		t.Fatal("mixed active witness included terminal history")
	}
	prepared, captured := image[first.Snapshot.DeliveryID], image[busy.Snapshot.DeliveryID]
	if prepared.Selected != selected.request.Preparation.Actors[0] || prepared.Ordinary != (deliverylifecycle.AgentClaimProfile{}) ||
		captured.Selected != (runfork.SelectedForkPreparedActor{}) || captured.Ordinary.ProfileID == prepared.Selected.Backend ||
		captured.Snapshot.RunID == prepared.Snapshot.RunID || captured.Lifecycle.Identity == prepared.Lifecycle.Identity ||
		captured.Snapshot.Authority.SourceArtifact().Matches(prepared.Snapshot.Authority.SourceArtifact()) {
		t.Fatalf("mixed proof lacks independent run/actor/source/profile owners: ordinary=%+v selected=%+v", captured, prepared)
	}
	if unclaimed := image[pendingID]; unclaimed.Snapshot.ClaimVersion != 0 || unclaimed.Ordinary != (deliverylifecycle.AgentClaimProfile{}) {
		t.Fatalf("mixed pending/v0 invented profile: %+v", unclaimed)
	}
}

func advanceMixedAgentProfileMembership(t *testing.T, selectedCtx context.Context, f agentClaimProfileFixture, selected, ordinary deliverylifecycle.ClaimedObligation, pending events.Event) events.Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(f.ctx, 10*time.Second)
	defer cancel()
	deadline, _ := ctx.Deadline()
	selectedCtx, cancelSelected := context.WithDeadline(selectedCtx, deadline)
	defer cancelSelected()
	for _, write := range []struct {
		ctx   context.Context
		claim deliverylifecycle.Claim
	}{{selectedCtx, selected.Claim}, {ctx, ordinary.Claim}} {
		settled, err := f.store.SettleSuccess(write.ctx, write.claim, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection())
		if err != nil || settled.Status != deliverylifecycle.StatusDelivered {
			t.Fatalf("mixed outside writer exact native settlement: %+v err=%v", settled, err)
		}
	}
	claimed, err := f.store.ClaimDelivery(ctx, f.authority, pending, f.route)
	if _, acquired := claimed.Acquired(); err != nil || !claimed.Acknowledged || !acquired || claimed.Snapshot.ClaimVersion != 1 {
		t.Fatalf("mixed outside writer exact first claim: %+v err=%v", claimed, err)
	}
	f.ctx = ctx
	return publishAgentProfileRecoveryEvent(t, f)
}

func assertMixedAgentProfileMembershipWriter(t *testing.T, selected, ordinary deliverylifecycle.ClaimedObligation, pending, added events.Event,
	route events.DeliveryRoute, before, after map[string]mixedAgentProfileMembership) {
	t.Helper()
	if len(after) != len(before)-1 {
		t.Fatalf("mixed writer membership=%d want=%d", len(after), len(before)-1)
	}
	for _, removed := range []string{selected.Snapshot.DeliveryID, ordinary.Snapshot.DeliveryID} {
		if _, found := after[removed]; found {
			t.Fatalf("mixed writer retained exact settled member %s", removed)
		}
	}
	pendingID, err := deliverylifecycle.DeliveryID(pending.ID(), route)
	if err != nil {
		t.Fatal(err)
	}
	current := after[pendingID]
	if current.Snapshot.Status != deliverylifecycle.StatusInProgress || current.Snapshot.ClaimVersion != 1 || current.Ordinary.Validate() != nil {
		t.Fatalf("mixed writer lost pending-to-first-claim result: %+v", current)
	}
	addedID, err := deliverylifecycle.DeliveryID(added.ID(), route)
	if err != nil {
		t.Fatal(err)
	}
	if member, found := after[addedID]; !found || member.Snapshot.EventID != added.ID() ||
		member.Snapshot.Status != deliverylifecycle.StatusPending || member.Snapshot.ClaimVersion != 0 || member.Ordinary != (deliverylifecycle.AgentClaimProfile{}) {
		t.Fatalf("mixed writer added wrong native pending member: %+v event=%s", member, added.ID())
	}
	for id, original := range before {
		if id == selected.Snapshot.DeliveryID || id == ordinary.Snapshot.DeliveryID || id == pendingID {
			continue
		}
		member, found := after[id]
		expected := original
		expected.Run.EventCount++
		if !found || !reflect.DeepEqual(expected, member) {
			t.Fatalf("mixed writer changed unrelated evidence: before=%+v after=%+v", original, member)
		}
	}
}

func assertMixedAgentPreparationContextRefusal(t *testing.T, ctx context.Context, store agentFixtureFlowStore, snapshot deliverylifecycle.Snapshot, reason string) {
	t.Helper()
	reader := store.(interface {
		ReadSelectedAgentPreparation(context.Context, deliverylifecycle.Snapshot) (runfork.SelectedForkPreparedActor, error)
	})
	actor, err := reader.ReadSelectedAgentPreparation(ctx, snapshot)
	if err == nil || !strings.Contains(err.Error(), reason) || actor != (runfork.SelectedForkPreparedActor{}) {
		t.Fatalf("selected preparation escaped inspection context: %+v want=%q err=%v", actor, reason, err)
	}
}
