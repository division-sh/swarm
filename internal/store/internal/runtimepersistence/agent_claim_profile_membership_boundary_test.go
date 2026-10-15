package runtimepersistence

import (
	"context"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/google/uuid"
)

// Exhausting a live authority's continuation scan is not complete recovery
// membership. This counterexample grants neither occupancy nor launch authority.
func TestAgentClaimProfileRecoveryScanHasAuthorityScopedMembershipBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			m := newAgentProfileMembershipFixture(t, backend)
			proveAgentProfileMembershipBoundary(t, m.current, m.retained.Snapshot, m.original, m.continuations, m.terminalID)
		})
	}
}

type agentProfileMembershipFixture struct {
	current       agentClaimProfileFixture
	retained      deliverylifecycle.ClaimedObligation
	original      deliverylifecycle.AgentClaimProfile
	busy          deliverylifecycle.ClaimedObligation
	pending       events.Event
	continuations map[string]deliverylifecycle.ClaimDisposition
	terminalID    string
}

func newAgentProfileMembershipFixture(t *testing.T, backend string) agentProfileMembershipFixture {
	t.Helper()
	first := newAgentClaimProfileFixture(t, backend)
	retained := claimAgentProfileRecoveryEvent(t, first)
	original, err := ReadFirstAgentClaimProfileForTest(first.ctx, first.store, retained.Snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := ExpireExactDeliveryClaimFaultForTest(first.ctx, first.store, retained.Claim); err != nil {
		t.Fatal(err)
	}
	next := newAgentProfileMembershipActor(t, first)
	if next.authority.Equal(first.authority) || next.source != first.source ||
		next.route.AgentIdentity.RunID == first.route.AgentIdentity.RunID {
		t.Fatal("membership counterexample requires actual distinct actor/run and successor grant on the same source")
	}
	busy := claimAgentProfileRecoveryEvent(t, next)
	pending := publishAgentProfileRecoveryEvent(t, next)
	pendingID, err := deliverylifecycle.DeliveryID(pending.ID(), next.route)
	if err != nil {
		t.Fatal(err)
	}
	due := claimAgentProfileRecoveryEvent(t, next)
	dueSnapshot := prepareStandingProfileInspectionClaim(t, next, due, deliverylifecycle.ClaimAcquired)
	future := claimAgentProfileRecoveryEvent(t, next)
	futureSnapshot := prepareStandingProfileInspectionClaim(t, next, future, deliverylifecycle.ClaimDeferred)
	terminal := claimAgentProfileRecoveryEvent(t, next)
	if _, err := next.store.SettleFailure(next.ctx, terminal.Claim, deliverylifecycle.Settlement{
		Disposition: deliverylifecycle.FailureDeadLetter, ReasonCode: "membership_terminal_control", Failure: testRetryableFailure(),
		RuleSelection: deliverylifecycle.NotApplicableHandlerRuleObservation(),
	}); err != nil {
		t.Fatal(err)
	}
	return agentProfileMembershipFixture{current: next, retained: retained, original: original, busy: busy, pending: pending,
		continuations: map[string]deliverylifecycle.ClaimDisposition{busy.Snapshot.DeliveryID: deliverylifecycle.ClaimBusy,
			pendingID: deliverylifecycle.ClaimAcquired, dueSnapshot.DeliveryID: deliverylifecycle.ClaimAcquired,
			futureSnapshot.DeliveryID: deliverylifecycle.ClaimDeferred}, terminalID: terminal.Snapshot.DeliveryID}
}

func newAgentProfileMembershipActor(t *testing.T, first agentClaimProfileFixture) agentClaimProfileFixture {
	t.Helper()
	ctx := correlation.WithRunID(first.ctx, uuid.NewString())
	identity := mustTestAgentIdentityForRun(correlation.RunIDFromContext(ctx), "membership-worker", "global")
	next := admitAgentClaimProfileIdentity(t, ctx, first.store, identity, first.source)
	replacement := agentFixtureStaticRecord(t, identity)
	profile, err := selection.ResolveActiveBackend(selection.BackendOpenAICompatible)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := llm.ResolveAgentExecution(executionposture.Live, profile, nil, replacement.Config)
	if err != nil {
		t.Fatal(err)
	}
	replacement.Config = resolved.Actor
	if err := agentfixture.UpsertStaticForSource(t, ctx, next.store, replacement, next.source); err != nil {
		t.Fatal(err)
	}
	next.authority = admitAgentClaimProfileAuthority(t, ctx, next.store, next.source)
	return next
}

func proveAgentProfileMembershipBoundary(t *testing.T, f agentClaimProfileFixture, retained deliverylifecycle.Snapshot,
	original deliverylifecycle.AgentClaimProfile, expected map[string]deliverylifecycle.ClaimDisposition, terminalID string) {
	t.Helper()
	inspector := f.store.(interface {
		InspectSnapshot(context.Context, func(context.Context) error) error
	})
	if err := inspector.InspectSnapshot(f.ctx, func(ctx context.Context) error {
		read := f
		read.ctx = ctx
		before := profileFixtureSnapshot(t, read)
		assertAgentProfileMembershipInventory(t, read, retained.RunID)
		assertAgentProfileMembershipScan(t, read, expected, original.ProfileID)
		actual, err := f.store.Snapshot(ctx, retained.DeliveryID)
		if err != nil || actual.Status != deliverylifecycle.StatusInProgress || actual.ClaimVersion != 1 ||
			!actual.ClaimReclaimable || !actual.Authority.Equal(retained.Authority) {
			t.Fatalf("authority-scoped scan omitted a claim that is no longer retained: %+v err=%v", actual, err)
		}
		assertAgentProfileRecoveryCapture(t, read, actual, original)
		observation, err := f.store.ObserveDeliveryContinuation(ctx, f.authority, actual.DeliveryID)
		_, wake := observation.Wake.After()
		if err != nil || observation.Disposition != deliverylifecycle.ClaimWrongAuthority || observation.Invariant != nil || wake {
			t.Fatalf("foreign grant became live through historical membership: %+v err=%v", observation, err)
		}
		terminal, err := f.store.Snapshot(ctx, terminalID)
		if err != nil || terminal.Status != deliverylifecycle.StatusDeadLetter {
			t.Fatalf("native terminal exclusion control: %+v err=%v", terminal, err)
		}
		observation, err = f.store.ObserveDeliveryContinuation(ctx, f.authority, terminalID)
		_, wake = observation.Wake.After()
		if err != nil || observation.Disposition != deliverylifecycle.ClaimTerminal || observation.Invariant != nil || wake {
			t.Fatalf("terminal history became executable membership: %+v err=%v", observation, err)
		}
		assertAgentProfileMembershipCapture(t, read, terminal, original.ProfileID)
		assertAgentProfileProcessNoAttempts(t, read)
		if after := profileFixtureSnapshot(t, read); !reflect.DeepEqual(before, after) {
			t.Fatal("membership diagnostic changed complete application storage")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	t.Log("one inspection cut: source inventory retains five active rows across two runs/actors; current-authority scan exhausts four, omitting the retained predecessor claim; terminal history remains readable but not eligible; no complete recovery image or slot permission")
}

func assertAgentProfileMembershipInventory(t *testing.T, f agentClaimProfileFixture, retainedRunID string) {
	t.Helper()
	inventory, err := f.store.InspectDeliveryRecovery(f.ctx, f.source)
	if err != nil || len(inventory.Runs) != 2 || inventory.Total() != 5 {
		t.Fatalf("same-cut native multi-run inventory: %+v err=%v", inventory, err)
	}
	for _, run := range inventory.Runs {
		want := deliverylifecycle.RecoveryRunInventory{RunID: retainedRunID, InProgress: 1}
		if run.RunID == f.route.AgentIdentity.RunID {
			want = deliverylifecycle.RecoveryRunInventory{RunID: run.RunID, Pending: 1, Failed: 2, InProgress: 1}
		}
		if run != want {
			t.Fatalf("native multi-run inventory lost exact status membership: %+v want=%+v", run, want)
		}
	}
}

func assertAgentProfileMembershipScan(t *testing.T, f agentClaimProfileFixture, expected map[string]deliverylifecycle.ClaimDisposition, oldProfile string) {
	t.Helper()
	seen := make(map[string]bool)
	var cursor deliverylifecycle.ContinuationCursor
	for pages := 0; pages <= len(expected); pages++ {
		page, err := f.store.ScanDeliveryContinuations(f.ctx, f.authority, cursor, 1)
		if err != nil || len(page.Items) != 1 {
			t.Fatalf("exact one-row membership page: %+v err=%v", page, err)
		}
		item := page.Items[0]
		want, found := expected[item.DeliveryID]
		_, wake := item.Wake.After()
		if !found || seen[item.DeliveryID] || item.Disposition != want || item.Invariant != nil ||
			wake != (want == deliverylifecycle.ClaimBusy || want == deliverylifecycle.ClaimDeferred) {
			t.Fatalf("authority page changed exact eligibility or wake: %+v expected=%v", item, expected)
		}
		seen[item.DeliveryID] = true
		assertAgentProfileMembershipCapture(t, f, item.Snapshot, oldProfile)
		if page.Exhausted {
			if len(seen) != len(expected) {
				t.Fatalf("authority scan lost its own members: %v want=%v", seen, expected)
			}
			return
		}
		cursor = page.Next
	}
	t.Fatal("authority scan did not terminate within its exact fixture membership")
}

func assertAgentProfileMembershipCapture(t *testing.T, f agentClaimProfileFixture, snapshot deliverylifecycle.Snapshot, oldProfile string) {
	t.Helper()
	profile, err := ReadFirstAgentClaimProfileForTest(f.ctx, f.store, snapshot)
	if snapshot.ClaimVersion == 0 {
		if snapshot.Status != deliverylifecycle.StatusPending || err == nil || profile != (deliverylifecycle.AgentClaimProfile{}) {
			t.Fatalf("pending/v0 invented immutable capture: %+v err=%v", profile, err)
		}
		return
	}
	if err != nil || !profile.MatchesDelivery(snapshot) || profile.ProfileID == oldProfile ||
		profile.BundleHash != f.source.BundleHash() || profile.ExecutionID != f.authority.ExecutionID() ||
		profile.ExecutionGeneration != f.authority.Generation() || profile.Identity != f.route.AgentIdentity {
		t.Fatalf("exact successor profile/source/actor/generation: %+v err=%v", profile, err)
	}
	state, found, err := f.store.LoadAgentLifecycleState(f.ctx, profile.Identity)
	if err != nil || !found || state.ConfigRevision != profile.ConfigurationRevision ||
		state.RuntimeEpoch != profile.AgentRuntimeEpoch || state.Generation != profile.AgentGeneration {
		t.Fatalf("same-cut successor configuration/agent-generation evidence: %+v profile=%+v found=%t err=%v", state, profile, found, err)
	}
}
