package runtimepersistence

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/google/uuid"
)

// This joins existing recovery observations to immutable claim evidence. It
// does not admit provider work or implement a dispatcher/occupancy owner.
func TestAgentFirstClaimProfileRecoveryBeforeAttemptBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			proveAgentProfileRecoveryBeforeAttempt(t, backend)
		})
	}
}

func proveAgentProfileRecoveryBeforeAttempt(t *testing.T, backend string) {
	t.Helper()
	open := agentClaimProfileReopener(t, backend)
	f := admitAgentClaimProfileFixture(t, testAuthorActivityContext(), open())
	first := claimAgentProfileRecoveryEvent(t, f)
	original, err := ReadFirstAgentClaimProfileForTest(f.ctx, f.store, first.Snapshot)
	if err != nil {
		t.Fatal(err)
	}
	replacement := agentFixtureStaticRecord(t, f.route.AgentIdentity)
	profile, err := selection.ResolveActiveBackend(selection.BackendOpenAICompatible)
	if err != nil || profile.ID == original.ProfileID {
		t.Fatalf("independent replacement profile: %+v err=%v", profile, err)
	}
	resolved, err := llm.ResolveAgentExecution(executionposture.Live, profile, nil, replacement.Config)
	if err != nil {
		t.Fatal(err)
	}
	replacement.Config = resolved.Actor
	if err := agentfixture.UpsertStaticForSource(t, f.ctx, f.store, replacement, f.source); err != nil {
		t.Fatal(err)
	}
	state, found, err := f.store.LoadAgentLifecycleState(f.ctx, f.route.AgentIdentity)
	if err != nil || !found || state.ConfigRevision == original.ConfigurationRevision || state.Generation != original.AgentGeneration+1 {
		t.Fatalf("real replacement did not advance the agent candidate: %+v found=%v err=%v", state, found, err)
	}
	f.authority = admitAgentClaimProfileAuthority(t, f.ctx, f.store, f.source)
	if f.authority.Equal(first.Snapshot.Authority) {
		t.Fatal("changed-plan replacement did not mint its actual successor authority")
	}
	second := claimAgentProfileRecoveryEvent(t, f)
	current, err := ReadFirstAgentClaimProfileForTest(f.ctx, f.store, second.Snapshot)
	if err != nil || current.ProfileID != profile.ID || current.ConfigurationRevision != state.ConfigRevision || current.AgentGeneration != state.Generation {
		t.Fatalf("new first claim did not capture the replacement: %+v err=%v", current, err)
	}
	pendingEvent := publishAgentProfileRecoveryEvent(t, f)
	pendingID, err := deliverylifecycle.DeliveryID(pendingEvent.ID(), f.route)
	if err != nil {
		t.Fatal(err)
	}
	retry := claimAgentProfileRecoveryEvent(t, f)
	retryProfile := current
	retryProfile.DeliveryID, retryProfile.EventID = retry.Snapshot.DeliveryID, retry.Snapshot.EventID
	assertAgentProfileRecoveryCapture(t, f, retry.Snapshot, retryProfile)
	if _, err := f.store.SettleFailure(f.ctx, retry.Claim, deliverylifecycle.Settlement{
		Disposition: deliverylifecycle.FailureRetry, Failure: testRetryableFailure(), RetryBase: time.Hour,
		RuleSelection: deliverylifecycle.NotApplicableHandlerRuleObservation(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := f.store.ActivateDeliveryAuthority(f.ctx, f.authority); err != nil {
		t.Fatal(err)
	}
	storageBeforeClose := profileFixtureSnapshot(t, f)
	if err := f.store.(interface{ Close() error }).Close(); err != nil {
		t.Fatal(err)
	}
	f.store = open()
	before := profileFixtureSnapshot(t, f)
	if !reflect.DeepEqual(storageBeforeClose["event_delivery_attempts"], before["event_delivery_attempts"]) ||
		!reflect.DeepEqual(storageBeforeClose["event_deliveries"], before["event_deliveries"]) {
		t.Fatal("native reopen changed durable claims or immutable capture bytes")
	}
	assertAgentProfileRecoveryLifecycle(t, f, state)
	assertAgentProfileRecoveryInventory(t, f, first, second, pendingID, original, current, retryProfile)
	if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(before, after) {
		t.Fatal("recovery enumeration/profile joins changed complete application storage")
	}
	if err := ApplyAgentClaimProfileFaultForTest(f.ctx, f.store, second.Claim, current, AgentClaimProfileCorrupt, deliverylifecycle.Claim{}); err != nil {
		t.Fatal(err)
	}
	corrupted := profileFixtureSnapshot(t, f)
	assertOnlyAgentProfileCellChanged(t, before, corrupted, second.Snapshot.DeliveryID)
	actual, err := f.store.Snapshot(f.ctx, second.Snapshot.DeliveryID)
	if err != nil {
		t.Fatal(err)
	}
	if capture, err := ReadFirstAgentClaimProfileForTest(f.ctx, f.store, actual); err == nil || capture != (deliverylifecycle.AgentClaimProfile{}) {
		t.Fatalf("recovery accepted corrupt capture or substituted current configuration: %+v err=%v", capture, err)
	}
	if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(corrupted, after) {
		t.Fatal("corrupt recovery evidence refusal changed complete application storage")
	}
}

func assertAgentProfileRecoveryLifecycle(t *testing.T, f agentClaimProfileFixture, expected manager.AgentLifecycleState) {
	t.Helper()
	actual, found, err := f.store.LoadAgentLifecycleState(f.ctx, f.route.AgentIdentity)
	if err != nil || !found || !reflect.DeepEqual(expected, actual) {
		t.Fatalf("native reopen changed the replacement lifecycle: before=%+v after=%+v found=%v err=%v", expected, actual, found, err)
	}
}

func publishAgentProfileRecoveryEvent(t *testing.T, f agentClaimProfileFixture) events.Event {
	t.Helper()
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), "test.profile_recovery", "operator", "", []byte(`{}`), 0,
		f.route.AgentIdentity.RunID, events.EventEnvelope{}, time.Now().UTC())
	event = eventtest.TargetRouted(event, f.route.Target.Route())
	commitSelectedReceiverClaimEvent(t, f.ctx, f.store, event, f.route, f.authority)
	if err := acknowledgePipelineEventFixture(f.ctx, f.store, event.ID()); err != nil {
		t.Fatal(err)
	}
	return event
}

func claimAgentProfileRecoveryEvent(t *testing.T, f agentClaimProfileFixture) deliverylifecycle.ClaimedObligation {
	t.Helper()
	event := publishAgentProfileRecoveryEvent(t, f)
	result, err := f.store.ClaimDelivery(f.ctx, f.authority, event, f.route)
	claimed, acquired := result.Acquired()
	if err != nil || !acquired || !result.Acknowledged || claimed.Snapshot.ClaimVersion != 1 {
		t.Fatalf("real pre-attempt claim: %+v err=%v", result, err)
	}
	return claimed
}

func assertAgentProfileRecoveryInventory(t *testing.T, f agentClaimProfileFixture, first, second deliverylifecycle.ClaimedObligation,
	pendingID string, original, current, retryProfile deliverylifecycle.AgentClaimProfile) {
	t.Helper()
	inventory, err := f.store.InspectDeliveryRecovery(f.ctx, f.source)
	want := deliverylifecycle.RecoveryInventory{Runs: []deliverylifecycle.RecoveryRunInventory{{
		RunID: f.route.AgentIdentity.RunID, Pending: 1, Failed: 1, InProgress: 2,
	}}}
	if err != nil || !reflect.DeepEqual(inventory, want) {
		t.Fatalf("native recovery inventory: %+v want=%+v err=%v", inventory, want, err)
	}
	page, err := f.store.ScanDeliveryContinuations(f.ctx, f.authority, deliverylifecycle.ContinuationCursor{}, 64)
	if err != nil || !page.Exhausted || len(page.Items) != 4 {
		t.Fatalf("native recovery references: %+v err=%v", page, err)
	}
	expected := map[string]deliverylifecycle.AgentClaimProfile{first.Snapshot.DeliveryID: original, second.Snapshot.DeliveryID: current}
	counts := make(map[string]int)
	for _, item := range page.Items {
		if item.Invariant != nil {
			t.Fatalf("recovery reference invariant: %+v", item)
		}
		if capture, claimed := expected[item.DeliveryID]; claimed {
			assertAgentProfileRecoveryClaimDisposition(t, item, item.DeliveryID == first.Snapshot.DeliveryID)
			read, err := ReadFirstAgentClaimProfileForTest(f.ctx, f.store, item.Snapshot)
			if err != nil || read != capture {
				t.Fatalf("recovery replaced original profile/config/source/generation: %+v want=%+v err=%v", read, capture, err)
			}
			counts[read.ProfileID]++
			delete(expected, item.DeliveryID)
			continue
		}
		assertAgentProfileRecoveryUnclaimedOrRetry(t, f, item, pendingID, retryProfile)
	}
	if len(expected) != 0 || !reflect.DeepEqual(counts, map[string]int{original.ProfileID: 1, current.ProfileID: 1}) {
		t.Fatalf("immutable profile counts lost original claim: counts=%v missing=%v", counts, expected)
	}
	attempts, err := ReadExternalAttemptStorageForTest(f.ctx, f.store)
	if err != nil || len(attempts) != 0 {
		t.Fatalf("recovery source proof launched provider work: attempts=%d err=%v", len(attempts), err)
	}
	t.Logf("native reopen before any provider attempt: two in-progress claims retain distinct original profiles; pending and future retry stay outside that count; exact config/source/agent+runtime generations conserved")
}

func assertAgentProfileRecoveryClaimDisposition(t *testing.T, item deliverylifecycle.ContinuationItem, predecessor bool) {
	t.Helper()
	if item.Snapshot.Status != deliverylifecycle.StatusInProgress || item.Snapshot.ClaimVersion != 1 {
		t.Fatalf("in-progress recovery changed claim history: %+v", item)
	}
	if predecessor {
		if item.Disposition != deliverylifecycle.ClaimReclaimable || !item.Snapshot.ClaimReclaimable {
			t.Fatalf("successor activation failed to expose the predecessor's exact reclaim: %+v", item)
		}
		return
	}
	if _, present := item.Wake.After(); item.Disposition != deliverylifecycle.ClaimBusy || !present {
		t.Fatalf("recovery changed the current claim's lease/wake ownership: %+v", item)
	}
}

func assertAgentProfileRecoveryUnclaimedOrRetry(t *testing.T, f agentClaimProfileFixture, item deliverylifecycle.ContinuationItem, pendingID string, retryProfile deliverylifecycle.AgentClaimProfile) {
	t.Helper()
	switch item.DeliveryID {
	case pendingID:
		if item.Snapshot.Status != deliverylifecycle.StatusPending || item.Snapshot.ClaimVersion != 0 || item.Disposition != deliverylifecycle.ClaimAcquired {
			t.Fatalf("unclaimed recovery reference: %+v", item)
		}
		if _, err := ReadFirstAgentClaimProfileForTest(f.ctx, f.store, item.Snapshot); err == nil {
			t.Fatal("pending delivery invented immutable claim evidence")
		}
	case retryProfile.DeliveryID:
		if item.Snapshot.Status != deliverylifecycle.StatusFailed || item.Disposition != deliverylifecycle.ClaimDeferred {
			t.Fatalf("future retry lost its independent wake: %+v", item)
		}
		if _, present := item.Wake.After(); !present {
			t.Fatal("future retry lost its due-time wake")
		}
		assertAgentProfileRecoveryCapture(t, f, item.Snapshot, retryProfile)
	default:
		t.Fatalf("foreign recovery reference: %+v", item)
	}
}

func assertAgentProfileRecoveryCapture(t *testing.T, f agentClaimProfileFixture, snapshot deliverylifecycle.Snapshot, expected deliverylifecycle.AgentClaimProfile) {
	t.Helper()
	actual, err := ReadFirstAgentClaimProfileForTest(f.ctx, f.store, snapshot)
	if err != nil || actual != expected {
		t.Fatalf("recovery retry changed its exact first-claim profile/config/source/generation: actual=%+v expected=%+v err=%v", actual, expected, err)
	}
}
