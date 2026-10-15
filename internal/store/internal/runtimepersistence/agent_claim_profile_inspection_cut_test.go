package runtimepersistence

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/google/uuid"
)

func TestAgentClaimProfileStandingInspectionRetainsOneNativeCutBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cell := range []struct {
			name string
			want deliverylifecycle.ClaimDisposition
		}{{"busy", deliverylifecycle.ClaimBusy}, {"reclaimable", deliverylifecycle.ClaimReclaimable},
			{"due_retry", deliverylifecycle.ClaimAcquired}, {"future_retry", deliverylifecycle.ClaimDeferred}} {
			t.Run(backend+"/"+cell.name, func(t *testing.T) { proveStandingProfileInspectionCut(t, backend, cell.want) })
		}
	}
}

func proveStandingProfileInspectionCut(t *testing.T, backend string, want deliverylifecycle.ClaimDisposition) {
	t.Helper()
	open := agentClaimProfileReopener(t, backend)
	standing := newStandingAgentProfileProcessFixture(t, open())
	f := startStandingAgentProfileProcessFixture(t, standing)
	first := claimAgentProfileRecoveryEvent(t, f)
	pending := publishAgentProfileRecoveryEvent(t, f)
	profile, err := ReadFirstAgentClaimProfileForTest(f.ctx, f.store, first.Snapshot)
	if err != nil {
		t.Fatal(err)
	}
	first.Snapshot = prepareStandingProfileInspectionClaim(t, f, first, want)
	stable := claimAgentProfileRecoveryEvent(t, f)
	stableProfile, err := ReadFirstAgentClaimProfileForTest(f.ctx, f.store, stable.Snapshot)
	if err != nil {
		t.Fatal(err)
	}
	process, err := agentfixture.ProcessCapability(t, f.ctx, f.store)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := process.Evidence()
	if err != nil {
		t.Fatal(err)
	}
	disposition, err := f.store.(runlifecycle.StandingRestartDispositionReader).StandingRunRestartDisposition(f.ctx, first.Snapshot.RunID)
	if err != nil || !disposition.Executable() {
		t.Fatalf("native standing cut requires its exact healthy generation: %+v err=%v", disposition, err)
	}
	foreign := newAgentClaimProfileFixture(t, backend)
	inspector := f.store.(interface {
		InspectSnapshot(context.Context, func(context.Context) error) error
	})
	var scoped context.Context
	var written map[string]SelectedForkStorageTableSnapshot
	if err := inspector.InspectSnapshot(f.ctx, func(ctx context.Context) error {
		scoped = ctx
		read := f
		read.ctx = ctx
		before := profileFixtureSnapshot(t, read)
		assertStandingProfileInspectionEvidence(t, read, first.Snapshot, profile, authority, disposition)
		assertStandingProfileInspectionStableClaim(t, read, stable.Snapshot, stableProfile)
		assertStandingProfileInspectionDeliveries(t, read, first.Snapshot, stable.Snapshot, pending, want)
		assertStandingProfileInspectionRefusals(t, read, foreign.store, first.Snapshot, pending)
		advanceStandingProfileInspectionWriter(t, f, first, profile, pending, want)
		written = profileFixtureSnapshot(t, f)
		assertStandingProfileInspectionEvidence(t, read, first.Snapshot, profile, authority, disposition)
		assertStandingProfileInspectionStableClaim(t, read, stable.Snapshot, stableProfile)
		assertStandingProfileInspectionDeliveries(t, read, first.Snapshot, stable.Snapshot, pending, want)
		if after := profileFixtureSnapshot(t, read); !reflect.DeepEqual(before, after) {
			t.Fatal("inspection crossed its native delivery/source/process/standing cut")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	assertStandingProfileInspectionWriterOutcome(t, f, first.Snapshot, profile, want)
	if captured, err := ReadFirstAgentClaimProfileForTest(scoped, f.store, first.Snapshot); err == nil ||
		!strings.Contains(err.Error(), "inspection snapshot is closed") || captured != (deliverylifecycle.AgentClaimProfile{}) {
		t.Fatalf("closed inspection context escaped its native cut: %+v err=%v", captured, err)
	}
	assertAgentProfileProcessNoAttempts(t, f)
	if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(written, after) {
		t.Fatal("inspection or closed-context refusal changed writer's complete storage")
	}
	t.Log("existing InspectSnapshot retains one native inventory/continuation/profile/source/process-stamp/standing-generation cut across committed settlement and claim; no slot or launch permission")
}

func prepareStandingProfileInspectionClaim(t *testing.T, f agentClaimProfileFixture, first deliverylifecycle.ClaimedObligation, want deliverylifecycle.ClaimDisposition) deliverylifecycle.Snapshot {
	t.Helper()
	switch want {
	case deliverylifecycle.ClaimReclaimable:
		if err := ExpireExactDeliveryClaimFaultForTest(f.ctx, f.store, first.Claim); err != nil {
			t.Fatal(err)
		}
	case deliverylifecycle.ClaimAcquired, deliverylifecycle.ClaimDeferred:
		base := time.Nanosecond
		if want == deliverylifecycle.ClaimDeferred {
			base = time.Hour
		}
		if _, err := f.store.SettleFailure(f.ctx, first.Claim, deliverylifecycle.Settlement{
			Disposition: deliverylifecycle.FailureRetry, Failure: testRetryableFailure(), RetryBase: base,
			RuleSelection: deliverylifecycle.NotApplicableHandlerRuleObservation(),
		}); err != nil {
			t.Fatal(err)
		}
	case deliverylifecycle.ClaimBusy:
	default:
		t.Fatalf("unsupported inspection proof disposition %s", want)
	}
	actual, err := f.store.Snapshot(f.ctx, first.Snapshot.DeliveryID)
	if err != nil {
		t.Fatal(err)
	}
	observation, err := f.store.ObserveDeliveryContinuation(f.ctx, f.authority, actual.DeliveryID)
	_, wake := observation.Wake.After()
	wantWake := want == deliverylifecycle.ClaimBusy || want == deliverylifecycle.ClaimDeferred
	if err != nil || observation.Disposition != want || observation.Invariant != nil || wake != wantWake {
		t.Fatalf("native inspection starting cut: %+v want=%s wake=%t err=%v", observation, want, wantWake, err)
	}
	return actual
}

func advanceStandingProfileInspectionWriter(t *testing.T, f agentClaimProfileFixture, first deliverylifecycle.ClaimedObligation, profile deliverylifecycle.AgentClaimProfile, pending events.Event, want deliverylifecycle.ClaimDisposition) {
	t.Helper()
	// The writer deliberately keeps the original context, never the read-only
	// inspection context. SQLite's normal bootstrapped WAL permits this commit.
	ctx, cancel := context.WithTimeout(f.ctx, 10*time.Second)
	defer cancel()
	claim := first.Claim
	if want == deliverylifecycle.ClaimAcquired || want == deliverylifecycle.ClaimReclaimable {
		prepared, found, err := f.store.(bus.PreparedPublishEventReader).LoadPreparedPublishEvent(ctx, first.Snapshot.EventID)
		if err != nil || !found {
			t.Fatalf("exact event for native retry/reclaim: found=%t err=%v", found, err)
		}
		result, err := f.store.ClaimDelivery(ctx, f.authority, prepared.Event.Event(), f.route)
		next, acquired := result.Acquired()
		if err != nil || !result.Acknowledged || !acquired || next.Snapshot.ClaimVersion != first.Snapshot.ClaimVersion+1 {
			t.Fatalf("native writer exact retry/reclaim while inspection held: %+v err=%v", result, err)
		}
		assertAgentProfileRecoveryCapture(t, f, next.Snapshot, profile)
		claim = next.Claim
	}
	if want != deliverylifecycle.ClaimDeferred {
		if _, err := f.store.SettleFailure(ctx, claim, deliverylifecycle.Settlement{
			Disposition: deliverylifecycle.FailureRetry, Failure: testRetryableFailure(), RetryBase: time.Hour,
			RuleSelection: deliverylifecycle.NotApplicableHandlerRuleObservation(),
		}); err != nil {
			t.Fatalf("native writer settlement while inspection held: %v", err)
		}
	}
	result, err := f.store.ClaimDelivery(ctx, f.authority, pending, f.route)
	second, acquired := result.Acquired()
	if err != nil || !result.Acknowledged || !acquired || second.Snapshot.ClaimVersion != 1 {
		t.Fatalf("native writer first claim while inspection held: %+v err=%v", result, err)
	}
	assertStandingProfileInspectionWriterOutcome(t, f, first.Snapshot, profile, want)
}

func assertStandingProfileInspectionWriterOutcome(t *testing.T, f agentClaimProfileFixture, first deliverylifecycle.Snapshot, profile deliverylifecycle.AgentClaimProfile, original deliverylifecycle.ClaimDisposition) {
	t.Helper()
	wantStatus, wantDisposition, wantVersion, failed := deliverylifecycle.StatusFailed, deliverylifecycle.ClaimDeferred, first.ClaimVersion, 1
	if original == deliverylifecycle.ClaimAcquired || original == deliverylifecycle.ClaimReclaimable {
		wantVersion++
	}
	if original == deliverylifecycle.ClaimAcquired {
		// The due retry already consumed the first failure. Its second failure
		// exhausts the unchanged agent retry policy, rather than arming retry3.
		wantStatus, wantDisposition, failed = deliverylifecycle.StatusDeadLetter, deliverylifecycle.ClaimTerminal, 0
	}
	actual, err := f.store.Snapshot(f.ctx, first.DeliveryID)
	if err != nil || actual.Status != wantStatus || actual.ClaimVersion != wantVersion {
		t.Fatalf("native writer exact settlement result: %+v status=%s version=%d err=%v", actual, wantStatus, wantVersion, err)
	}
	assertAgentProfileRecoveryCapture(t, f, actual, profile)
	observed, err := f.store.ObserveDeliveryContinuation(f.ctx, f.authority, actual.DeliveryID)
	_, wake := observed.Wake.After()
	if err != nil || observed.Invariant != nil || observed.Disposition != wantDisposition || wake != (wantDisposition == deliverylifecycle.ClaimDeferred) {
		t.Fatalf("native writer retained the wrong continuation/wake: %+v want=%s err=%v", observed, wantDisposition, err)
	}
	assertAgentProfileCompositionInventory(t, f, 0, failed, 2)
}

func assertStandingProfileInspectionEvidence(t *testing.T, f agentClaimProfileFixture, first deliverylifecycle.Snapshot, profile deliverylifecycle.AgentClaimProfile, process startupownership.Authority, standing runlifecycle.StandingRestartDisposition) {
	t.Helper()
	reader := f.store.(interface {
		InspectAuthority(context.Context) (startupownership.AuthorityInspection, error)
		ObserveOrdinaryRunSource(context.Context, correlation.SourceArtifactFact, string) (bool, error)
		runlifecycle.StandingRestartDispositionReader
	})
	stamp, err := reader.InspectAuthority(f.ctx)
	if err != nil || stamp.Validate() != nil || stamp.Authority == nil || *stamp.Authority != process || stamp.Possession != nil {
		t.Fatalf("inspection lost exact durable process stamp or inferred possession: %+v err=%v", stamp, err)
	}
	matches, err := reader.ObserveOrdinaryRunSource(f.ctx, f.source, first.RunID)
	if err != nil || !matches {
		t.Fatalf("inspection lost exact source/run ownership: matches=%t err=%v", matches, err)
	}
	current, err := reader.StandingRunRestartDisposition(f.ctx, first.RunID)
	if err != nil || current.Validate() != nil || current != standing {
		t.Fatalf("inspection lost exact intrinsic standing generation: %+v err=%v", current, err)
	}
	state, found, err := f.store.LoadAgentLifecycleState(f.ctx, profile.Identity)
	if err != nil || !found || state.ConfigRevision != profile.ConfigurationRevision || state.RuntimeEpoch != profile.AgentRuntimeEpoch ||
		state.Generation != profile.AgentGeneration || state.ProcessBinding.ProcessAuthorityID != process.AuthorityID ||
		state.ProcessBinding.ProcessBootID != process.BootID || state.ProcessBinding.ProcessOwnerID != process.OwnerID ||
		state.ProcessBinding.RuntimeInstanceID != process.RuntimeInstanceID || state.ProcessBinding.RuntimeGeneration != f.authority.Generation() ||
		state.ProcessBinding.GenerationGrantID != f.authority.ExecutionID() || state.ProcessBinding.BundleHash != f.source.BundleHash() {
		t.Fatalf("inspection changed native agent/config/process join: %+v found=%t err=%v", state, found, err)
	}
	assertAgentProfileRecoveryCapture(t, f, first, profile)
}

func assertStandingProfileInspectionStableClaim(t *testing.T, f agentClaimProfileFixture, stable deliverylifecycle.Snapshot, profile deliverylifecycle.AgentClaimProfile) {
	t.Helper()
	actual, err := f.store.Snapshot(f.ctx, stable.DeliveryID)
	if err != nil || actual.Status != deliverylifecycle.StatusInProgress || !reflect.DeepEqual(actual, stable) {
		t.Fatalf("inspection changed the unaffected in-progress claim: %+v err=%v", actual, err)
	}
	assertAgentProfileRecoveryCapture(t, f, actual, profile)
}

func assertStandingProfileInspectionDeliveries(t *testing.T, f agentClaimProfileFixture, first, stable deliverylifecycle.Snapshot, pending events.Event, wantFirst deliverylifecycle.ClaimDisposition) {
	t.Helper()
	if first.Status == deliverylifecycle.StatusFailed {
		assertAgentProfileCompositionInventory(t, f, 1, 1, 1)
	} else {
		assertAgentProfileCompositionInventory(t, f, 1, 0, 2)
	}
	actual, err := f.store.Snapshot(f.ctx, first.DeliveryID)
	if err != nil || !reflect.DeepEqual(actual, first) {
		t.Fatalf("inspection changed original in-progress claim: %+v err=%v", actual, err)
	}
	id, err := deliverylifecycle.DeliveryID(pending.ID(), f.route)
	if err != nil {
		t.Fatal(err)
	}
	unclaimed, err := f.store.Snapshot(f.ctx, id)
	if err != nil || unclaimed.Status != deliverylifecycle.StatusPending || unclaimed.ClaimVersion != 0 {
		t.Fatalf("inspection crossed pending/v0 membership: %+v err=%v", unclaimed, err)
	}
	if captured, err := ReadFirstAgentClaimProfileForTest(f.ctx, f.store, unclaimed); err == nil || captured != (deliverylifecycle.AgentClaimProfile{}) {
		t.Fatalf("pending/v0 inspection invented a capture: %+v err=%v", captured, err)
	}
	page, err := f.store.ScanDeliveryContinuations(f.ctx, f.authority, deliverylifecycle.ContinuationCursor{}, 64)
	if err != nil || !page.Exhausted || len(page.Items) != 3 {
		t.Fatalf("inspection changed complete continuation membership: %+v err=%v", page, err)
	}
	expected := map[string]deliverylifecycle.ClaimDisposition{
		first.DeliveryID: wantFirst, stable.DeliveryID: deliverylifecycle.ClaimBusy, id: deliverylifecycle.ClaimAcquired,
	}
	for _, item := range page.Items {
		want, found := expected[item.DeliveryID]
		if !found {
			t.Fatalf("inspection included a foreign delivery: %+v", item)
		}
		delete(expected, item.DeliveryID)
		_, wake := item.Wake.After()
		wantWake := want == deliverylifecycle.ClaimBusy || want == deliverylifecycle.ClaimDeferred
		if item.Disposition != want || item.Invariant != nil || wake != wantWake {
			t.Fatalf("inspection changed exact continuation eligibility: %+v want=%s", item, want)
		}
	}
	if len(expected) != 0 {
		t.Fatalf("inspection lost required delivery membership: %v", expected)
	}
}

func assertStandingProfileInspectionRefusals(t *testing.T, f agentClaimProfileFixture, foreign agentFixtureFlowStore, first deliverylifecycle.Snapshot, pending events.Event) {
	t.Helper()
	if captured, err := ReadFirstAgentClaimProfileForTest(f.ctx, foreign, first); err == nil ||
		!strings.Contains(err.Error(), "different selected store") || captured != (deliverylifecycle.AgentClaimProfile{}) {
		t.Fatalf("inspection escaped into another selected owner: %+v err=%v", captured, err)
	}
	wrong, err := deliverylifecycle.NewNormalExecutionAuthority(f.source, uuid.NewString(), f.authority.Generation()+1)
	if err != nil {
		t.Fatal(err)
	}
	observed, err := f.store.ObserveDeliveryContinuation(f.ctx, wrong, first.DeliveryID)
	_, wake := observed.Wake.After()
	if err != nil || observed.Disposition != deliverylifecycle.ClaimWrongAuthority || wake || observed.Invariant != nil {
		t.Fatalf("historical inspection authorized foreign execution: %+v err=%v", observed, err)
	}
	result, err := f.store.ClaimDelivery(f.ctx, f.authority, pending, f.route)
	_, acquired := result.Acquired()
	if err == nil || !strings.Contains(err.Error(), "inspection snapshot") || acquired || result.Acknowledged {
		t.Fatalf("read-only inspection admitted a native claim: %+v err=%v", result, err)
	}
}
