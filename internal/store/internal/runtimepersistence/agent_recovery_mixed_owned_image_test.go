package runtimepersistence

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/google/uuid"
)

type mixedOwnedRecoveryMember struct {
	mixedAgentProfileMembership
	Restart runlifecycle.StandingRestartDisposition
}

// This test image is evidence only: neither a slot count nor live permission.
type mixedOwnedRecoveryImage struct {
	Head    agenttopology.SourceSetPlan
	Process startupownership.AuthorityInspection
	Members map[string]mixedOwnedRecoveryMember
}

func proveMixedOwnedRecoveryImage(t *testing.T, ordinary agentClaimProfileFixture, selected selectedCompletionFixture, foreign agentFixtureFlowStore) {
	t.Helper()
	standing := startStandingAgentProfileProcessFixture(t, newStandingAgentProfileProcessFixture(t, ordinary.store))
	busy := claimAgentProfileRecoveryEvent(t, standing)
	pending := publishAgentProfileRecoveryEvent(t, standing)
	retry := claimAgentProfileRecoveryEvent(t, standing)
	prepareStandingProfileInspectionClaim(t, standing, retry, deliverylifecycle.ClaimDeferred)
	reader := ordinary.store.(agentRecoveryReadOwners)
	var held context.Context
	var original, latest mixedOwnedRecoveryImage
	var written map[string]SelectedForkStorageTableSnapshot
	if err := reader.InspectSnapshot(ordinary.ctx, func(ctx context.Context) error {
		held = ctx
		read := ordinary
		read.ctx = ctx
		before := profileFixtureSnapshot(t, read)
		original = readMixedOwnedRecoveryImage(t, read)
		assertMixedOwnedRecoveryClasses(t, original, read, standing, selected)
		assertConnectedAgentRecoveryImage(t, read, original)
		counts := map[string]int{selection.BackendAnthropic: 2, selection.BackendOpenAICompatible: 1}
		assertNativeAgentRecoveryOccupancy(t, read, counts)
		assertRecoveryReadOwnerContextRefusal(t, ctx, foreign.(agentRecoveryReadOwners), "different selected store")
		assertConnectedAgentRecoveryContextRefusal(t, ctx, foreign, "different selected store")
		added := advanceMixedOwnedRecoveryImage(t, standing, selected, busy, pending.ID(), original.Head)
		latest = readMixedOwnedRecoveryImage(t, ordinary)
		assertConnectedAgentRecoveryImage(t, ordinary, latest)
		assertNativeAgentRecoveryOccupancy(t, ordinary, counts)
		written = profileFixtureSnapshot(t, ordinary)
		if latest.Head.Revision == original.Head.Revision || !reflect.DeepEqual(latest.Process, original.Process) {
			t.Fatal("outside native source-head replacement was not distinct or changed the process stamp")
		}
		if len(latest.Members) != len(original.Members) {
			t.Fatal("mixed source cut requires equal cardinality across changed exact membership")
		}
		if _, found := latest.Members[busy.Snapshot.DeliveryID]; found {
			t.Fatal("outside settlement retained its exact standing member")
		}
		pendingID, err := deliverylifecycle.DeliveryID(pending.ID(), standing.route)
		if err != nil {
			t.Fatal(err)
		}
		if next := latest.Members[pendingID]; next.Snapshot.ClaimVersion != 1 || next.Snapshot.Status != deliverylifecycle.StatusInProgress || next.Ordinary.Validate() != nil {
			t.Fatalf("outside native standing first claim lost its exact captured evidence: %+v", next)
		}
		addedID, err := deliverylifecycle.DeliveryID(added.ID(), standing.route)
		if err != nil {
			t.Fatal(err)
		}
		if next := latest.Members[addedID]; next.Snapshot.EventID != added.ID() || next.Snapshot.ClaimVersion != 0 || next.Snapshot.Status != deliverylifecycle.StatusPending || next.Ordinary != (deliverylifecycle.AgentClaimProfile{}) {
			t.Fatalf("outside native publication lost its exact unclaimed event: %+v event=%s", next, added.ID())
		}
		for id, member := range original.Members {
			if id == busy.Snapshot.DeliveryID || id == pendingID {
				continue
			}
			expected := member
			if member.Snapshot.RunID == standing.route.AgentIdentity.RunID {
				expected.Run.EventCount++
			}
			if !reflect.DeepEqual(expected, latest.Members[id]) {
				t.Fatalf("outside standing writer changed unrelated complete evidence %s: before=%+v after=%+v", id, member, latest.Members[id])
			}
		}
		if again := readMixedOwnedRecoveryImage(t, read); !reflect.DeepEqual(original, again) {
			t.Fatal("owned mixed source/evidence image crossed its held native cut")
		}
		assertConnectedAgentRecoveryImage(t, read, original)
		assertNativeAgentRecoveryOccupancy(t, read, counts)
		if after := profileFixtureSnapshot(t, read); !reflect.DeepEqual(before, after) {
			t.Fatal("held mixed owned image changed complete application storage")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	assertRecoveryReadOwnerContextRefusal(t, held, reader, "inspection snapshot is closed")
	assertConnectedAgentRecoveryContextRefusal(t, held, ordinary.store, "inspection snapshot is closed")
	if current := readMixedOwnedRecoveryImage(t, ordinary); !reflect.DeepEqual(current, latest) {
		t.Fatal("closing the inspection hid the exact current mixed image")
	}
	assertConnectedAgentRecoveryImage(t, ordinary, latest)
	assertAgentProfileProcessNoAttempts(t, ordinary)
	if after := profileFixtureSnapshot(t, ordinary); !reflect.DeepEqual(written, after) {
		t.Fatal("mixed owned observations or context refusal changed complete storage")
	}
	process, err := agentfixture.ProcessCapability(t, standing.ctx, standing.store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := process.RestoreSourceSet(standing.ctx, agenttopology.SourceSetCommitRequest{
		OperationID: uuid.NewString(), ExpectedRevision: latest.Head.Revision, Plan: original.Head,
	}); err != nil {
		t.Fatal(err)
	}
	t.Log("one native cut enumerates ordinary, standing and fenced-selected durable membership through owned read ports with exact source head, process stamp and canonical captures; no slot count, possession or launch permission")
}

func readMixedOwnedRecoveryImage(t *testing.T, f agentClaimProfileFixture) mixedOwnedRecoveryImage {
	t.Helper()
	reader := f.store.(agentRecoveryReadOwners)
	head, found, err := reader.InspectSourceSetHead(f.ctx)
	if err != nil || !found || head.Validate() != nil {
		t.Fatalf("mixed image requires its complete canonical source head: %+v found=%t err=%v", head, found, err)
	}
	stamp, err := f.store.(interface {
		InspectAuthority(context.Context) (startupownership.AuthorityInspection, error)
	}).InspectAuthority(f.ctx)
	if err != nil || stamp.Validate() != nil || stamp.Authority == nil || stamp.Possession != nil {
		t.Fatalf("mixed image lost its durable process stamp or invented possession: %+v err=%v", stamp, err)
	}
	active, err := reader.ReadActiveDeliverySnapshots(f.ctx)
	if err != nil || active == nil {
		t.Fatalf("mixed image failed complete native membership: %+v err=%v", active, err)
	}
	image := mixedOwnedRecoveryImage{Head: head, Process: stamp, Members: make(map[string]mixedOwnedRecoveryMember)}
	for _, raw := range active {
		if _, duplicate := image.Members[raw.DeliveryID]; duplicate || raw.SubscriberClass != deliverylifecycle.SubscriberAgent {
			t.Fatalf("mixed native fixture duplicated or changed its subscriber class: %+v", raw)
		}
		actual, err := f.store.Snapshot(f.ctx, raw.DeliveryID)
		undecorated := actual
		undecorated.RetryScheduled, undecorated.ClaimReclaimable = false, false
		if err != nil || !reflect.DeepEqual(undecorated, raw) {
			t.Fatalf("mixed raw membership differs from exact native durable fields: %+v snapshot=%+v err=%v", raw, actual, err)
		}
		member := readMixedOwnedRecoveryMember(t, f, actual, head)
		binding := member.Lifecycle.ProcessBinding
		if binding.ProcessAuthorityID != stamp.Authority.AuthorityID || binding.ProcessBootID != stamp.Authority.BootID ||
			binding.ProcessOwnerID != stamp.Authority.OwnerID || binding.RuntimeInstanceID != stamp.Authority.RuntimeInstanceID {
			t.Fatalf("mixed actor and durable process stamp do not share exact immutable attribution: %+v stamp=%+v", binding, stamp)
		}
		image.Members[raw.DeliveryID] = member
	}
	return image
}

func readMixedOwnedRecoveryMember(t *testing.T, f agentClaimProfileFixture, actual deliverylifecycle.Snapshot, head agenttopology.SourceSetPlan) mixedOwnedRecoveryMember {
	t.Helper()
	member := mixedOwnedRecoveryMember{mixedAgentProfileMembership: mixedAgentProfileMembership{Snapshot: actual}}
	var err error
	member.Run, err = f.store.(bus.RunLifecycleReadPersistence).LoadRunLifecycleSnapshot(f.ctx, actual.RunID)
	if err != nil || member.Run.RunID != actual.RunID {
		t.Fatalf("mixed owned image lost its exact native run: %+v err=%v", member.Run, err)
	}
	switch actual.Authority.Kind() {
	case deliverylifecycle.ExecutionAuthoritySelectedContractFork:
		member.Selected, member.Lifecycle = readMixedSelectedAgentProfile(t, f, actual)
	case deliverylifecycle.ExecutionAuthorityNormalRuntime:
		member.Ordinary, err = ReadFirstAgentClaimProfileForTest(f.ctx, f.store, actual)
		if actual.ClaimVersion == 0 {
			if actual.Status != deliverylifecycle.StatusPending || err == nil || member.Ordinary != (deliverylifecycle.AgentClaimProfile{}) {
				t.Fatalf("mixed pending/v0 invented a captured provider profile: %+v err=%v", member, err)
			}
		} else if err != nil || !member.Ordinary.MatchesDelivery(actual) {
			t.Fatalf("mixed native first-capture evidence: %+v err=%v", member, err)
		}
		var found bool
		member.Lifecycle, found, err = f.store.LoadAgentLifecycleState(f.ctx, actual.Route.AgentIdentity)
		if err != nil || !found || member.Lifecycle.Identity != actual.Route.AgentIdentity {
			t.Fatalf("mixed native lifecycle read: %+v found=%t err=%v", member.Lifecycle, found, err)
		}
		if actual.ClaimVersion > 0 && (member.Lifecycle.ConfigRevision != member.Ordinary.ConfigurationRevision ||
			member.Lifecycle.RuntimeEpoch != member.Ordinary.AgentRuntimeEpoch || member.Lifecycle.Generation != member.Ordinary.AgentGeneration ||
			member.Lifecycle.ProcessBinding.GenerationGrantID != member.Ordinary.ExecutionID || member.Lifecycle.ProcessBinding.RuntimeGeneration != member.Ordinary.ExecutionGeneration) {
			t.Fatalf("mixed first capture lost the exact native configuration/agent/execution generations: %+v", member)
		}
		owned, err := f.store.(interface {
			ObserveOrdinaryRunSource(context.Context, correlation.SourceArtifactFact, string) (bool, error)
		}).ObserveOrdinaryRunSource(f.ctx, actual.Authority.SourceArtifact(), actual.RunID)
		if err != nil || !owned {
			t.Fatalf("mixed exact ordinary/standing source-run binding: owned=%t err=%v", owned, err)
		}
		coordinate := agenttopology.SourceCoordinate{BundleHash: actual.Authority.SourceArtifact().BundleHash()}
		sourceFound := false
		for _, source := range head.Sources {
			if source == coordinate {
				sourceFound = true
			}
		}
		if !sourceFound || member.Lifecycle.ProcessBinding.BundleHash != coordinate.BundleHash {
			t.Fatalf("mixed native actor/source missing from complete head: %+v head=%+v", member, head)
		}
		member.Restart, err = f.store.(runlifecycle.StandingRestartDispositionReader).StandingRunRestartDisposition(f.ctx, actual.RunID)
		if err != nil || member.Restart.Validate() != nil {
			t.Fatalf("mixed native restart classification: %+v err=%v", member.Restart, err)
		}
		if member.Restart.UsesGenericRecovery() {
			identity, err := actual.Route.AgentIdentity.Plan()
			if err != nil {
				t.Fatal(err)
			}
			joined := false
			for _, desired := range head.Agents {
				if desired.Identity == identity && desired.ConfigRevision == member.Lifecycle.ConfigRevision && desired.Source == coordinate {
					joined = true
				}
			}
			if !joined {
				t.Fatalf("ordinary native configuration is not in the exact complete source plan: %+v", member)
			}
		}
	default:
		t.Fatalf("mixed native agent authority is unclassified: %+v", actual)
	}
	return member
}

func assertMixedOwnedRecoveryClasses(t *testing.T, image mixedOwnedRecoveryImage, ordinary, standing agentClaimProfileFixture, selected selectedCompletionFixture) {
	t.Helper()
	classes := make(map[string]int)
	for _, member := range image.Members {
		switch member.Snapshot.RunID {
		case ordinary.route.AgentIdentity.RunID:
			if !member.Restart.UsesGenericRecovery() {
				t.Fatal("ordinary member acquired a standing restart classification")
			}
			classes["ordinary"]++
		case standing.route.AgentIdentity.RunID:
			if !member.Restart.Executable() || member.Restart.UsesGenericRecovery() {
				t.Fatal("standing member lost its exact healthy intrinsic generation")
			}
			classes["standing"]++
		case selected.forkRun:
			if member.Selected != selected.request.Preparation.Actors[0] || member.Ordinary != (deliverylifecycle.AgentClaimProfile{}) {
				t.Fatal("fenced selected member changed its immutable preparation owner")
			}
			observation, err := ordinary.store.ObserveDeliveryContinuation(ordinary.ctx, member.Snapshot.Authority, member.Snapshot.DeliveryID)
			_, wake := observation.Wake.After()
			if err != nil || observation.Disposition != deliverylifecycle.ClaimWrongAuthority || wake {
				t.Fatalf("fenced selected membership reopened live authorization: %+v err=%v", observation, err)
			}
			classes["selected"]++
		default:
			t.Fatalf("mixed fixture contains an unclassified native run: %+v", member)
		}
	}
	if !reflect.DeepEqual(classes, map[string]int{"ordinary": 3, "standing": 3, "selected": 1}) {
		t.Fatalf("mixed complete membership lost a class: %v", classes)
	}
}

func advanceMixedOwnedRecoveryImage(t *testing.T, standing agentClaimProfileFixture, selected selectedCompletionFixture, busy deliverylifecycle.ClaimedObligation, pendingEventID string, head agenttopology.SourceSetPlan) events.Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(standing.ctx, 10*time.Second)
	defer cancel()
	if _, err := standing.store.SettleSuccess(ctx, busy.Claim, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection()); err != nil {
		t.Fatal(err)
	}
	prepared, found, err := standing.store.(bus.PreparedPublishEventReader).LoadPreparedPublishEvent(ctx, pendingEventID)
	if err != nil || !found {
		t.Fatalf("standing pending event missing before exact native claim: found=%t err=%v", found, err)
	}
	claim, err := standing.store.ClaimDelivery(ctx, standing.authority, prepared.Event.Event(), standing.route)
	if _, acquired := claim.Acquired(); err != nil || !claim.Acknowledged || !acquired {
		t.Fatalf("outside mixed writer could not acquire its exact standing event: %+v err=%v", claim, err)
	}
	standing.ctx = ctx
	added := publishAgentProfileRecoveryEvent(t, standing)
	sources := append([]agenttopology.SourceCoordinate(nil), head.Sources...)
	sources = append(sources, agenttopology.SourceCoordinate{BundleHash: selected.request.DeclarationPlan.BundleHash})
	next, err := agenttopology.NewSourceSetPlan(sources, head.Agents)
	if err != nil || next.Revision == head.Revision {
		t.Fatalf("outside source-head writer requires a distinct valid full candidate: %+v err=%v", next, err)
	}
	process, err := agentfixture.ProcessCapability(t, ctx, standing.store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := process.RestoreSourceSet(ctx, agenttopology.SourceSetCommitRequest{
		OperationID: uuid.NewString(), ExpectedRevision: head.Revision, Plan: next,
	}); err != nil {
		t.Fatal(err)
	}
	return added
}
