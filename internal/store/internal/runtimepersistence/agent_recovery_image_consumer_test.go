package runtimepersistence

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/startuprecovery"
	"github.com/google/uuid"
)

var _ startuprecovery.AgentRecoveryReader = (*PostgresStore)(nil)
var _ startuprecovery.AgentRecoveryReader = (*SQLiteRuntimeStore)(nil)

type countedAgentRecoveryImageReader struct {
	startuprecovery.AgentRecoveryReader
	inspections int
}

func (r *countedAgentRecoveryImageReader) InspectSnapshot(ctx context.Context, read func(context.Context) error) error {
	r.inspections++
	return r.AgentRecoveryReader.InspectSnapshot(ctx, read)
}

func inspectNativeAgentRecoveryImage(t *testing.T, f agentClaimProfileFixture) (startuprecovery.AgentRecoveryImage, error) {
	t.Helper()
	reader := &countedAgentRecoveryImageReader{AgentRecoveryReader: f.store.(startuprecovery.AgentRecoveryReader)}
	image, err := startuprecovery.InspectAgentRecoveryImage(f.ctx, reader)
	if reader.inspections != 1 {
		t.Fatalf("agent recovery image opened %d inspection scopes, want one", reader.inspections)
	}
	return image, err
}

func assertConnectedAgentRecoveryImage(t *testing.T, f agentClaimProfileFixture, expected mixedOwnedRecoveryImage) {
	t.Helper()
	image, err := inspectNativeAgentRecoveryImage(t, f)
	if err != nil || !reflect.DeepEqual(image.SourceHead, expected.Head) || !reflect.DeepEqual(image.Process, expected.Process) || len(image.Members) != len(expected.Members) {
		t.Fatalf("connected consumer differs from native complete image: %+v err=%v", image, err)
	}
	seen := make(map[string]bool, len(image.Members))
	for _, member := range image.Members {
		want, found := expected.Members[member.Delivery.DeliveryID]
		if !found || seen[member.Delivery.DeliveryID] {
			t.Fatalf("connected consumer added/duplicated an exact member: %+v", member)
		}
		seen[member.Delivery.DeliveryID] = true
		raw := want.Snapshot
		raw.ClaimReclaimable, raw.RetryScheduled = false, false
		if !reflect.DeepEqual(member.Delivery, raw) || !reflect.DeepEqual(member.Run, want.Run) ||
			!reflect.DeepEqual(member.Lifecycle, want.Lifecycle) || member.Restart != want.Restart {
			t.Fatalf("connected consumer changed native member evidence: %+v want=%+v", member, want)
		}
		switch {
		case raw.ClaimVersion == 0:
			if member.CaptureKind != startuprecovery.AgentRecoveryUnclaimed || member.FirstClaim != nil || member.Selected != nil {
				t.Fatalf("pending/v0 invented captured provider evidence: %+v", member)
			}
		case raw.Authority.Kind() == deliverylifecycle.ExecutionAuthoritySelectedContractFork:
			if member.CaptureKind != startuprecovery.AgentRecoverySelectedPreparation || member.FirstClaim != nil || member.Selected == nil || *member.Selected != want.Selected {
				t.Fatalf("selected member lost immutable preparation: %+v want=%+v", member, want)
			}
		default:
			if member.CaptureKind != startuprecovery.AgentRecoveryOrdinaryFirstClaim || member.Selected != nil || member.FirstClaim == nil || *member.FirstClaim != want.Ordinary {
				t.Fatalf("ordinary/standing member lost immutable first claim: %+v want=%+v", member, want)
			}
		}
	}
}

func assertNativeAgentRecoveryImageRefusal(t *testing.T, f agentClaimProfileFixture) {
	t.Helper()
	before := profileFixtureSnapshot(t, f)
	image, err := inspectNativeAgentRecoveryImage(t, f)
	if err == nil || !reflect.DeepEqual(image, startuprecovery.AgentRecoveryImage{}) {
		t.Fatalf("failed native evidence exposed a partial recovery image: %+v err=%v", image, err)
	}
	if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(before, after) {
		t.Fatal("native connected recovery refusal changed complete application storage")
	}
}

func proveAgentRecoveryNonAgentAndTerminalExclusion(t *testing.T, f agentClaimProfileFixture) {
	t.Helper()
	original, err := inspectNativeAgentRecoveryImage(t, f)
	if err != nil || len(original.Members) == 0 {
		t.Fatalf("native classification requires existing agent members: %+v err=%v", original, err)
	}
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(mustPersistenceRootNode("recovery-non-agent")), Target: f.route.Target}
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), "test.profile_recovery", "operator", "", []byte(`{}`), 0, f.route.AgentIdentity.RunID, events.EventEnvelope{}, time.Now().UTC())
	event = eventtest.TargetRouted(event, route.Target.Route())
	commitSelectedReceiverClaimEvent(t, f.ctx, f.store, event, route, f.authority)
	raw, err := f.store.(deliverylifecycle.ActiveSnapshotReader).ReadActiveDeliverySnapshots(f.ctx)
	if err != nil || len(raw) != len(original.Members)+1 {
		t.Fatalf("native membership did not retain the unrelated active node: %+v err=%v", raw, err)
	}
	nodeID, err := deliverylifecycle.DeliveryID(event.ID(), route)
	if err != nil {
		t.Fatal(err)
	}
	nodes := 0
	for _, snapshot := range raw {
		if snapshot.DeliveryID == nodeID && snapshot.SubscriberClass == deliverylifecycle.SubscriberNode && snapshot.Status == deliverylifecycle.StatusPending && snapshot.ClaimVersion == 0 {
			nodes++
		}
	}
	if nodes != 1 {
		t.Fatalf("native unrelated node has no exact pending membership: %d", nodes)
	}
	before := profileFixtureSnapshot(t, f)
	image, err := inspectNativeAgentRecoveryImage(t, f)
	if err != nil || len(image.Members) != len(original.Members) {
		t.Fatalf("non-agent active delivery dropped an agent or became an agent member: %+v err=%v", image, err)
	}
	for i, member := range original.Members {
		if member.Delivery.RunID == event.RunID() {
			member.Run.EventCount++
		}
		if !reflect.DeepEqual(member, image.Members[i]) {
			t.Fatalf("non-agent exclusion changed an existing agent: %+v want=%+v", image.Members[i], member)
		}
	}
	rows, err := DecodeSelectedForkSnapshotRowsForTest(before["event_deliveries"])
	if err != nil {
		t.Fatal(err)
	}
	terminal := 0
	for _, row := range rows {
		status, err := row["status"].Text()
		if err != nil {
			t.Fatal(err)
		}
		if !deliverylifecycle.Status(status).Terminal() {
			continue
		}
		terminal++
		id, err := row["delivery_id"].Text()
		if err != nil {
			t.Fatal(err)
		}
		for _, member := range image.Members {
			if member.Delivery.DeliveryID == id {
				t.Fatalf("terminal history became an active recovery member: %+v", member)
			}
		}
	}
	if terminal == 0 {
		t.Fatal("terminal exclusion control lacks real terminal history")
	}
	if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(before, after) {
		t.Fatal("non-agent/terminal classification changed complete application storage")
	}
	assertAgentProfileProcessNoAttempts(t, f)
}

type wrongAgentRecoveryGenerationReader struct {
	startuprecovery.AgentRecoveryReader
	deliveryID   string
	profileReads int
}

func (r *wrongAgentRecoveryGenerationReader) ReadActiveDeliverySnapshots(ctx context.Context) ([]deliverylifecycle.Snapshot, error) {
	active, err := r.AgentRecoveryReader.ReadActiveDeliverySnapshots(ctx)
	if err != nil {
		return nil, err
	}
	for i := range active {
		if active[i].DeliveryID == r.deliveryID {
			authority := active[i].Authority
			active[i].Authority, err = deliverylifecycle.NewNormalExecutionAuthority(authority.SourceArtifact(), authority.ExecutionID(), authority.Generation()+1)
			if err != nil {
				return nil, err
			}
		}
	}
	return active, nil
}

func (r *wrongAgentRecoveryGenerationReader) ReadFirstAgentClaimProfile(ctx context.Context, expected deliverylifecycle.Snapshot) (deliverylifecycle.AgentClaimProfile, error) {
	r.profileReads++
	return r.AgentRecoveryReader.ReadFirstAgentClaimProfile(ctx, expected)
}

func proveAgentRecoveryExactNativeGenerationJoin(t *testing.T, f agentClaimProfileFixture) {
	t.Helper()
	image, err := inspectNativeAgentRecoveryImage(t, f)
	if err != nil {
		t.Fatal(err)
	}
	reader := &wrongAgentRecoveryGenerationReader{AgentRecoveryReader: f.store.(startuprecovery.AgentRecoveryReader)}
	for _, member := range image.Members {
		if member.CaptureKind == startuprecovery.AgentRecoveryOrdinaryFirstClaim {
			reader.deliveryID = member.Delivery.DeliveryID
			break
		}
	}
	if reader.deliveryID == "" {
		t.Fatal("exact-generation negative requires a real immutable ordinary capture")
	}
	before := profileFixtureSnapshot(t, f)
	image, err = startuprecovery.InspectAgentRecoveryImage(f.ctx, reader)
	if err == nil || !strings.Contains(err.Error(), "first agent profile differs from exact delivery/source authority") ||
		reader.profileReads == 0 || !reflect.DeepEqual(image, startuprecovery.AgentRecoveryImage{}) {
		t.Fatalf("hostile membership generation bypassed canonical exact native evidence: %+v profileReads=%d err=%v", image, reader.profileReads, err)
	}
	if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(before, after) {
		t.Fatal("refused expected-generation input changed complete native storage")
	}
}

func assertConnectedAgentRecoveryContextRefusal(t *testing.T, ctx context.Context, owner agentFixtureFlowStore, reason string) {
	t.Helper()
	image, err := inspectNativeAgentRecoveryImage(t, agentClaimProfileFixture{ctx: ctx, store: owner})
	if err == nil || !strings.Contains(err.Error(), reason) || !reflect.DeepEqual(image, startuprecovery.AgentRecoveryImage{}) {
		t.Fatalf("%s inspection exposed partial connected evidence: %+v err=%v", reason, image, err)
	}
}
