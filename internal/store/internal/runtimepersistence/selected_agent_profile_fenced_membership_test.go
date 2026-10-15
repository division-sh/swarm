package runtimepersistence

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/google/uuid"
)

// Active membership and immutable preparation survive a selected fence; none
// of those historical facts substitute for current work authorization.
func proveSelectedProfileFencedMembership(t *testing.T, selectedCtx context.Context, ordinary agentClaimProfileFixture,
	selected selectedCompletionFixture, execution effects.Authority, settled deliverylifecycle.Snapshot, foreign agentFixtureFlowStore) {
	t.Helper()
	if settled.Status != deliverylifecycle.StatusDelivered || !settled.Terminal() {
		t.Fatalf("selected membership control requires the actual settled original: %+v", settled)
	}
	baseline := readMixedAgentProfileMembership(t, ordinary)
	f := agentClaimProfileFixture{ctx: selectedCtx, store: ordinary.store, authority: settled.Authority,
		route: settled.Route, source: settled.Authority.SourceArtifact()}
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), "test.selected_profile", "operator", "", []byte(`{}`), 0,
		settled.RunID, events.EventEnvelope{}, time.Now().UTC())
	commitSelectedReceiverClaimEvent(t, f.ctx, f.store, event, f.route, f.authority)
	if err := runDeliveryFixtureAttempt(f.ctx, f.store, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
		adapter := postgresDeliveryAdapter
		if selected.sqlite {
			adapter = sqliteDeliveryAdapter
		}
		return adapter.CommitPipelineHandoff(ctx, attempt, event.ID())
	}); err != nil {
		t.Fatal(err)
	}
	result, err := f.store.ClaimDelivery(f.ctx, f.authority, event, f.route)
	claimed, acquired := result.Acquired()
	if err != nil || !result.Acknowledged || !acquired || claimed.Snapshot.ClaimVersion != 1 {
		t.Fatalf("real selected pre-fence first claim: %+v err=%v", result, err)
	}
	inspector := f.store.(interface {
		InspectSnapshot(context.Context, func(context.Context) error) error
	})
	var held context.Context
	var original map[string]mixedAgentProfileMembership
	var written map[string]SelectedForkStorageTableSnapshot
	if err := inspector.InspectSnapshot(ordinary.ctx, func(ctx context.Context) error {
		held = ctx
		read := ordinary
		read.ctx = ctx
		before := profileFixtureSnapshot(t, read)
		original = readMixedAgentProfileMembership(t, read)
		assertSelectedProfileMembershipBeforeFence(t, original, baseline, claimed.Snapshot, settled.DeliveryID)
		assertClaimHistoryContinuations(t, ctx, f.store, claimed.Snapshot, deliverylifecycle.ClaimBusy, true)
		assertAgentProfileMembershipWitnessRefusal(t, ctx, foreign, "different selected store")
		assertMixedAgentPreparationContextRefusal(t, ctx, foreign, claimed.Snapshot, "different selected store")
		writer, cancel := context.WithTimeout(selectedCtx, 10*time.Second)
		defer cancel()
		if err := selected.store.QuiesceRunForkSelectedContractRuntimeExecution(writer, execution); err != nil {
			t.Fatal(err)
		}
		written = profileFixtureSnapshot(t, ordinary)
		if reflect.DeepEqual(before, written) {
			t.Fatal("outside selected quiescence did not change durable execution state")
		}
		assertFencedSelectedContinuations(t, f.ctx, f.store, event, f.route, claimed.Snapshot, deliverylifecycle.ClaimWrongAuthority)
		assertSelectedProfileFencedImage(t, ordinary, selected, claimed.Snapshot, original)
		assertClaimHistoryContinuations(t, ctx, f.store, claimed.Snapshot, deliverylifecycle.ClaimBusy, true)
		if again := readMixedAgentProfileMembership(t, read); !reflect.DeepEqual(original, again) {
			t.Fatal("held selected membership crossed the outside execution fence")
		}
		if after := profileFixtureSnapshot(t, read); !reflect.DeepEqual(before, after) {
			t.Fatal("held application storage crossed the outside selected quiescence")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	assertAgentProfileMembershipWitnessRefusal(t, held, ordinary.store, "inspection snapshot is closed")
	assertMixedAgentPreparationContextRefusal(t, held, ordinary.store, claimed.Snapshot, "inspection snapshot is closed")
	assertSelectedProfileFencedImage(t, ordinary, selected, claimed.Snapshot, original)
	assertFencedSelectedContinuations(t, f.ctx, f.store, event, f.route, claimed.Snapshot, deliverylifecycle.ClaimWrongAuthority)
	assertAgentProfileProcessNoAttempts(t, ordinary)
	if after := profileFixtureSnapshot(t, ordinary); !reflect.DeepEqual(written, after) {
		t.Fatal("fenced historical membership observation or live refusal changed complete application storage")
	}
	t.Log("native selected quiescence leaves exact in-progress membership and original preparation readable, while current single/batch/scan/direct entrances fence it; held single/batch/scan still see their original busy lease; no slot or launch permission")
}

func assertSelectedProfileMembershipBeforeFence(t *testing.T, image, baseline map[string]mixedAgentProfileMembership,
	claimed deliverylifecycle.Snapshot, settledID string) {
	t.Helper()
	if len(image) != len(baseline)+1 || len(baseline) == 0 {
		t.Fatalf("selected membership fixture lacks mixed retained members: before=%d after=%d", len(baseline), len(image))
	}
	if _, found := image[settledID]; found {
		t.Fatal("active selected witness included the settled original claim")
	}
	for id, ordinary := range baseline {
		if ordinary.Snapshot.Authority.Kind() != deliverylifecycle.ExecutionAuthorityNormalRuntime {
			t.Fatalf("pre-existing mixed member is not the ordinary control: %+v", ordinary)
		}
		if member, found := image[id]; !found || !reflect.DeepEqual(ordinary, member) {
			t.Fatalf("selected publication changed unrelated ordinary membership: before=%+v after=%+v", ordinary, member)
		}
	}
	member, found := image[claimed.DeliveryID]
	if !found || !reflect.DeepEqual(member.Snapshot, claimed) || member.Snapshot.Status != deliverylifecycle.StatusInProgress ||
		member.Snapshot.ClaimVersion != 1 || member.Ordinary != (deliverylifecycle.AgentClaimProfile{}) {
		t.Fatalf("selected active witness lost its exact first claim: %+v expected=%+v", member, claimed)
	}
}

func assertSelectedProfileFencedImage(t *testing.T, f agentClaimProfileFixture, selected selectedCompletionFixture,
	claimed deliverylifecycle.Snapshot, before map[string]mixedAgentProfileMembership) {
	t.Helper()
	current := readMixedAgentProfileMembership(t, f)
	if !reflect.DeepEqual(before, current) {
		t.Fatal("live selected fence hid or replaced durable active membership/profile evidence")
	}
	member, found := current[claimed.DeliveryID]
	if !found || !reflect.DeepEqual(member.Snapshot, claimed) || member.Selected != selected.request.Preparation.Actors[0] {
		t.Fatalf("fenced selected claim lost its original immutable preparation: %+v expected=%+v", member, claimed)
	}
}
