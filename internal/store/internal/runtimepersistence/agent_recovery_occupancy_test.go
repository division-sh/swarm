package runtimepersistence

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/startuprecovery"
)

func assertNativeAgentRecoveryOccupancy(t *testing.T, f agentClaimProfileFixture, want map[string]int) {
	t.Helper()
	reader := &countedAgentRecoveryImageReader{AgentRecoveryReader: f.store.(startuprecovery.AgentRecoveryReader)}
	candidate, err := startuprecovery.InspectAgentRecoveryOccupancyCandidate(f.ctx, reader)
	if err != nil || reader.inspections != 1 || !reflect.DeepEqual(candidate.InProgressByProfile, want) || candidate.Image.Process.Possession != nil {
		t.Fatalf("native occupancy candidate: %+v want=%v inspections=%d err=%v", candidate, want, reader.inspections, err)
	}
	t.Logf("one native inspection: durable in-progress profiles=%v, complete agent members=%d; not slot or launch authority", candidate.InProgressByProfile, len(candidate.Image.Members))
}

func assertNativeAgentRecoveryOccupancyRefusal(t *testing.T, f agentClaimProfileFixture) {
	t.Helper()
	candidate, err := startuprecovery.InspectAgentRecoveryOccupancyCandidate(f.ctx, f.store.(startuprecovery.AgentRecoveryReader))
	if err == nil || !reflect.DeepEqual(candidate, startuprecovery.AgentRecoveryOccupancyCandidate{}) {
		t.Fatalf("native evidence failure exposed partial occupancy: %+v err=%v", candidate, err)
	}
}

type duplicateAgentRecoveryMembershipReader struct {
	startuprecovery.AgentRecoveryReader
}

func (r duplicateAgentRecoveryMembershipReader) ReadActiveDeliverySnapshots(ctx context.Context) ([]deliverylifecycle.Snapshot, error) {
	active, err := r.AgentRecoveryReader.ReadActiveDeliverySnapshots(ctx)
	if err != nil || len(active) == 0 {
		return active, err
	}
	return append(active, active[0]), nil
}

func proveDuplicateAgentRecoveryOccupancyRefusal(t *testing.T, f agentClaimProfileFixture) {
	t.Helper()
	before := profileFixtureSnapshot(t, f)
	reader := duplicateAgentRecoveryMembershipReader{AgentRecoveryReader: f.store.(startuprecovery.AgentRecoveryReader)}
	candidate, err := startuprecovery.InspectAgentRecoveryOccupancyCandidate(f.ctx, reader)
	if err == nil || !strings.Contains(err.Error(), "membership duplicates delivery") || !reflect.DeepEqual(candidate, startuprecovery.AgentRecoveryOccupancyCandidate{}) {
		t.Fatalf("duplicate exact native member exposed occupancy: %+v err=%v", candidate, err)
	}
	if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(before, after) {
		t.Fatal("duplicate membership refusal changed complete application storage")
	}
}
