package runtimepersistence

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

type gateRunCountCase struct {
	table string
	read  func(context.Context, any, string) (int, error)
}

func gateRunCountCases() []gateRunCountCase {
	return []gateRunCountCase{
		{"entity_state", CountWorkflowRunFieldRowsForTest},
		{"flow_instances", CountRunFlowInstanceStorageForTest},
		{"workflow_instance_initial_materializations", func(ctx context.Context, selected any, run string) (int, error) {
			accounting, err := ObserveH2ConstructionAccountingForTest(ctx, selected, run)
			return accounting.Constructions, err
		}},
		{"flow_instance_runtime_readiness", CountRunFlowRuntimeReadinessStorageForTest},
		{"timers", CountRunTimerStorageForTest},
		{"entity_mutations", CountRunMutationStorageForTest},
		{"fan_out_intents", CountRunFanOutIntentStorageForTest},
		{"fan_out_obligation_barriers", CountRunFanOutBarrierStorageForTest},
		{"fan_out_outcomes", CountNestedRunFanOutOutcomesForTest},
	}
}

func TestGateRunCountsRetainPhysicalScopesAndIndependentReadsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, state := newCompetingRevisionPairFixture(t, backend)
			runID := state.Identity.RunID
			cases := gateRunCountCases()
			want := make([]int, len(cases))
			for index, tc := range cases {
				if err := f.db.QueryRowContext(f.ctx, "SELECT COUNT(*) FROM "+tc.table+" WHERE run_id=$1", runID).Scan(&want[index]); err != nil {
					t.Fatal(err)
				}
			}
			if want[0] == 0 || want[1] == 0 || want[2] == 0 || want[3] == 0 || want[5] == 0 {
				t.Fatalf("native construction did not establish positive scoped evidence: %v", want)
			}
			before := competingRevisionPairSnapshot(t, f)
			probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			for index, tc := range cases {
				if got, err := tc.read(f.ctx, f.store, runID); err != nil || got != want[index] {
					t.Fatalf("%s count changed its physical run scope: %d want %d err=%v", tc.table, got, want[index], err)
				}
			}
			if counters := probe.Snapshot(); counters.Active != 0 || counters.Total.Begun != uint64(len(cases)) || counters.Total.ReadCommits != uint64(len(cases)) || counters.Total.WriteCommits != 0 {
				t.Fatalf("counts merged temporal cuts or escaped the selected read coordinator: %+v", counters)
			}
			for _, tc := range cases {
				if got, err := tc.read(f.ctx, f.store, uuid.NewString()); err != nil || got != 0 {
					t.Fatalf("%s borrowed other-run history: %d %v", tc.table, got, err)
				}
				cancelled, cancel := context.WithCancel(f.ctx)
				cancel()
				if got, err := tc.read(cancelled, f.store, runID); !errors.Is(err, context.Canceled) || got != 0 {
					t.Fatalf("%s ignored cancellation or exposed partial evidence: %d %v", tc.table, got, err)
				}
			}
			if !reflect.DeepEqual(before, competingRevisionPairSnapshot(t, f)) {
				t.Fatal("counts changed data, history, leases or clocks")
			}
			restore()
			assertGateMembershipEventCountExcludesActualOrdinaryEvent(t, f, runID)
		})
	}
}

func assertGateMembershipEventCountExcludesActualOrdinaryEvent(t *testing.T, f receiverConfigActivationFixture, runID string) {
	t.Helper()
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), "review/finish.requested", "fixture", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
	if err := commitSemanticEventFixtureWithRoutes(f.ctx, f.store.(deliveryFixtureStore), event, nil); err != nil {
		t.Fatal(err)
	}
	var physical, all int
	if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name IN ('platform.join_complete','platform.join_timeout','items.child','batch.completed')`, runID).Scan(&physical); err != nil {
		t.Fatal(err)
	}
	if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1`, runID).Scan(&all); err != nil || all <= physical {
		t.Fatalf("actual ordinary event did not distinguish the refusal scope: %d/%d %v", all, physical, err)
	}
	before := competingRevisionPairSnapshot(t, f)
	if got, err := CountA2MembershipRefusalEventsForTest(f.ctx, f.store, runID); err != nil || got != physical {
		t.Fatalf("membership footprint borrowed ordinary events: %d want %d err=%v", got, physical, err)
	}
	if got, err := CountA2MembershipRefusalEventsForTest(f.ctx, f.store, uuid.NewString()); err != nil || got != 0 {
		t.Fatalf("membership footprint borrowed another run: %d %v", got, err)
	}
	if !reflect.DeepEqual(before, competingRevisionPairSnapshot(t, f)) {
		t.Fatal("membership event count changed persisted evidence")
	}
}

func TestGateRunCountsRefuseRawAndClosedOwners(t *testing.T) {
	f, state := newCompetingRevisionPairFixture(t, "sqlite")
	cases := append(gateRunCountCases(), gateRunCountCase{"membership_events", CountA2MembershipRefusalEventsForTest})
	for _, tc := range cases {
		if got, err := tc.read(f.ctx, f.db, state.Identity.RunID); err == nil || got != 0 {
			t.Fatalf("%s accepted a raw pool: %d %v", tc.table, got, err)
		}
	}
	if err := f.store.(*SQLiteRuntimeStore).Close(); err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		if got, err := tc.read(f.ctx, f.store, state.Identity.RunID); err == nil || got != 0 {
			t.Fatalf("%s accepted a closed owner: %d %v", tc.table, got, err)
		}
	}
}
