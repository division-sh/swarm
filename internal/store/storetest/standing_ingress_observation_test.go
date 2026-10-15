package storetest

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func standingIngressCountPorts() []struct {
	name string
	read func(context.Context, any) (int, error)
} {
	return []struct {
		name string
		read func(context.Context, any) (int, error)
	}{
		{"services", CountStandingServicesStorage},
		{"generations", CountStandingGenerationsStorage},
		{"journal", CountStandingJournalStorage},
		{"origin_runs", CountStandingOriginRunsStorage},
		{"unsettled_deliveries", CountUnsettledStandingDeliveriesStorage},
		{"inbound_publications", CountInboundPublicationsStorage},
		{"inbound_events", CountInboundNamedEventsStorage},
		{"external_operations", CountExternalEffectOperationsStorage},
		{"flow_authorities", func(ctx context.Context, selected any) (int, error) {
			return CountStandingFlowAuthoritiesStorage(ctx, selected, "absent-flow")
		}},
		{"pending_retirement_cards", func(ctx context.Context, selected any) (int, error) {
			return CountPendingStandingRetirementCardsStorage(ctx, selected, "00000000-0000-4000-8000-000000000001")
		}},
		{"entity_rows", func(ctx context.Context, selected any) (int, error) {
			return CountStandingEntityStorage(ctx, selected, "00000000-0000-4000-8000-000000000001")
		}},
	}
}

func TestStandingIngressReadPortsKeepEmptyDomainsAndLeftJoinBothStores(t *testing.T) {
	for _, backend := range runFixtureProofBackends() {
		t.Run(backend.name, func(t *testing.T) {
			selected := backend.open(t)
			sourceartifactfixture.Require(t, context.Background(), selected)
			runID := uuid.NewString()
			RequireRun(t, context.Background(), selected, RunFixture{RunID: runID, Origin: ScenarioSetupOrigin()})
			reader := selected.(interface {
				InspectSnapshot(context.Context, func(context.Context) error) error
			})
			if err := reader.InspectSnapshot(context.Background(), func(ctx context.Context) error {
				row, err := ReadServedRunControlStorage(ctx, selected, runID)
				if err != nil || row != (ServedRunControlStorage{RunStatus: "running"}) {
					t.Fatalf("LEFT JOIN lost a run without control: %+v, %v", row, err)
				}
				for _, cut := range standingIngressCountPorts() {
					count, err := cut.read(ctx, selected)
					if err != nil || count != 0 {
						t.Fatalf("%s invented a physical fact: %d, %v", cut.name, count, err)
					}
				}
				_, err = ReadFirstStandingServiceStorage(ctx, selected)
				if !errors.Is(err, sql.ErrNoRows) {
					t.Fatal("missing standing service became evidence:", err)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStandingIngressReadPortsRefuseForeignCancelledAndClosedOwnersBothStores(t *testing.T) {
	for _, selected := range []any{nil, struct{}{}} {
		for _, cut := range standingIngressCountPorts() {
			if count, err := cut.read(context.Background(), selected); err == nil || count != 0 {
				t.Fatalf("%s admitted foreign owner: %d, %v", cut.name, count, err)
			}
		}
	}
	for _, backend := range runFixtureProofBackends() {
		t.Run(backend.name, func(t *testing.T) {
			selected := backend.open(t)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			for _, cut := range standingIngressCountPorts() {
				if count, err := cut.read(ctx, selected); !errors.Is(err, context.Canceled) || count != 0 {
					t.Fatalf("%s lost cancellation: %d, %v", cut.name, count, err)
				}
			}
			if err := selected.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			for _, cut := range standingIngressCountPorts() {
				if count, err := cut.read(context.Background(), selected); err == nil || count != 0 {
					t.Fatalf("%s retained closed-owner evidence: %d, %v", cut.name, count, err)
				}
			}
		})
	}
}
