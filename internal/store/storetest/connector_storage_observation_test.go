package storetest

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestConnectorIdentityAndChronologyPreservePhysicalScopeBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) RunFixtureStore
	}{
		{"sqlite", func(t *testing.T) RunFixtureStore { return StartSQLiteRuntimeStore(t) }},
		{"postgres", func(t *testing.T) RunFixtureStore { return StartPostgresRuntimeStore(t) }},
	} {
		t.Run(backend.name, func(t *testing.T) {
			selected := backend.open(t)
			ctx := semanticFixtureContext(context.Background(), sourceartifactfixture.Fact())
			run, otherRun, entity, otherEntity := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
			for _, id := range []string{run, otherRun} {
				RequireRun(t, ctx, selected, RunFixture{RunID: id, Origin: ScenarioSetupOrigin()})
			}
			at := time.Now().UTC().Truncate(time.Microsecond)
			put := func(run, entity, name string, payload []byte, at time.Time) string {
				t.Helper()
				id := uuid.NewString()
				CommitSemanticEvent(t, ctx, selected, eventtest.ExistingRunRootIngressWithRoutingSource(id, events.EventType(name), "connector-observer", "", payload, 0, run,
					events.EventEnvelope{EntityID: entity}, eventtest.RootRoutingSource(run), at))
				return id
			}
			old := put(run, entity, "fixture.normalized", []byte(`{}`), at)
			latest := put(run, entity, "fixture.normalized", []byte(`{}`), at.Add(time.Second))
			foreignRun := put(otherRun, entity, "fixture.normalized", []byte(`{}`), at.Add(10*time.Second))
			put(run, otherEntity, "fixture.normalized", []byte(`{}`), at.Add(11*time.Second))
			wrongName := put(run, entity, "fixture.other", []byte(`{}`), at.Add(12*time.Second))
			probe := CollectTransactions(t, selected, TransactionProbeOptions{})
			count, err := CountConnectorNamedEventIdentity(ctx, selected, latest, "fixture.normalized")
			if err != nil || count != 1 {
				t.Fatalf("exact identity witness: %d/%v", count, err)
			}
			if counts := probe.Snapshot(); counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("identity observation escaped original read coordinator: %+v", counts)
			}
			if count, err := CountConnectorNamedEventIdentity(ctx, selected, latest, "fixture.normalized"); err != nil || count != 1 {
				t.Fatalf("exact identity witness: %d/%v", count, err)
			}
			if count, err := CountConnectorNamedEventIdentity(ctx, selected, latest, "fixture.other"); err != nil || count != 0 {
				t.Fatalf("name mismatch borrowed identity: %d/%v", count, err)
			}
			if count, err := CountConnectorNamedEventIdentity(ctx, selected, foreignRun, "fixture.normalized"); err != nil || count != 1 {
				t.Fatalf("identity-only witness gained a run restriction: %d/%v", count, err)
			}
			chronology, err := ReadConnectorEventChronology(ctx, selected, run)
			if err != nil || !strings.HasPrefix(chronology, old+":fixture.normalized,"+latest+":fixture.normalized,") || strings.Contains(chronology, foreignRun) || !strings.Contains(chronology, wrongName+":fixture.other") {
				t.Fatalf("physical chronology filtered or reordered events: %s/%v", chronology, err)
			}
			stored, found, err := ReadCanonicalEventRecord(ctx, selected, latest)
			if err != nil || !found || !stored.CreatedAt().Equal(at.Add(time.Second)) || stored.RunID() != run || stored.Envelope().EntityID != entity {
				t.Fatalf("canonical read reconstructed identity or clock: found=%t event=%v error=%v", found, stored, err)
			}
		})
	}
}

func TestConnectorStorageObservationsRefuseForeignCancelledAndClosedOwnersBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) RunFixtureStore
	}{
		{"sqlite", func(t *testing.T) RunFixtureStore { return StartSQLiteRuntimeStore(t) }},
		{"postgres", func(t *testing.T) RunFixtureStore { return StartPostgresRuntimeStore(t) }},
	} {
		t.Run(backend.name, func(t *testing.T) {
			selected := backend.open(t)
			run, entity, event, source := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
			reads := []struct {
				name   string
				read   func(context.Context, any) (bool, error)
				absent bool
			}{
				{"normalized", func(ctx context.Context, owner any) (bool, error) {
					v, err := ReadConnectorNormalizedProviderEventID(ctx, owner, run, entity, "fixture.ready", "provider-1")
					return v == "", err
				}, true},
				{"source_count", func(ctx context.Context, owner any) (bool, error) {
					v, err := CountConnectorNamedSourceEvent(ctx, owner, run, "fixture.ready", source)
					return v == 0, err
				}, false},
				{"identity_count", func(ctx context.Context, owner any) (bool, error) {
					v, err := CountConnectorNamedEventIdentity(ctx, owner, event, "fixture.ready")
					return v == 0, err
				}, false},
				{"chronology", func(ctx context.Context, owner any) (bool, error) {
					v, err := ReadConnectorEventChronology(ctx, owner, run)
					return v == "", err
				}, false},
				{"node_count", func(ctx context.Context, owner any) (bool, error) {
					v, err := CountConnectorRunNodeDeliveries(ctx, owner, run, "connector.node")
					return v == 0, err
				}, false},
				{"node_latest", func(ctx context.Context, owner any) (bool, error) {
					v, err := ReadConnectorLatestNodeDeliveryDiagnostic(ctx, owner, run, "connector.node")
					return v == "", err
				}, true},
				{"deadletter_count", func(ctx context.Context, owner any) (bool, error) {
					v, err := CountConnectorEntityDeadLetters(ctx, owner, entity)
					return v == 0, err
				}, false},
				{"deadletter_latest", func(ctx context.Context, owner any) (bool, error) {
					v, err := ReadConnectorLatestEntityDeadLetterDiagnostic(ctx, owner, entity)
					return v == "", err
				}, true},
				{"receipt_latest", func(ctx context.Context, owner any) (bool, error) {
					v, err := ReadConnectorLatestActivityReceiptDiagnostic(ctx, owner, run)
					return v == "", err
				}, true},
				{"attempt_count", func(ctx context.Context, owner any) (bool, error) {
					v, err := CountConnectorRunActivityAttempts(ctx, owner, run)
					return v == 0, err
				}, false},
				{"channel_succeeded", func(ctx context.Context, owner any) (bool, error) {
					v, err := CountChannelSucceededToolAttempts(ctx, owner, run, "channel.compiled_tool")
					return v == 0, err
				}, false},
				{"channel_first_source", func(ctx context.Context, owner any) (bool, error) {
					v, err := ReadChannelFirstActivitySourceRoute(ctx, owner, run)
					return v == nil, err
				}, true},
			}
			for _, read := range reads {
				t.Run(read.name, func(t *testing.T) {
					zero, err := read.read(context.Background(), selected)
					if !zero || (read.absent && !errors.Is(err, sql.ErrNoRows)) || (!read.absent && err != nil) {
						t.Fatalf("empty physical witness changed: zero=%t error=%v", zero, err)
					}
					for _, owner := range []any{nil, &sql.DB{}, &sql.Tx{}, struct{}{}} {
						if zero, err := read.read(context.Background(), owner); err == nil || !zero {
							t.Fatalf("foreign %T returned successful/provisional facts: zero=%t error=%v", owner, zero, err)
						}
					}
					cancelled, cancel := context.WithCancel(context.Background())
					cancel()
					if zero, err := read.read(cancelled, selected); !errors.Is(err, context.Canceled) || !zero {
						t.Fatalf("cancelled read returned facts: zero=%t error=%v", zero, err)
					}
				})
			}
			if err := selected.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			for _, read := range reads {
				if zero, err := read.read(context.Background(), selected); err == nil || !zero {
					t.Fatalf("closed %s read returned facts: zero=%t error=%v", read.name, zero, err)
				}
			}
		})
	}
}
