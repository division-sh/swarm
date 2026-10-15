package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

// This physical-entry control uses a unit session, not an admitted delivery or
// a native recovery image. Real full/room claims are proved separately.
type capacityEntryUnitSession struct {
	*sqliteFreshnessUnitSession
	backend *Backend
	domains atomic.Int32
	failure error
}

func (s *capacityEntryUnitSession) ClaimAgentRecoveryDelivery(ctx context.Context, authority deliverylifecycle.ExecutionAuthority, event events.Event, route events.DeliveryRoute) (deliverylifecycle.ClaimResult, error) {
	bound, err := runtimeownership.BindAgentRecoveryClaimIntent(ctx, s.writes, authority, event, route)
	if err != nil {
		return deliverylifecycle.ClaimResult{}, err
	}
	ack, err := s.backend.RunTransactionOutcome(bound, "unit capacity physical entry", func(context.Context, *sql.Tx) error {
		s.domains.Add(1)
		return s.failure
	})
	return deliverylifecycle.ClaimResult{Acknowledged: ack}, err
}

func (s *capacityEntryUnitSession) TryClaimAgentRecoveryDelivery(ctx context.Context, authority deliverylifecycle.ExecutionAuthority, event events.Event, route events.DeliveryRoute) (deliverylifecycle.ClaimResult, bool, error) {
	result, err := s.ClaimAgentRecoveryDelivery(ctx, authority, event, route)
	return result, false, err
}

type capacityBusyEntryConnector struct {
	*exitFaultConnector
	entered, release chan struct{}
	begins           atomic.Int32
}

func (c *capacityBusyEntryConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.exitFaultConnector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &capacityBusyEntryConn{Conn: conn, owner: c}, nil
}

type capacityBusyEntryConn struct {
	driver.Conn
	owner *capacityBusyEntryConnector
}

func (c *capacityBusyEntryConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.owner.begins.Add(1) == 1 {
		close(c.owner.entered)
		<-c.owner.release
		return nil, errors.New("database is locked: held original pre-domain BEGIN")
	}
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
}

func TestSQLiteCapacityInnerRefusesBusyReentryBeforePoolUnit(t *testing.T) {
	for _, capacity := range []bool{true, false} {
		name := "freshness_only_control"
		if capacity {
			name = "capacity_one_shot"
		}
		t.Run(name, func(t *testing.T) {
			control := &capacityBusyEntryConnector{exitFaultConnector: &exitFaultConnector{path: filepath.Join(t.TempDir(), "capacity-entry.db")}, entered: make(chan struct{}), release: make(chan struct{})}
			db := sql.OpenDB(control)
			// Every pool acquisition opens a physical connection, making even a
			// rejected second acquisition observable rather than a reused handle.
			db.SetMaxIdleConns(0)
			t.Cleanup(func() { _ = db.Close() })
			backend, err := New(db)
			if err != nil {
				t.Fatal(err)
			}
			session, process := newCapacityEntryUnitProcess(t, backend)
			event, route, execution := capacityEntryUnitCommand(t)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				var err error
				if capacity {
					_, err = runtimeownership.TryClaimAgentRecoveryDelivery(ctx, process, execution, event, route)
				} else {
					_, err = runtimeownership.ClaimAgentRecoveryDelivery(ctx, process, execution, event, route)
				}
				done <- err
			}()
			var once sync.Once
			unblock := func() { once.Do(func() { close(control.release) }) }
			joined := false
			t.Cleanup(func() {
				unblock()
				if !joined {
					<-done
				}
			})
			select {
			case <-control.entered:
			case <-ctx.Done():
				t.Fatal("original physical BEGIN never reached held cut")
			}
			if err := runtimeownership.SeedAgentRecovery(ctx, process); err == nil {
				t.Fatal("held physical entry allowed seed reconstruction")
			}
			unblock()
			err = <-done
			joined = true
			wantOpens, wantDomains := int32(2), int32(1)
			if capacity {
				wantOpens, wantDomains = 1, 0
				if err == nil || !strings.Contains(err.Error(), "complete fresh reconstruction") {
					t.Fatalf("capacity reentry did not refuse at original hook: %v", err)
				}
			} else if err != nil {
				t.Fatalf("freshness-only busy recovery changed: %v", err)
			}
			if control.opens.Load() != wantOpens || control.begins.Load() != wantOpens || session.domains.Load() != wantDomains || control.closes.Load() != wantOpens {
				t.Fatalf("unexpected native entry: opens=%d begins=%d domains=%d closes=%d", control.opens.Load(), control.begins.Load(), session.domains.Load(), control.closes.Load())
			}
			if err := runtimeownership.SeedAgentRecovery(ctx, process); err != nil {
				t.Fatalf("original reverse cleanup left an open observation: %v", err)
			}
		})
	}
}

func newCapacityEntryUnitProcess(t *testing.T, backend *Backend) (*capacityEntryUnitSession, runtimeownership.ProcessCapability) {
	t.Helper()
	plan, err := agenttopology.EmptySourceSetPlan()
	if err != nil {
		t.Fatal(err)
	}
	session := &capacityEntryUnitSession{backend: backend, sqliteFreshnessUnitSession: &sqliteFreshnessUnitSession{writes: backend.AgentRecoveryWriteObserver(), value: runtimeownership.AgentRecoverySeedRead{
		Process:    runtimeownership.Authority{AuthorityID: uuid.NewString(), AuthorityGeneration: 1, OwnerID: "capacity-entry-unit", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString(), Backend: processbinding.SQLiteLeaseBackend},
		SourceHead: plan, InProgressByProfile: map[string]int{},
	}}}
	process, err := runtimeownership.NewProcessCapability(session)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = process.Release(context.Background()) })
	cfg := &config.Config{LLM: config.LLMConfig{Backend: "anthropic", Session: config.LLMSessionConfig{LockTTL: time.Second, RotateAfterTurns: 1, RotateOnParseFailures: 1}}}
	if err := runtimeownership.BindAgentRecoveryProfilePolicy(t.Context(), process, cfg); err != nil {
		t.Fatal(err)
	}
	if err := runtimeownership.SeedAgentRecovery(t.Context(), process); err != nil {
		t.Fatal(err)
	}
	return session, process
}

func TestSQLiteCapacityPairJoinsOriginalRollbackCleanupUnit(t *testing.T) {
	control := &exitFaultConnector{path: filepath.Join(t.TempDir(), "capacity-cleanup.db"), rollbackEntered: make(chan struct{}), rollbackRelease: make(chan struct{})}
	db := sql.OpenDB(control)
	t.Cleanup(func() { _ = db.Close() })
	backend, err := New(db)
	if err != nil {
		t.Fatal(err)
	}
	session, process := newCapacityEntryUnitProcess(t, backend)
	session.failure = errors.New("held original capacity rollback")
	event, route, authority := capacityEntryUnitCommand(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := runtimeownership.TryClaimAgentRecoveryDelivery(ctx, process, authority, event, route)
		done <- err
	}()
	var once sync.Once
	unblock := func() { once.Do(func() { close(control.rollbackRelease) }) }
	joined := false
	t.Cleanup(func() {
		unblock()
		if !joined {
			<-done
		}
	})
	select {
	case <-control.rollbackEntered:
	case <-ctx.Done():
		t.Fatal("original capacity rollback did not reach held cleanup")
	}
	if err := runtimeownership.SeedAgentRecovery(ctx, process); err == nil {
		t.Fatal("paired observations ended before original rollback cleanup")
	}
	select {
	case err := <-done:
		joined = true
		t.Fatalf("capacity entry returned before original cleanup joined: %v", err)
	default:
	}
	unblock()
	err = <-done
	joined = true
	if !errors.Is(err, session.failure) || session.domains.Load() != 1 {
		t.Fatalf("pair changed original callback/rollback result: %v", err)
	}
	if err := runtimeownership.SeedAgentRecovery(ctx, process); err != nil {
		t.Fatalf("original cleanup left paired observations active: %v", err)
	}
}

func capacityEntryUnitCommand(t *testing.T) (events.Event, events.DeliveryRoute, deliverylifecycle.ExecutionAuthority) {
	t.Helper()
	runID := uuid.NewString()
	name, err := agentidentity.DeclaredName("capacity-unit", "entry-proof")
	if err != nil {
		t.Fatal(err)
	}
	instance, err := agentidentity.PresentRoute("global", uuid.NewString(), "global")
	if err != nil {
		t.Fatal(err)
	}
	identity, err := agentidentity.New(runID, name, instance)
	if err != nil {
		t.Fatal(err)
	}
	route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(name.AgentID), AgentIdentity: identity, Target: events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: "global", FlowInstance: identity.FlowInstance()})}
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), "test.capacity_entry", "operator", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
	authority, err := deliverylifecycle.NewNormalExecutionAuthority(sourceartifactfixture.Fact(), uuid.NewString(), 1)
	if err != nil {
		t.Fatal(err)
	}
	return event, route, authority
}
