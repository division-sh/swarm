package runtimepersistence

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/google/uuid"
)

// Omit only the effect recovery scan to retain the pre-repair transaction split.
// Native takeover cells, immutable facts and drains are still authored by their
// real owners. History faults affect readback only; they write no rows.
type providerTakeoverHistoryConnector struct {
	driver.Connector
	omit    atomic.Bool
	omitted atomic.Int32
	reads   atomic.Int32
	fault   string
}

func (c *providerTakeoverHistoryConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &providerTakeoverHistoryConn{pipelineCrashConn: &pipelineCrashConn{Conn: conn}, control: c}, nil
}

type providerTakeoverHistoryConn struct {
	*pipelineCrashConn
	control *providerTakeoverHistoryConnector
}

func (c *providerTakeoverHistoryConn) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, options)
}

func (c *providerTakeoverHistoryConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := c.pipelineCrashConn.QueryContext(ctx, query, args)
	if err != nil {
		return nil, err
	}
	if c.control.omit.Load() && strings.Contains(query, "FROM runtime_external_effect_operations o") && strings.Contains(query, "o.effect_kind='provider_turn'") {
		c.control.omitted.Add(1)
		return &providerTakeoverHistoryRows{Rows: rows, fault: "missing"}, nil
	}
	if strings.Contains(query, "FROM agent_lifecycle_transition_facts f JOIN agent_lifecycle_operations o") {
		c.control.reads.Add(1)
		return &providerTakeoverHistoryRows{Rows: rows, fault: c.control.fault}, nil
	}
	return rows, nil
}

type providerTakeoverHistoryRows struct {
	driver.Rows
	fault string
}

func (r *providerTakeoverHistoryRows) Next(values []driver.Value) error {
	if r.fault == "missing" {
		return io.EOF
	}
	if r.fault == "read_error" {
		return errors.New("retained takeover history read diagnostic")
	}
	if err := r.Rows.Next(values); err != nil {
		return err
	}
	for i, column := range r.Columns() {
		if r.fault == "foreign_operation" && column == "operation_id" {
			values[i] = uuid.NewString()
		}
		if r.fault == "ordinary_restart" && column == "operation_kind" {
			values[i] = "restart"
		}
	}
	return nil
}

func TestProviderTakeoverRecoveryRetainedHistoryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, fault := range []string{"", "missing", "foreign_operation", "ordinary_restart", "read_error"} {
			name := fault
			if name == "" {
				name = "same_identity_chain"
			}
			t.Run(backend+"/"+name, func(t *testing.T) {
				control := &providerTakeoverHistoryConnector{}
				fixture, _ := openCompletionOutcomeFixture(t, backend, func(native driver.Connector) driver.Connector {
					control.Connector = native
					return control
				})
				attempts := makeProviderTakeoverAttempts(t, fixture)
				grant, request, process := prepareProviderTakeover(t, fixture, nil)
				control.omit.Store(true)
				for i := 0; i < 2; i++ {
					if i != 0 {
						grant, request, process = prepareProviderTakeover(t, fixture, process)
					}
					if _, err := grant.CommitAgentLifecycleTransition(testAuthorActivityContext(), request); err != nil {
						t.Fatalf("retain original takeover split: %v", err)
					}
				}
				control.omit.Store(false)
				if control.omitted.Load() != 2 {
					t.Fatalf("legacy split did not cross two original effect scans: %d", control.omitted.Load())
				}
				for _, attempt := range attempts[1:] {
					requireProviderDrainState(t, fixture, attempt, "pending")
					requireExternalAttemptState(t, fixture.db, fixture.sqlite, attempt, effects.StateLaunched)
				}
				grant, request, _ = prepareProviderTakeover(t, fixture, process)
				before, err := ReadSelectedForkApplicationStorageSnapshotForTest(testAuthorActivityContext(), fixture.store)
				if err != nil {
					t.Fatal(err)
				}
				control.fault = fault
				_, recoveryErr := grant.CommitAgentLifecycleTransition(testAuthorActivityContext(), request)
				control.fault = ""
				if control.reads.Load() == 0 {
					t.Fatal("retained-chain validation was not reached")
				}
				after, err := ReadSelectedForkApplicationStorageSnapshotForTest(testAuthorActivityContext(), fixture.store)
				if err != nil {
					t.Fatal(err)
				}
				if fault != "" {
					if recoveryErr == nil || !reflect.DeepEqual(before, after) {
						t.Fatalf("invalid retained history escaped refusal/rollback: error=%v", recoveryErr)
					}
					return
				}
				if recoveryErr != nil {
					t.Fatalf("recover exact retained takeover chain: %v", recoveryErr)
				}
				for _, attempt := range attempts[1:] {
					requireProviderDrainState(t, fixture, attempt, "settled")
					requireCompletionRecoveryRows(t, fixture, attempt, 1, 1, 0)
				}
				if !reflect.DeepEqual(before["runtime_provider_attempt_drains"].Columns, after["runtime_provider_attempt_drains"].Columns) {
					t.Fatal("retained chain recovery changed the native drain shape")
				}
			})
		}
	}
}
