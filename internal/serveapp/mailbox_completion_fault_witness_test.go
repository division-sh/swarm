package serveapp

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
	modernsqlite "modernc.org/sqlite"
)

var mailboxCompletionFaultFunction struct {
	once     sync.Once
	err      error
	counters sync.Map
}

func mailboxCompletionRunEffects(t *testing.T, rt servedControlProofRuntime, runID string) []string {
	t.Helper()
	// Delivery settlement precedes the durable pipeline-to-continuation handoff.
	// Snapshot only after that owner has acknowledged the actual publication.
	waitPublicationSiteCompletion(t, rt, runID)
	var snapshot []string
	for _, query := range []string{
		`SELECT * FROM decision_cards WHERE run_id=$1 ORDER BY card_id`,
		`SELECT d.* FROM decision_card_input_drafts d JOIN decision_cards c ON c.card_id=d.card_id WHERE c.run_id=$1 ORDER BY d.input_draft_id`,
		`SELECT d.* FROM decision_card_changes d JOIN decision_cards c ON c.card_id=d.card_id WHERE c.run_id=$1 ORDER BY d.change_id`,
		`SELECT * FROM human_task_continuations WHERE run_id=$1 ORDER BY card_id`,
		`SELECT * FROM proposed_effect_continuations WHERE run_id=$1 ORDER BY card_id`,
		// Diagnostics are asynchronous observations, not mailbox domain effects.
		// Keep every execution/control event and every field, including timestamps.
		`SELECT * FROM events WHERE run_id=$1 AND event_class NOT IN ('runtime_diagnostic','diagnostic_direct') ORDER BY event_id`,
		`SELECT d.* FROM event_deliveries d JOIN events e ON e.event_id=d.event_id WHERE e.run_id=$1 ORDER BY d.delivery_id`,
		`SELECT * FROM entity_state WHERE run_id=$1 ORDER BY entity_id,flow_instance`,
		`SELECT * FROM flow_instances WHERE run_id=$1 ORDER BY instance_path`,
	} {
		rows, err := rt.DB.Query(query, runID)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			t.Fatal(err)
		}
		for rows.Next() {
			values := make([]any, len(columns))
			pointers := make([]any, len(columns))
			for i := range values {
				pointers[i] = &values[i]
			}
			if err := rows.Scan(pointers...); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			raw, err := json.Marshal(values)
			if err != nil {
				rows.Close()
				t.Fatal(err)
			}
			snapshot = append(snapshot, query+":"+string(raw))
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
	}
	return snapshot
}

func mailboxEffectsDifference(before, after []string) string {
	for i := 0; i < len(before) && i < len(after); i++ {
		if before[i] != after[i] {
			return fmt.Sprintf("first differing row %d:\nbefore=%s\nafter=%s", i, before[i], after[i])
		}
	}
	return fmt.Sprintf("snapshot lengths before=%d after=%d", len(before), len(after))
}

// Register before opening the SQLite fixture: functions are installed on each
// new connection. The counter survives rollback without writing domain state.
func requireMailboxCompletionFaultFunction(t *testing.T) {
	t.Helper()
	mailboxCompletionFaultFunction.once.Do(func() {
		mailboxCompletionFaultFunction.err = modernsqlite.RegisterScalarFunction("swarm_test_mailbox_completion_cut", 1, func(_ *modernsqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
			id, ok := args[0].(string)
			if !ok {
				return nil, fmt.Errorf("completion cut identity has type %T", args[0])
			}
			value, ok := mailboxCompletionFaultFunction.counters.Load(id)
			if !ok {
				return nil, fmt.Errorf("completion cut %s is not registered", id)
			}
			value.(*atomic.Int64).Add(1)
			return int64(1), nil
		})
	})
	if mailboxCompletionFaultFunction.err != nil {
		t.Fatal(mailboxCompletionFaultFunction.err)
	}
}

func installMailboxCompletionFaultWitness(t *testing.T, rt servedControlProofRuntime, key string) (assertReached func(), remove func()) {
	t.Helper()
	id := uuid.NewString()
	counter := &atomic.Int64{}
	mailboxCompletionFaultFunction.counters.Store(id, counter)
	t.Cleanup(func() { mailboxCompletionFaultFunction.counters.Delete(id) })
	var selected any = rt.SQLite
	if rt.Postgres != nil {
		selected = rt.Postgres
	}
	if err := storetest.SetMailboxCompletionInsertFault(context.Background(), selected, key, id, true); err != nil {
		t.Fatal(err)
	}
	var removeOnce sync.Once
	remove = func() {
		t.Helper()
		removeOnce.Do(func() {
			if err := storetest.SetMailboxCompletionInsertFault(context.Background(), selected, key, id, false); err != nil {
				t.Error(err)
			}
		})
	}
	t.Cleanup(remove)
	return func() {
		t.Helper()
		seen := counter.Load()
		if rt.Backend == "postgres" {
			var called bool
			if err := rt.DB.QueryRow(`SELECT last_value,is_called FROM mailbox_completion_cut_seen`).Scan(&seen, &called); err != nil {
				t.Fatal(err)
			}
			if !called {
				seen = 0
			}
		}
		if seen != 1 {
			t.Fatalf("expected exact response INSERT fault once, reached %d times", seen)
		}
	}, remove
}
