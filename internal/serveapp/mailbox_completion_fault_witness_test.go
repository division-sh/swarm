package serveapp

import (
	"context"
	"database/sql/driver"
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

func mailboxCompletionRunEffects(t *testing.T, reader receiverProofStateReader, runID string) []string {
	t.Helper()
	// Delivery settlement precedes the durable pipeline-to-continuation handoff.
	// Snapshot only after that owner has acknowledged the actual publication.
	waitPublicationSiteCompletion(t, reader, runID)
	var snapshot []string
	err := reader.InspectSnapshot(context.Background(), func(ctx context.Context) error {
		var err error
		snapshot, err = storetest.ReadMailboxCompletionRunEffects(ctx, reader, runID)
		return err
	})
	if err != nil {
		t.Fatal(err)
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
