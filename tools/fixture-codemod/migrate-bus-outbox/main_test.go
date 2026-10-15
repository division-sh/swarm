package main

import (
	"bytes"
	"strings"
	"testing"
)

const descriptorCaller = `package bus_test
func TestEngineOutboxDistinctIntentBatchDispatchesEachOnce(t *testing.T) {
 db, mock, err := sqlmock.New()
 if err != nil { t.Fatal(err) }
 defer db.Close()
 store := &directRecipientTransactionalStore{descriptors: descriptors}
 eb, err := newScopedTestEventBus(store)
 if err != nil { t.Fatal(err) }
 mock.ExpectBegin()
 mock.ExpectCommit()
 tx, err := db.Begin()
 if err != nil { t.Fatal(err) }
 ctx := runtimepipelinefixture.WithSQLTx(context.Background(), tx)
 intent := eventtest.RunCreatingRootIngress(id, kind, agent, task, payload, 0, run, "", envelope, at)
 if err := commitEnginePublicationsForTest(ctx, eb, store, []runtimeengine.EmitIntent{intent}); err != nil { t.Fatal(err) }
 if err := tx.Commit(); err != nil { t.Fatal(err) }
 if err := eb.EngineDispatcher().DispatchPostCommit(context.Background(), intents); err != nil { t.Fatal(err) }
 if got := len(store.events); got != 2 { t.Fatalf("persisted events = %d, want 2", got) }
 assertExactOrder(t, intents)
 requireNoBusEvent(t, receiver, "distinct intent over-dispatch")
 if err := mock.ExpectationsWereMet(); err != nil { t.Fatal(err) }
}
func TestUnrelated(t *testing.T) { preservePureRecorder(t) }
`

func TestFiniteOutboxTransformPreservesOraclesAndReplays(t *testing.T) {
	output, err := rewrite([]byte(descriptorCaller))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`[]string{"sqlite", "postgres"}`, `newNativeOutboxFixture(t, backend, descriptors...)`,
		`fixture.commitError([]runtimeengine.EmitIntent{intent})`,
		`DispatchPostCommit(fixture.ctx, intents)`, `fixture.eventCount(t)`,
		`got != 2`, `assertExactOrder(t, intents)`, `distinct intent over-dispatch`,
		`eventtest.ExistingRunRootIngress(id, kind, agent, task, payload, 0, run, envelope, at)`,
		`func TestUnrelated(t *testing.T) { preservePureRecorder(t) }`,
	} {
		if !strings.Contains(string(output), want) {
			t.Errorf("output lost %q", want)
		}
	}
	for _, retired := range []string{"sqlmock.", "tx.Commit", "WithSQLTx", "commitEnginePublicationsForTest", "directRecipientTransactionalStore"} {
		if strings.Contains(string(output), retired) {
			t.Errorf("output retains %s", retired)
		}
	}
	replayed, err := rewrite(output)
	if err != nil || !bytes.Equal(output, replayed) {
		t.Fatalf("replay changed output: %v", err)
	}
}

func TestFiniteOutboxTransformRefusesSemanticFixtureFields(t *testing.T) {
	for _, field := range []string{"events: seededEvents", "deadLetterErr: fault", "targetOwners: targets", "routes: routes"} {
		source := strings.Replace(descriptorCaller, "descriptors: descriptors", field, 1)
		if _, err := rewrite([]byte(source)); err == nil {
			t.Errorf("accepted semantic fixture field %s", field)
		}
	}
}
