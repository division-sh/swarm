package storetest

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestRevisionedExistingRunRootFixturePreservesCanonicalEvidence(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) RunFixtureStore
	}{
		{"sqlite", func(t *testing.T) RunFixtureStore { return StartSQLiteRuntimeStore(t) }},
		{"postgres", func(t *testing.T) RunFixtureStore { return StartPostgresRuntimeStore(t) }},
	} {
		t.Run(backend.name, func(t *testing.T) {
			ctx := semanticFixtureContext(context.Background(), sourceartifactfixture.Fact())
			selected := backend.open(t)
			runID, eventID := uuid.NewString(), uuid.NewString()
			at := time.Now().UTC().Truncate(time.Microsecond)
			RequireRun(t, ctx, selected, RunFixture{RunID: runID, Origin: ScenarioSetupOrigin(), StartedAt: at.Add(-time.Minute)})
			producer := eventtest.Producer(events.EventProducerExternal, "revision-proof")
			payload := []byte(`{"value":1}`)
			before := ReadSemanticEventFixtureEvidence(t, ctx, selected, runID, eventID)
			written := InsertRevisionedExistingRunRootEventRecord(t, ctx, selected, eventID, runID, "fixture.ready", producer, payload, events.EventEnvelope{}, at)
			admitted := AdmitNativeDeliveryEvent(t, written)
			expected, err := eventrecord.FromAdmitted(admitted, canonicalFixtureSettlement(t, admitted.Event(), nil))
			if err != nil {
				t.Fatal(err)
			}
			after := ReadSemanticEventFixtureEvidence(t, ctx, selected, runID, eventID)
			if !after.Record.Equal(expected) {
				t.Fatalf("selected event differs from the complete canonical existing-run record: got=%#v want=%#v", after.Record, expected)
			}
			if !after.RecordFound || after.RevisionCount != before.RevisionCount+1 || after.RunEventCount != before.RunEventCount+1 || len(after.DeliveryProjections) != 0 || after.PipelineReceiptCount != 0 {
				t.Fatalf("event must capture one revision/counter delta without delivery work: before=%+v after=%+v", before, after)
			}
			InsertRevisionedExistingRunRootEventRecord(t, ctx, selected, eventID, runID, "fixture.ready", producer, payload, events.EventEnvelope{}, at)
			if duplicate := ReadSemanticEventFixtureEvidence(t, ctx, selected, runID, eventID); !reflect.DeepEqual(duplicate, after) {
				t.Fatalf("exact duplicate changed canonical event/history evidence: before=%+v after=%+v", after, duplicate)
			}
			if _, err := private.InsertRevisionedExistingRunRootEventForTest(ctx, selected, eventID, runID, "fixture.ready", producer, []byte(`{"value":2}`), events.EventEnvelope{}, at); err == nil {
				t.Fatal("conflicting duplicate was accepted")
			}
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			canceledID, missingRunID := uuid.NewString(), uuid.NewString()
			if _, err := private.InsertRevisionedExistingRunRootEventForTest(canceled, selected, canceledID, runID, "fixture.ready", producer, payload, events.EventEnvelope{}, at); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled fixture error=%v want caller cancellation", err)
			}
			if _, err := private.InsertRevisionedExistingRunRootEventForTest(ctx, selected, missingRunID, uuid.NewString(), "fixture.ready", producer, payload, events.EventEnvelope{}, at); err == nil {
				t.Fatal("existing-run fixture created an event for an absent run")
			}
			if _, err := private.InsertRevisionedExistingRunRootEventForTest(ctx, struct{}{}, eventID, runID, "fixture.ready", producer, payload, events.EventEnvelope{}, at); err == nil {
				t.Fatal("event fixture accepted a non-native owner")
			}
			for _, id := range []string{canceledID, missingRunID} {
				if _, found, err := ReadCanonicalEventRecord(ctx, selected, id); err != nil || found {
					t.Fatalf("refused fixture left an event: id=%s found=%v err=%v", id, found, err)
				}
			}
			if refused := ReadSemanticEventFixtureEvidence(t, ctx, selected, runID, eventID); !reflect.DeepEqual(refused, after) {
				t.Fatalf("refused writes changed canonical evidence: before=%+v after=%+v", after, refused)
			}
		})
	}
}
