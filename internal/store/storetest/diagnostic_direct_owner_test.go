package storetest

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestStandaloneDiagnosticFixtureUsesOriginalNamedWriterBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) any
	}{
		{"sqlite", func(t *testing.T) any { return StartSQLiteRuntimeStore(t) }},
		{"postgres", func(t *testing.T) any { return StartPostgresRuntimeStore(t) }},
	} {
		t.Run(backend.name, func(t *testing.T) {
			selected := backend.open(t)
			ctx := semanticFixtureContext(context.Background(), sourceartifactfixture.Fact())
			probe := CollectTransactions(t, selected, TransactionProbeOptions{})
			at := time.Date(2026, 7, 7, 12, 0, 0, 123000000, time.UTC)
			payload := []byte(`{"log_level":"error","message":"reader corruption control","details":"not-an-object"}`)
			id := uuid.NewString()
			event := InsertDiagnosticDirectEventRecord(t, ctx, selected, id, payload, at)
			if counts := probe.Snapshot(); counts.Total.WriteCommits != 1 || counts.Active != 0 {
				t.Fatalf("diagnostic fixture bypassed original write coordinator: %+v", counts)
			}
			stored, found, err := ReadCanonicalEventRecord(ctx, selected, id)
			if err != nil || !found || stored.ID() != event.ID() || stored.AdmissionClass() != events.EventAdmissionDiagnosticDirect ||
				stored.Type() != events.EventTypePlatformRuntimeLog || stored.RunID() != "" || stored.ParentEventID() != "" || stored.Envelope().Scope != events.EventScopeGlobal ||
				stored.ProducerType() != events.EventProducerPlatform || stored.Producer().ID() != "runtime" || stored.ExecutionMode() != executionmode.Live ||
				!stored.CreatedAt().Equal(at) || !bytes.Equal(stored.Payload(), payload) {
				t.Fatalf("diagnostic fixture lost exact subtype/payload/clock/identity: %+v found=%v err=%v", stored, found, err)
			}
			counts := ObserveActivityResultPublicationStorage(t, ctx, selected)
			if counts.Events != 1 || counts.Runs != 0 || counts.Deliveries != 0 || counts.Entities != 0 || counts.Receipts != 0 {
				t.Fatalf("standalone diagnostic fabricated execution or run: %+v", counts)
			}
			InsertDiagnosticDirectEventRecord(t, ctx, selected, id, payload, at)
			if after := ObserveActivityResultPublicationStorage(t, ctx, selected); after != counts {
				t.Fatalf("exact diagnostic replay changed physical cardinalities: %+v -> %+v", counts, after)
			}
		})
	}
}
