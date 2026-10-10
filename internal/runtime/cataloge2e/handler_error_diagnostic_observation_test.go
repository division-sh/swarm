package cataloge2e

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestCatalogHandlerErrorDiagnosticPreservesLatestPhysicalFilterBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			h := newRuntimeHarnessForBackend(t, canonicalrouting.CopyReceiverOptionalChild(t, false), backend, false)
			ctx := h.ctx
			var selected any = h.pg
			var writer runtimepkg.RuntimeLogPersistence = h.pg
			closeOwner := h.pg.Close
			if h.sqlite != nil {
				selected, writer, closeOwner = h.sqlite, h.sqlite, h.sqlite.Close
			}
			reader, err := h.catalogOperatorEventLister()
			if err != nil {
				t.Fatal(err)
			}
			if payload, err := storetest.ReadLatestHandlerErrorLog(ctx, reader); err != nil || payload != nil {
				t.Fatalf("missing diagnostic changed: %s %v", payload, err)
			}
			base := time.Now().UTC().Truncate(time.Microsecond).Add(-time.Minute)
			admit := runtimepkg.NewRuntimePayloadAdmitter(nil, semanticview.Wrap(h.bundle), catalogSourceArtifactFact(t, h.bundle))
			var want []byte
			for i, entry := range []runtimepkg.RuntimeLogEntry{
				{Level: diaglog.LevelError, Component: "catalog", Action: "handler_error", Message: "old failure"},
				{Level: diaglog.LevelError, Component: "catalog", Action: "handler_error", Message: "latest selected failure"},
				{Level: diaglog.LevelInfo, Component: "catalog", Action: "other", Message: "new unrelated log"},
			} {
				record, _, err := runtimepkg.EncodeRuntimeLogRecord(runtimepkg.RuntimeLogFacts{EventID: uuid.NewString(), CreatedAt: base.Add(time.Duration(i) * time.Second), ExecutionMode: executionmode.Live}, entry)
				if err != nil {
					t.Fatal(err)
				}
				event := eventtest.DiagnosticDirect(record.EventID, events.EventTypePlatformRuntimeLog, "runtime", "", record.Payload, 0, "", "", events.EventEnvelope{}, record.CreatedAt)
				admission, err := admit(ctx, event, "")
				if err != nil {
					t.Fatal(err)
				}
				record.Payload, record.PayloadAdmission = admission.Payload(), admission
				if err := writer.PersistRuntimeLog(ctx, record); err != nil {
					t.Fatal(err)
				}
				if i == 1 {
					want = record.Payload
				}
			}
			h.shutdown()
			ctx = testAuthorActivityContext(context.Background())
			probe := storetest.CollectTransactions(t, selected, storetest.TransactionProbeOptions{})
			payload, err := storetest.ReadLatestHandlerErrorLog(ctx, reader)
			if err != nil {
				t.Fatal(err)
			}
			gotCanonical, err := canonicalJSONBytes(payload)
			wantCanonical, wantErr := canonicalJSONBytes(want)
			if err != nil || wantErr != nil || string(gotCanonical) != string(wantCanonical) {
				t.Fatalf("latest/action/type physical selection changed: %s err=%v / %v", payload, err, wantErr)
			}
			if proof := probe.Snapshot(); proof.Total.ReadCommits != 1 || proof.Total.WriteCommits != 0 || proof.Active != 0 {
				t.Fatalf("diagnostic escaped native read ownership: %+v", proof)
			}
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			if payload, err := storetest.ReadLatestHandlerErrorLog(canceled, reader); !errors.Is(err, context.Canceled) || payload != nil {
				t.Fatalf("canceled diagnostic returned evidence: %s %v", payload, err)
			}
			if err := closeOwner(); err != nil {
				t.Fatal(err)
			}
			if payload, err := storetest.ReadLatestHandlerErrorLog(ctx, reader); err == nil || payload != nil {
				t.Fatalf("closed diagnostic returned evidence: %s %v", payload, err)
			}
		})
	}
	if payload, err := storetest.ReadLatestHandlerErrorLog(context.Background(), struct{}{}); err == nil || payload != nil {
		t.Fatalf("foreign diagnostic owner returned evidence: %s %v", payload, err)
	}
}
