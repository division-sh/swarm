package sessionprovider

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	runtimeinbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	"github.com/google/uuid"
)

func TestWhatsAppCaptureRedeliveryAfterRetirementUsesHistoricalEvidence(t *testing.T) {
	event := captureFixture(t)
	request := capturePublicationFixture(t, event)
	path := filepath.Join(t.TempDir(), "incoming.db")
	db, spool := openCaptureFixture(t, path, event.Scope.Session.ConnectionID)
	ctx := context.Background()
	if err := spool.capture(ctx, event); err != nil {
		t.Fatal(err)
	}
	if err := spool.stagePublication(ctx, event, request); err != nil {
		t.Fatal(err)
	}
	reader := publishedCaptureFixture(request)
	if err := spool.retirePublished(ctx, event, reader); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	_, spool = openCaptureFixture(t, path, event.Scope.Session.ConnectionID)
	for _, fresh := range []bool{false, true, true} {
		duplicate := event
		if fresh {
			duplicate.OccurrenceID = uuid.NewString()
			duplicate.ReceivedAt = event.ReceivedAt.Add(time.Hour)
		}
		if err := spool.capture(ctx, duplicate); err != nil {
			t.Fatal(err)
		}
		if fresh {
			if err := spool.stagePublication(ctx, duplicate, request); !errors.Is(err, runtimeinbound.ErrRequestIdentityConflict) {
				t.Fatal("unverified staging adopted another occurrence", err)
			}
		}
		settled, err := spool.reconcilePublished(ctx, duplicate, reader)
		if err != nil || !settled {
			t.Fatalf("identical redelivery failed historical reconciliation: %t %v", settled, err)
		}
		pending, err := spool.pending(ctx)
		if err != nil || len(pending) != 0 {
			t.Fatal("historical duplicate remained stranded", err)
		}
	}
	original, err := publicationCaptureProvenance(reader.record.Request)
	if err != nil || !original.SameCapture(event) {
		t.Fatal("historical reconciliation overwrote original provenance", err)
	}
}

func TestWhatsAppHistoricalCaptureRedeliveryRejectsChangedStableEvidence(t *testing.T) {
	for _, cell := range []struct {
		name string
		edit func(*capturedEvent)
	}{
		{"body", func(e *capturedEvent) { e.Body = []byte(`{"text":"changed sender or body"}`) }},
		{"connection", func(e *capturedEvent) { e.Scope.Session.ConnectionID = uuid.NewString() }},
		{"account", func(e *capturedEvent) { e.Scope.Session.AccountRef = "other" }},
		{"admission", func(e *capturedEvent) { e.Scope.Session.AdmissionID = uuid.NewString() }},
		{"admission_revision", func(e *capturedEvent) { e.Scope.Session.Revision++ }},
		{"service", func(e *capturedEvent) { e.Scope.PublicationBinding.ServiceID = uuid.NewString() }},
		{"run", func(e *capturedEvent) { e.Scope.PublicationBinding.RunID = uuid.NewString() }},
		{"generation", func(e *capturedEvent) {
			e.Scope.PublicationBinding.Generation++
			e.Source.Coordinate.TargetGeneration++
		}},
		{"source", func(e *capturedEvent) {
			e.Scope.Source.BundleIdentity = "other"
			e.Source.Coordinate.BundleIdentity = "other"
		}},
		{"principal", func(e *capturedEvent) { e.Scope.PrincipalID = uuid.NewString() }},
		{"operation", func(e *capturedEvent) { e.Scope.OnboardingOperation = uuid.NewString() }},
		{"activation", func(e *capturedEvent) { e.Scope.ActivationID = uuid.NewString() }},
		{"binding", func(e *capturedEvent) { e.Scope.BindingRevision++ }},
	} {
		t.Run(cell.name, func(t *testing.T) {
			original := captureFixture(t)
			request := capturePublicationFixture(t, original)
			duplicate := original
			duplicate.OccurrenceID = uuid.NewString()
			cell.edit(&duplicate)
			_, spool := openCaptureFixture(t, filepath.Join(t.TempDir(), "incoming.db"), duplicate.Scope.Session.ConnectionID)
			ctx := context.Background()
			if err := spool.capture(ctx, duplicate); err != nil {
				t.Fatal(err)
			}
			settled, err := spool.reconcilePublished(ctx, duplicate, publishedCaptureFixture(request))
			if err == nil || settled {
				t.Fatalf("changed stable evidence adopted historical authority: settled=%t err=%v", settled, err)
			}
			pending, err := spool.pending(ctx)
			if err != nil || len(pending) != 1 || !pending[0].SameCapture(duplicate) {
				t.Fatal("refusal discarded changed capture evidence", err)
			}
		})
	}
}

func TestWhatsAppHistoricalCaptureRejectsMalformedRedelivery(t *testing.T) {
	for _, change := range []string{"operation_revision", "activation_revision", "activation_id", "occurrence", "target_generation"} {
		t.Run(change, func(t *testing.T) {
			original := captureFixture(t)
			request := capturePublicationFixture(t, original)
			duplicate := original
			switch change {
			case "operation_revision":
				duplicate.Scope.OperationRevision = 0
			case "activation_revision":
				duplicate.Scope.ActivationRevision = 0
			case "activation_id":
				duplicate.Scope.ActivationID = ""
			case "occurrence":
				duplicate.OccurrenceID = ""
			case "target_generation":
				duplicate.Source.Coordinate.TargetGeneration = 0
			}
			if _, err := verifyHistoricalCapture(duplicate, publishedCaptureFixture(request).record); err == nil {
				t.Fatal("stable historical comparison admitted malformed capture")
			}
		})
	}
}
