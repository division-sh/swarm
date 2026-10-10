package sessionprovider

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/store/sessionstate"

	"github.com/google/uuid"
)

func captureStoredRowsFixture(t *testing.T, fixture *sessionstate.Fixture) []string {
	t.Helper()
	rows, err := fixture.CaptureStoredRows(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestWhatsAppPrivateStateFixtureRefusesUnlistedCuts(t *testing.T) {
	event := captureFixture(t)
	fixture, capture := openCaptureFixture(t, filepath.Join(t.TempDir(), "incoming.db"), event.Scope.Session.ConnectionID)
	if err := capture.capture(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	before := captureStoredRowsFixture(t, fixture)
	if err := fixture.CorruptCaptureIndex(context.Background(), sessionstate.CaptureCorruption("arbitrary-column")); err == nil {
		t.Fatal("unlisted corruption admitted")
	}
	if err := fixture.SetCaptureFault(context.Background(), sessionstate.CaptureFault("arbitrary-table"), true); err == nil {
		t.Fatal("unlisted fault admitted")
	}
	if after := captureStoredRowsFixture(t, fixture); !reflect.DeepEqual(before, after) {
		t.Fatal("invalid fixture operation mutated evidence")
	}
}

func TestWhatsAppCaptureAdmissionCannotAcknowledgeCorruptStoredRows(t *testing.T) {
	for _, mutation := range []struct {
		name  string
		query sessionstate.CaptureCorruption
	}{
		{"connection", sessionstate.CorruptConnection},
		{"account", sessionstate.CorruptAccount},
		{"conversation", sessionstate.CorruptConversation},
		{"event", sessionstate.CorruptEvent},
		{"kind", sessionstate.CorruptKind},
		{"byte_count", sessionstate.CorruptByteCount},
		{"envelope", sessionstate.CorruptEnvelope},
		{"digest", sessionstate.CorruptDigest},
	} {
		for _, consumer := range []string{"redelivery", "new_event_quota"} {
			t.Run(mutation.name+"/"+consumer, func(t *testing.T) {
				event := captureFixture(t)
				db, capture := openCaptureFixture(t, filepath.Join(t.TempDir(), "incoming.db"), event.Scope.Session.ConnectionID)
				if err := capture.capture(context.Background(), event); err != nil {
					t.Fatal(err)
				}
				if err := db.CorruptCaptureIndex(context.Background(), mutation.query); err != nil {
					t.Fatal(err)
				}
				if _, err := capture.pending(context.Background()); err == nil {
					t.Fatal("fixture did not produce corrupt recovery evidence")
				}
				if consumer == "new_event_quota" {
					event.EventID = "new_event"
				} else if mutation.name == "conversation" {
					// The reviewer reproduced a false ACK by selecting the tampered
					// routing index while the envelope still named the old conversation.
					event.Conversation = "another_conversation"
				}
				before := captureStoredRowsFixture(t, db)
				guard, err := newCallbackGuard(context.Background(), event.Scope.Session.ConnectionID, event.OccurrenceID,
					func(ctx context.Context, _ any) error { return capture.capture(ctx, event) }, capture.recordFailure)
				if err != nil {
					t.Fatal(err)
				}
				if guard.receive(event) {
					t.Fatal("corrupt capture evidence granted local successful acknowledgment")
				}
				if after := captureStoredRowsFixture(t, db); !reflect.DeepEqual(before, after) {
					t.Fatal("refusal rewrote or discarded original capture evidence")
				}
			})
		}
	}
}

func TestWhatsAppCaptureValidRedeliveryRetainsOriginalOccurrence(t *testing.T) {
	event := captureFixture(t)
	db, capture := openCaptureFixture(t, filepath.Join(t.TempDir(), "incoming.db"), event.Scope.Session.ConnectionID)
	if err := capture.capture(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	before := captureStoredRowsFixture(t, db)
	redelivery := event
	redelivery.OccurrenceID = uuid.NewString()
	redelivery.ReceivedAt = event.ReceivedAt.Add(time.Hour)
	if err := capture.capture(context.Background(), redelivery); err != nil {
		t.Fatal(err)
	}
	if after := captureStoredRowsFixture(t, db); !reflect.DeepEqual(before, after) {
		t.Fatal("valid redelivery changed frozen original evidence")
	}
	pending, err := capture.pending(context.Background())
	if err != nil || len(pending) != 1 || !reflect.DeepEqual(pending[0], event) {
		t.Fatalf("original capture lost on new-occurrence redelivery: %v %v", pending, err)
	}
}

func TestWhatsAppCaptureDuplicateScopeBodyAndEnvelopeValidation(t *testing.T) {
	for _, dimension := range []string{"body", "principal", "source", "admission", "revision", "responsibility", "invalid_envelope"} {
		t.Run(dimension, func(t *testing.T) {
			event := captureFixture(t)
			db, capture := openCaptureFixture(t, filepath.Join(t.TempDir(), "incoming.db"), event.Scope.Session.ConnectionID)
			if err := capture.capture(context.Background(), event); err != nil {
				t.Fatal(err)
			}
			candidate := event
			candidate.OccurrenceID = uuid.NewString()
			switch dimension {
			case "body":
				candidate.Body = []byte(`{"text":"different capture"}`)
			case "principal":
				candidate.Scope.PrincipalID = uuid.NewString()
			case "source":
				candidate.Scope.Source.BundleIdentity = "source:other"
				candidate.Source.Coordinate.BundleIdentity = "source:other"
			case "admission":
				candidate.Scope.Session.AdmissionID = uuid.NewString()
			case "revision":
				candidate.Scope.BindingRevision++
			case "responsibility":
				candidate.Scope.OnboardingOperation = uuid.NewString()
			case "invalid_envelope":
				raw, err := json.Marshal(event)
				if err != nil {
					t.Fatal(err)
				}
				raw = append(raw[:len(raw)-1], []byte(`,"UnknownAuthority":true}`)...)
				digest := sha256.Sum256(raw)
				if err := db.ReplaceCaptureEnvelope(context.Background(), raw, digest[:]); err != nil {
					t.Fatal(err)
				}
			}
			before := captureStoredRowsFixture(t, db)
			err := capture.capture(context.Background(), candidate)
			if err == nil || dimension != "invalid_envelope" && !errors.Is(err, errCaptureConflict) {
				t.Fatalf("contradictory capture evidence acknowledged: %v", err)
			}
			if after := captureStoredRowsFixture(t, db); !reflect.DeepEqual(before, after) {
				t.Fatal("capture refusal replaced immutable evidence")
			}
		})
	}
}
