package whatsapp

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeinbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	"github.com/division-sh/swarm/internal/runtime/plangeneration"
	"github.com/google/uuid"
)

func captureFixture(t *testing.T) capturedEvent {
	t.Helper()
	generation, err := plangeneration.FromCanonicalValue(map[string]string{"test": "capture"})
	if err != nil {
		t.Fatal(err)
	}
	return capturedEvent{
		Scope: captureScope{
			Kind: channelonboarding.SessionInputBusiness,
			PublicationBinding: runtimeinbound.BindingGeneration{ServiceID: runtimeflowidentity.StandingServiceID("."),
				RunID: uuid.NewString(), Generation: 1},
			Session: operatorchannel.SessionAccountAdmission{
				Provider: "whatsapp", ConnectionID: uuid.NewString(), AccountRef: "synthetic_account",
				AdmissionID: uuid.NewString(), Revision: 1,
			},
			Source: channelonboarding.ChannelDurableContextIdentity{
				BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64), BundleIdentity: "source:test",
				PackInventoryGeneration: "inventory:test", PlanGeneration: generation,
			},
			OnboardingOperation: uuid.NewString(), OperationRevision: 1, ActivationRevision: 1, TargetSelector: "ingress:.:whatsapp",
			PrincipalID: uuid.NewString(), BindingRevision: 1,
		},
		OccurrenceID: uuid.NewString(), Conversation: "synthetic_conversation", EventID: "event_1",
		Kind: "message", Body: []byte("{ \"text\" : \"private fixture\" }"),
		ReceivedAt: time.Date(2026, 10, 7, 12, 0, 0, 123456000, time.UTC),
	}
}

func TestWhatsAppCaptureScopeAndReceiptAreClosedProducts(t *testing.T) {
	business := captureFixture(t)
	bootstrap := business
	bootstrap.Scope.Kind = channelonboarding.SessionInputOnboarding
	bootstrap.Scope.PublicationBinding = runtimeinbound.BindingGeneration{}
	bootstrap.Scope.BindingRevision, bootstrap.Scope.ActivationRevision = 0, 0
	for _, event := range []capturedEvent{bootstrap, business} {
		if err := event.validate(); err != nil {
			t.Fatal(err)
		}
	}
	for _, row := range []struct {
		name string
		base capturedEvent
		edit func(*capturedEvent)
	}{
		{"unknown_scope", business, func(e *capturedEvent) { e.Scope.Kind = "guessed" }},
		{"no_scope", business, func(e *capturedEvent) { e.Scope.Kind = "" }},
		{"no_operation_revision", business, func(e *capturedEvent) { e.Scope.OperationRevision = 0 }},
		{"no_target", business, func(e *capturedEvent) { e.Scope.TargetSelector = "" }},
		{"bootstrap_binding", bootstrap, func(e *capturedEvent) { e.Scope.PublicationBinding = business.Scope.PublicationBinding }},
		{"bootstrap_activation", bootstrap, func(e *capturedEvent) { e.Scope.ActivationRevision = 1 }},
		{"business_activation", business, func(e *capturedEvent) { e.Scope.ActivationRevision = 0 }},
		{"missing_receipt", business, func(e *capturedEvent) { e.ReceivedAt = time.Time{} }},
		{"unrepresentable_receipt", business, func(e *capturedEvent) { e.ReceivedAt = e.ReceivedAt.Add(time.Nanosecond) }},
	} {
		t.Run(row.name, func(t *testing.T) {
			event := row.base
			row.edit(&event)
			_, spool := openCaptureFixture(t, filepath.Join(t.TempDir(), "incoming.db"), event.Scope.Session.ConnectionID)
			if err := spool.capture(context.Background(), event); err == nil {
				t.Fatal("invalid scope or receipt acquired durable capture")
			}
			if rows, err := spool.pending(context.Background()); err != nil || len(rows) != 0 {
				t.Fatal("failed capture mutated private evidence", err)
			}
		})
	}
}

func TestWhatsAppCaptureProcessDeathBeforeAndAfterCommit(t *testing.T) {
	if mode := os.Getenv("SWARM_WHATSAPP_CAPTURE_TEST_MODE"); mode != "" {
		var event capturedEvent
		if err := json.Unmarshal([]byte(os.Getenv("SWARM_WHATSAPP_CAPTURE_TEST_EVENT")), &event); err != nil {
			t.Fatal(err)
		}
		_, store := openCaptureFixture(t, os.Getenv("SWARM_WHATSAPP_CAPTURE_TEST_PATH"), event.Scope.Session.ConnectionID)
		if mode == "after_commit" {
			if err := store.capture(context.Background(), event); err != nil {
				t.Fatal(err)
			}
		} else if mode != "before_commit" {
			t.Fatal("unknown capture process boundary")
		}
		fmt.Println("CAPTURE_BOUNDARY_READY")
		for {
			time.Sleep(time.Hour)
		}
	}
	for _, mode := range []string{"before_commit", "after_commit"} {
		t.Run(mode, func(t *testing.T) {
			event := captureFixture(t)
			encoded, err := json.Marshal(event)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "incoming.db")
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			child := exec.CommandContext(ctx, binary, "-test.run=^TestWhatsAppCaptureProcessDeathBeforeAndAfterCommit$", "-test.timeout=30s")
			child.Env = append(os.Environ(), "SWARM_WHATSAPP_CAPTURE_TEST_MODE="+mode,
				"SWARM_WHATSAPP_CAPTURE_TEST_EVENT="+string(encoded), "SWARM_WHATSAPP_CAPTURE_TEST_PATH="+path)
			output, err := child.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := child.Start(); err != nil {
				t.Fatal(err)
			}
			joined := false
			defer func() {
				if !joined {
					_ = child.Process.Kill()
					_ = child.Wait()
				}
			}()
			scanner := bufio.NewScanner(output)
			ready := false
			for scanner.Scan() {
				if scanner.Text() == "CAPTURE_BOUNDARY_READY" {
					ready = true
					break
				}
			}
			if !ready {
				t.Fatalf("child failed before capture boundary: %v", scanner.Err())
			}
			if err := child.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			err = child.Wait()
			joined = true
			if err == nil {
				t.Fatal("actual process-death proof returned ordinary success")
			}
			_, reopened := openCaptureFixture(t, path, event.Scope.Session.ConnectionID)
			got, err := reopened.pending(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			want := 0
			if mode == "after_commit" {
				want = 1
			}
			if len(got) != want || (want == 1 && !reflect.DeepEqual(got[0], event)) {
				t.Fatalf("wrong durable process-death result: %+v", got)
			}
		})
	}
}

func openCaptureFixture(t *testing.T, path, connectionID string) (*sql.DB, *captureStore) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path)+"?_pragma=synchronous(FULL)")
	if err != nil {
		t.Fatal(err)
	}
	store, err := newCaptureStore(context.Background(), db, connectionID)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db, store
}

func TestWhatsAppCaptureAcknowledgmentRequiresCommitAndSurvivesReopen(t *testing.T) {
	event := captureFixture(t)
	path := filepath.Join(t.TempDir(), "incoming.db")
	db, store := openCaptureFixture(t, path, event.Scope.Session.ConnectionID)
	guard, err := newCallbackGuard(context.Background(), event.Scope.Session.ConnectionID, event.OccurrenceID,
		func(context.Context, any) error { return store.capture(context.Background(), event) }, store.recordFailure)
	if err != nil {
		t.Fatal(err)
	}
	if !guard.receive(event) {
		t.Fatal("committed capture was not acknowledged")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	_, reopened := openCaptureFixture(t, path, event.Scope.Session.ConnectionID)
	got, err := reopened.pending(context.Background())
	if err != nil || len(got) != 1 || !reflect.DeepEqual(got[0], event) {
		t.Fatalf("acknowledged capture did not survive reopen exactly: %+v %v", got, err)
	}
	redelivered := event
	redelivered.OccurrenceID = uuid.NewString()
	if err := reopened.capture(context.Background(), redelivered); err != nil {
		t.Fatal(err)
	}
	got, err = reopened.pending(context.Background())
	if err != nil || len(got) != 1 || got[0].OccurrenceID != event.OccurrenceID || !bytes.Equal(got[0].Body, event.Body) {
		t.Fatal("redelivery overwrote original capture or duplicated work")
	}
}

func TestWhatsAppCaptureWriteFailureHasNoAckAndRetainsFailureEvidence(t *testing.T) {
	event := captureFixture(t)
	db, store := openCaptureFixture(t, filepath.Join(t.TempDir(), "incoming.db"), event.Scope.Session.ConnectionID)
	if _, err := db.Exec(`CREATE TRIGGER reject_capture BEFORE INSERT ON whatsapp_incoming_capture
		BEGIN SELECT RAISE(ABORT,'capture fixture write failure'); END`); err != nil {
		t.Fatal(err)
	}
	guard, err := newCallbackGuard(context.Background(), event.Scope.Session.ConnectionID, event.OccurrenceID,
		func(context.Context, any) error { return store.capture(context.Background(), event) }, store.recordFailure)
	if err != nil {
		t.Fatal(err)
	}
	if guard.receive(event) || guard.currentFailure() == nil {
		t.Fatal("failed capture reported acknowledgment")
	}
	got, err := store.pending(context.Background())
	if err != nil || len(got) != 0 {
		t.Fatalf("failed capture retained a partial event: %+v %v", got, err)
	}
	var reason string
	if err := db.QueryRow(`SELECT reason FROM whatsapp_callback_failures WHERE occurrence_id=?`, event.OccurrenceID).Scan(&reason); err != nil || reason != "capture_failed" {
		t.Fatalf("failure evidence missing: %q %v", reason, err)
	}
}

func TestWhatsAppCapturePanicsBeforeCommitRetainOnlyFailureEvidence(t *testing.T) {
	event := captureFixture(t)
	db, store := openCaptureFixture(t, filepath.Join(t.TempDir(), "incoming.db"), event.Scope.Session.ConnectionID)
	guard, err := newCallbackGuard(context.Background(), event.Scope.Session.ConnectionID, event.OccurrenceID,
		func(context.Context, any) error { panic("private pre-capture data") }, store.recordFailure)
	if err != nil {
		t.Fatal(err)
	}
	if guard.receive(event) {
		t.Fatal("panic was acknowledged")
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM whatsapp_callback_failures WHERE occurrence_id=? AND reason='crash'`, event.OccurrenceID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("durable crash evidence missing: %d %v", count, err)
	}
	got, err := store.pending(context.Background())
	if err != nil || len(got) != 0 {
		t.Fatal("pre-capture panic created incoming work")
	}
}

func TestWhatsAppCaptureBoundsRefuseWithoutTrimming(t *testing.T) {
	for _, bound := range []string{"event", "count", "bytes"} {
		t.Run(bound, func(t *testing.T) {
			event := captureFixture(t)
			db, store := openCaptureFixture(t, filepath.Join(t.TempDir(), "incoming.db"), event.Scope.Session.ConnectionID)
			switch bound {
			case "event":
				event.Body = []byte(`"` + strings.Repeat("x", maxCaptureEventBytes-1) + `"`)
			case "count":
				seedCaptureQuotaFixture(t, db, store, event, maxPendingCaptureCount)
				event.EventID = "overflow"
			case "bytes":
				event.Body = []byte(`"` + strings.Repeat("x", maxCaptureEventBytes-2) + `"`)
				seedCaptureQuotaFixture(t, db, store, event, maxPendingCaptureBytes/maxCaptureEventBytes)
				event.EventID = "overflow"
			}
			before := captureRawRowsFixture(t, db)
			if err := store.capture(context.Background(), event); !errors.Is(err, errCaptureCapacity) {
				t.Fatalf("capacity not enforced: %v", err)
			}
			after := captureRawRowsFixture(t, db)
			if !reflect.DeepEqual(before, after) {
				t.Fatal("overflow trimmed or changed existing evidence")
			}
		})
	}
}

// Quota setup is not an append-throughput proof. Seed the exact private state
// once, then independently validate every row through the production reader.
func seedCaptureQuotaFixture(t *testing.T, db *sql.DB, capture *captureStore, event capturedEvent, count int) {
	t.Helper()
	event.EventID = "0"
	if err := capture.capture(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for i := 1; i < count; i++ {
		event.EventID = fmt.Sprint(i)
		envelope, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(envelope)
		if _, err := tx.Exec(`INSERT INTO whatsapp_incoming_capture
			(connection_id,account_ref,conversation_ref,event_id,event_kind,body_bytes,envelope,digest)
			VALUES(?,?,?,?,?,?,?,?)`, event.Scope.Session.ConnectionID, event.Scope.Session.AccountRef, event.Conversation,
			event.EventID, event.Kind, len(event.Body), envelope, digest[:]); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	pending, err := capture.pending(context.Background())
	if err != nil || len(pending) != count {
		t.Fatalf("invalid quota fixture: count=%d err=%v", len(pending), err)
	}
	for i, got := range pending {
		want := event
		want.EventID = fmt.Sprint(i)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("quota row %d differs from exact source fixture", i)
		}
	}
}

func captureRawRowsFixture(t *testing.T, db *sql.DB) [][]byte {
	t.Helper()
	rows, err := db.Query(`SELECT envelope,digest FROM whatsapp_incoming_capture ORDER BY sequence`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var result [][]byte
	for rows.Next() {
		var envelope, digest []byte
		if err := rows.Scan(&envelope, &digest); err != nil {
			t.Fatal(err)
		}
		result = append(result, envelope, digest)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestWhatsAppCaptureSurvivesRebindResetWithoutNewAuthority(t *testing.T) {
	event := captureFixture(t)
	path := filepath.Join(t.TempDir(), "incoming.db")
	db, store := openCaptureFixture(t, path, event.Scope.Session.ConnectionID)
	if err := store.capture(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	_, store = openCaptureFixture(t, path, event.Scope.Session.ConnectionID)
	changes := map[string]func(*captureScope){
		"source":               func(scope *captureScope) { scope.Source.BundleHash = "bundle-v2:sha256:" + strings.Repeat("b", 64) },
		"principal":            func(scope *captureScope) { scope.PrincipalID = uuid.NewString() },
		"account":              func(scope *captureScope) { scope.Session.AccountRef = "replacement_account" },
		"admission":            func(scope *captureScope) { scope.Session.AdmissionID = uuid.NewString() },
		"revision":             func(scope *captureScope) { scope.BindingRevision++ },
		"reset_responsibility": func(scope *captureScope) { scope.OnboardingOperation = uuid.NewString() },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			current := event.Scope
			change(&current)
			got, err := store.pending(context.Background())
			if err != nil || len(got) != 1 || !reflect.DeepEqual(got[0], event) {
				t.Fatal("retained capture was deleted or rewritten")
			}
			if err := got[0].requireOriginalScope(current); !errors.Is(err, errCaptureScopeChanged) {
				t.Fatalf("old capture adopted new authority: %v", err)
			}
		})
	}
}

func TestWhatsAppCaptureIdentityCollisionAndCorruptionRefuse(t *testing.T) {
	event := captureFixture(t)
	db, store := openCaptureFixture(t, filepath.Join(t.TempDir(), "incoming.db"), event.Scope.Session.ConnectionID)
	if err := store.capture(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	conflict := event
	conflict.Body = []byte(`{"text":"different"}`)
	if err := store.capture(context.Background(), conflict); !errors.Is(err, errCaptureConflict) {
		t.Fatalf("conflicting body admitted: %v", err)
	}
	for _, change := range []func(*capturedEvent){
		func(event *capturedEvent) { event.Conversation = "another_conversation" },
		func(event *capturedEvent) { event.Kind = "edit" },
		func(event *capturedEvent) { event.Kind = "revoke" },
	} {
		sibling := event
		change(&sibling)
		if err := store.capture(context.Background(), sibling); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`UPDATE whatsapp_incoming_capture SET conversation_ref='tampered' WHERE sequence=1`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.pending(context.Background()); err == nil {
		t.Fatal("corrupt capture routing header was read as valid")
	}
}
