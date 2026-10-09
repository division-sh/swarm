package sessionprovider

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	runtimeinbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	"github.com/google/uuid"
)

func capturePublicationFixture(t *testing.T, event capturedEvent) runtimeinbound.Request {
	t.Helper()
	identity, err := event.publicationIdentity()
	if err != nil {
		t.Fatal(err)
	}
	fingerprint, err := event.publicationFingerprint()
	if err != nil {
		t.Fatal(err)
	}
	publicationID, markerID, err := runtimeinbound.DeterministicIDs(identity)
	if err != nil {
		t.Fatal(err)
	}
	request := runtimeinbound.Request{PublicationID: publicationID, MarkerEventID: markerID, Provider: "whatsapp",
		ProviderEventID: identity.ProviderEventID, RequestFingerprint: fingerprint,
		RequestProjectionVersion: runtimeinbound.RequestSemanticProjectionVersion, StableServiceID: identity.ServiceID,
		FlowPath: ".", TargetAlias: "whatsapp",
		ExpectedGeneration: identity.Generation, ExpectedPublicationSequence: 1, ResolvedRunID: identity.RunID,
		AcknowledgementMode:       runtimeinbound.AcknowledgementDurableBeforeDispatch,
		OriginalReceivedAt:        event.ReceivedAt,
		OriginalTransportMetadata: []byte(`{"transport":"managed_session","fixture":true}`)}
	request, err = withCaptureProvenance(event, request)
	if err != nil {
		t.Fatal(err)
	}
	return request
}

// This double exercises only the private retirement boundary. It does not prove
// selected-store integrity, account authentication or business route admission.
type capturePublicationReader struct {
	record runtimeinbound.Record
	found  bool
	err    error
	calls  int
}

func (r *capturePublicationReader) LoadInboundPublicationByIdentity(_ context.Context, identity runtimeinbound.Identity) (runtimeinbound.Record, bool, error) {
	r.calls++
	if identity != r.record.Identity() {
		return runtimeinbound.Record{}, false, errors.New("publication lookup changed original identity")
	}
	return r.record, r.found, r.err
}

func publishedCaptureFixture(request runtimeinbound.Request) *capturePublicationReader {
	return &capturePublicationReader{found: true, record: runtimeinbound.Record{
		Request: request, State: "committed", CommittedAt: request.OriginalReceivedAt.Add(time.Second)}}
}

func equalCapturePublicationRequest(t *testing.T, actual *runtimeinbound.Request, expected runtimeinbound.Request) bool {
	t.Helper()
	if actual == nil {
		return false
	}
	got, err := publicationRequestBytes(*actual)
	if err != nil {
		t.Fatal(err)
	}
	want, err := publicationRequestBytes(expected)
	if err != nil {
		t.Fatal(err)
	}
	return bytes.Equal(got, want)
}

func TestWhatsAppCapturePublicationStagingSurvivesReopenAndRefusesReplacement(t *testing.T) {
	event := captureFixture(t)
	request := capturePublicationFixture(t, event)
	path := filepath.Join(t.TempDir(), "incoming.db")
	db, store := openCaptureFixture(t, path, event.Scope.Session.ConnectionID)
	ctx := context.Background()
	if err := store.capture(ctx, event); err != nil {
		t.Fatal(err)
	}
	if err := store.stagePublication(ctx, event, request); err != nil {
		t.Fatal(err)
	}
	if err := store.stagePublication(ctx, event, request); err != nil {
		t.Fatal("exact stage duplicate failed", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	_, reopened := openCaptureFixture(t, path, event.Scope.Session.ConnectionID)
	rows, err := reopened.readPendingRows(ctx, reopened.db)
	if err != nil || len(rows) != 1 || !rows[0].event.sameCapture(event) ||
		!equalCapturePublicationRequest(t, rows[0].request, request) {
		t.Fatalf("recovery lost original target or capture: %+v, %v", rows, err)
	}
	for _, mutate := range []struct {
		name string
		fn   func(*runtimeinbound.Request)
	}{
		{"target", func(r *runtimeinbound.Request) { r.TargetAlias = "replacement" }},
		{"generation", func(r *runtimeinbound.Request) { r.ExpectedGeneration++ }},
		{"run", func(r *runtimeinbound.Request) { r.ResolvedRunID = uuid.NewString() }},
		{"source metadata", func(r *runtimeinbound.Request) { r.OriginalTransportMetadata = []byte(`{"source":"replacement"}`) }},
		{"receipt time", func(r *runtimeinbound.Request) { r.OriginalReceivedAt = r.OriginalReceivedAt.Add(time.Microsecond) }},
	} {
		t.Run(mutate.name, func(t *testing.T) {
			changed := request
			mutate.fn(&changed)
			var err error
			changed.PublicationID, changed.MarkerEventID, err = runtimeinbound.DeterministicIDs(changed.Identity())
			if err != nil {
				t.Fatal(err)
			}
			if err := reopened.stagePublication(ctx, event, changed); !errors.Is(err, runtimeinbound.ErrRequestIdentityConflict) {
				t.Fatalf("replacement staging = %v", err)
			}
		})
	}
	for _, mutate := range []struct {
		name string
		fn   func(*capturedEvent)
	}{
		{"account", func(e *capturedEvent) { e.Scope.Session.AccountRef = "replacement" }},
		{"admission", func(e *capturedEvent) { e.Scope.Session.AdmissionID = uuid.NewString() }},
		{"source", func(e *capturedEvent) {
			e.Scope.Source.BundleIdentity = "replacement"
			e.Source.Coordinate.BundleIdentity = "replacement"
		}},
		{"principal", func(e *capturedEvent) { e.Scope.PrincipalID = uuid.NewString() }},
		{"binding", func(e *capturedEvent) { e.Scope.BindingRevision++ }},
		{"occurrence", func(e *capturedEvent) { e.OccurrenceID = uuid.NewString() }},
	} {
		t.Run(mutate.name, func(t *testing.T) {
			changed := event
			mutate.fn(&changed)
			if err := reopened.stagePublication(ctx, changed, request); err == nil {
				t.Fatal("replacement authority adopted captured work")
			}
			reader := publishedCaptureFixture(request)
			if err := reopened.retirePublished(ctx, changed, reader); !errors.Is(err, errCaptureMissing) || reader.calls != 0 {
				t.Fatalf("replacement retirement reached publication owner: %v, calls=%d", err, reader.calls)
			}
		})
	}
	rows, err = reopened.readPendingRows(ctx, reopened.db)
	if err != nil || len(rows) != 1 || !rows[0].event.sameCapture(event) || !equalCapturePublicationRequest(t, rows[0].request, request) {
		t.Fatal("rejected replacement changed retained evidence", err)
	}
}

func TestWhatsAppCaptureRetirementRequiresDurableExactReadback(t *testing.T) {
	for _, cell := range []string{"missing", "io_failure", "reserved", "no_commit_time", "foreign_target", "changed_fingerprint", "committed"} {
		t.Run(cell, func(t *testing.T) {
			event := captureFixture(t)
			request := capturePublicationFixture(t, event)
			_, store := openCaptureFixture(t, filepath.Join(t.TempDir(), "incoming.db"), event.Scope.Session.ConnectionID)
			ctx := context.Background()
			if err := store.capture(ctx, event); err != nil {
				t.Fatal(err)
			}
			reader := publishedCaptureFixture(request)
			reader.found = false
			if err := store.retirePublished(ctx, event, reader); !errors.Is(err, errCapturePublicationPending) || reader.calls != 1 {
				t.Fatal("unstaged capture consumed an unrelated receipt", err)
			}
			reader.found = true
			if err := store.stagePublication(ctx, event, request); err != nil {
				t.Fatal(err)
			}
			switch cell {
			case "missing":
				reader.found = false
			case "io_failure":
				reader.err = errors.New("fixture read failure")
			case "reserved":
				reader.record.State = "reserved"
			case "no_commit_time":
				reader.record.CommittedAt = time.Time{}
			case "foreign_target":
				reader.record.TargetAlias = "replacement"
			case "changed_fingerprint":
				reader.record.RequestFingerprint = string(bytes.Repeat([]byte("b"), 64))
			}
			err := store.retirePublished(ctx, event, reader)
			if (err == nil) != (cell == "committed") {
				t.Fatalf("retirement = %v", err)
			}
			pending, err := store.pending(ctx)
			if err != nil || len(pending) != map[bool]int{false: 1, true: 0}[cell == "committed"] {
				t.Fatalf("retirement lost pending evidence: %+v, %v", pending, err)
			}
		})
	}
}

func TestWhatsAppCaptureRetirementWriteFailureRetainsPublicationRequest(t *testing.T) {
	event := captureFixture(t)
	request := capturePublicationFixture(t, event)
	db, store := openCaptureFixture(t, filepath.Join(t.TempDir(), "incoming.db"), event.Scope.Session.ConnectionID)
	ctx := context.Background()
	if err := store.capture(ctx, event); err != nil {
		t.Fatal(err)
	}
	if err := store.stagePublication(ctx, event, request); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TRIGGER reject_retirement BEFORE DELETE ON whatsapp_incoming_capture
		BEGIN SELECT RAISE(ABORT,'capture retirement write failure'); END`); err != nil {
		t.Fatal(err)
	}
	reader := publishedCaptureFixture(request)
	if err := store.retirePublished(ctx, event, reader); err == nil {
		t.Fatal("failed local retirement reported success")
	}
	rows, err := store.readPendingRows(ctx, db)
	if err != nil || len(rows) != 1 || !rows[0].event.sameCapture(event) || !equalCapturePublicationRequest(t, rows[0].request, request) {
		t.Fatal("failed retirement lost original evidence", err)
	}
	if _, err := db.Exec(`DROP TRIGGER reject_retirement`); err != nil {
		t.Fatal(err)
	}
	if err := store.retirePublished(ctx, event, reader); err != nil {
		t.Fatal("exact readback retry failed", err)
	}
}

func TestWhatsAppCapturePublicationCorruptionBlocksEveryConsumer(t *testing.T) {
	for _, corruption := range []string{"digest", "request_identity"} {
		for _, consumer := range []string{"pending", "duplicate", "new_event", "stage", "retire"} {
			t.Run(corruption+"/"+consumer, func(t *testing.T) {
				event := captureFixture(t)
				request := capturePublicationFixture(t, event)
				db, store := openCaptureFixture(t, filepath.Join(t.TempDir(), "incoming.db"), event.Scope.Session.ConnectionID)
				ctx := context.Background()
				if err := store.capture(ctx, event); err != nil {
					t.Fatal(err)
				}
				if err := store.stagePublication(ctx, event, request); err != nil {
					t.Fatal(err)
				}
				if corruption == "digest" {
					if _, err := db.Exec(`UPDATE whatsapp_incoming_capture SET publication_digest=?`, make([]byte, 32)); err != nil {
						t.Fatal(err)
					}
				} else {
					changed := request
					changed.ProviderEventID = "foreign"
					var err error
					changed.PublicationID, changed.MarkerEventID, err = runtimeinbound.DeterministicIDs(changed.Identity())
					if err != nil {
						t.Fatal(err)
					}
					raw, err := publicationRequestBytes(changed)
					if err != nil {
						t.Fatal(err)
					}
					digest := sha256.Sum256(raw)
					if _, err := db.Exec(`UPDATE whatsapp_incoming_capture SET publication_request=?,publication_digest=?`, raw, digest[:]); err != nil {
						t.Fatal(err)
					}
				}
				var err error
				reader := publishedCaptureFixture(request)
				switch consumer {
				case "pending":
					_, err = store.pending(ctx)
				case "duplicate":
					err = store.capture(ctx, event)
				case "new_event":
					changed := event
					changed.EventID = "NEW"
					err = store.capture(ctx, changed)
				case "stage":
					err = store.stagePublication(ctx, event, request)
				case "retire":
					err = store.retirePublished(ctx, event, reader)
				}
				if err == nil || reader.calls != 0 {
					t.Fatalf("corrupt publication evidence accepted: err=%v, reads=%d", err, reader.calls)
				}
				var count int
				if err := db.QueryRow(`SELECT COUNT(*) FROM whatsapp_incoming_capture`).Scan(&count); err != nil || count != 1 {
					t.Fatal("corruption refusal rewrote capture evidence", err)
				}
			})
		}
	}
}

func TestWhatsAppCapturePublicationStageWriteFailureCannotRetire(t *testing.T) {
	event := captureFixture(t)
	request := capturePublicationFixture(t, event)
	db, store := openCaptureFixture(t, filepath.Join(t.TempDir(), "incoming.db"), event.Scope.Session.ConnectionID)
	ctx := context.Background()
	if err := store.capture(ctx, event); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TRIGGER reject_stage BEFORE UPDATE ON whatsapp_incoming_capture
		BEGIN SELECT RAISE(ABORT,'capture staging write failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := store.stagePublication(ctx, event, request); err == nil {
		t.Fatal("failed staging reported success")
	}
	reader := publishedCaptureFixture(request)
	reader.found = false
	if err := store.retirePublished(ctx, event, reader); !errors.Is(err, errCapturePublicationPending) || reader.calls != 1 {
		t.Fatal("partial staging retired capture", err)
	}
	rows, err := store.readPendingRows(ctx, db)
	if err != nil || len(rows) != 1 || rows[0].request != nil || !rows[0].event.sameCapture(event) {
		t.Fatal("failed staging lost evidence or retained a partial request", err)
	}
	if _, err := db.Exec(`DROP TRIGGER reject_stage`); err != nil {
		t.Fatal(err)
	}
	if err := store.stagePublication(ctx, event, request); err != nil {
		t.Fatal(err)
	}
	reader.found = true
	if err := store.retirePublished(ctx, event, reader); err != nil {
		t.Fatal(err)
	}
}

// Actual process death proves private-state boundaries only. Selected-store
// publication remains the explicitly named double, not a live admission proof.
func TestWhatsAppCapturePublicationProcessDeathBoundaries(t *testing.T) {
	const modeKey = "SWARM_WHATSAPP_PUBLICATION_TEST_MODE"
	const eventKey = "SWARM_WHATSAPP_PUBLICATION_TEST_EVENT"
	const requestKey = "SWARM_WHATSAPP_PUBLICATION_TEST_REQUEST"
	const pathKey = "SWARM_WHATSAPP_PUBLICATION_TEST_PATH"
	if mode := os.Getenv(modeKey); mode != "" {
		var event capturedEvent
		var request runtimeinbound.Request
		if err := json.Unmarshal([]byte(os.Getenv(eventKey)), &event); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal([]byte(os.Getenv(requestKey)), &request); err != nil {
			t.Fatal(err)
		}
		_, store := openCaptureFixture(t, os.Getenv(pathKey), event.Scope.Session.ConnectionID)
		if err := store.capture(context.Background(), event); err != nil {
			t.Fatal(err)
		}
		if mode != "before_stage" {
			if err := store.stagePublication(context.Background(), event, request); err != nil {
				t.Fatal(err)
			}
		}
		if mode == "after_retire" {
			if err := store.retirePublished(context.Background(), event, publishedCaptureFixture(request)); err != nil {
				t.Fatal(err)
			}
		} else if mode != "before_stage" && mode != "after_stage" {
			t.Fatal("unknown publication process boundary")
		}
		fmt.Println("PUBLICATION_BOUNDARY_READY")
		for {
			time.Sleep(time.Hour)
		}
	}
	for _, mode := range []string{"before_stage", "after_stage", "after_retire"} {
		t.Run(mode, func(t *testing.T) {
			event := captureFixture(t)
			request := capturePublicationFixture(t, event)
			encodedEvent, err := json.Marshal(event)
			if err != nil {
				t.Fatal(err)
			}
			encodedRequest, err := json.Marshal(request)
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
			child := exec.CommandContext(ctx, binary, "-test.run=^TestWhatsAppCapturePublicationProcessDeathBoundaries$", "-test.timeout=30s")
			child.Env = append(os.Environ(), modeKey+"="+mode, pathKey+"="+path,
				eventKey+"="+string(encodedEvent), requestKey+"="+string(encodedRequest))
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
				if scanner.Text() == "PUBLICATION_BOUNDARY_READY" {
					ready = true
					break
				}
			}
			if !ready {
				t.Fatalf("child did not reach publication boundary: %v", scanner.Err())
			}
			if err := child.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			err = child.Wait()
			joined = true
			if err == nil {
				t.Fatal("process-death fixture returned ordinary success")
			}
			_, reopened := openCaptureFixture(t, path, event.Scope.Session.ConnectionID)
			rows, err := reopened.readPendingRows(context.Background(), reopened.db)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "after_retire" {
				if len(rows) != 0 {
					t.Fatal("retired capture reappeared after process death")
				}
				return
			}
			if len(rows) != 1 || !rows[0].event.sameCapture(event) || (rows[0].request != nil) != (mode == "after_stage") {
				t.Fatal("crash lost original capture or changed staging boundary")
			}
			if mode == "after_stage" && !equalCapturePublicationRequest(t, rows[0].request, request) {
				t.Fatal("crash recovery changed the original publication request")
			}
		})
	}
}
