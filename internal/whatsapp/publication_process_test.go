package whatsapp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	runtimeinbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	"github.com/division-sh/swarm/internal/store"
	"github.com/google/uuid"
)

type historicalPublicationBarrierReader struct {
	publicationReader
	phase string
}

func historicalPublicationBoundary() {
	fmt.Println("HISTORICAL_PUBLICATION_READY")
	for {
		time.Sleep(time.Hour)
	}
}

func (r historicalPublicationBarrierReader) LoadInboundPublicationByIdentity(ctx context.Context, provider, entity, event string) (runtimeinbound.Record, bool, error) {
	if r.phase == "before_read" {
		historicalPublicationBoundary()
	}
	record, found, err := r.publicationReader.LoadInboundPublicationByIdentity(ctx, provider, entity, event)
	if err == nil && found && r.phase == "after_verified_read" {
		historicalPublicationBoundary()
	}
	return record, found, err
}

func TestWhatsAppHistoricalPublicationProcessDeathBothStores(t *testing.T) {
	const phaseKey = "SWARM_WHATSAPP_HISTORY_PHASE"
	const eventKey = "SWARM_WHATSAPP_HISTORY_EVENT"
	const backendKey = "SWARM_WHATSAPP_HISTORY_BACKEND"
	const locationKey = "SWARM_WHATSAPP_HISTORY_LOCATION"
	const capturePathKey = "SWARM_WHATSAPP_HISTORY_CAPTURE_PATH"
	if phase := os.Getenv(phaseKey); phase != "" {
		var event capturedEvent
		if err := json.Unmarshal([]byte(os.Getenv(eventKey)), &event); err != nil {
			t.Fatal(err)
		}
		var reader publicationReader
		if os.Getenv(backendKey) == "sqlite" {
			selected, err := store.NewSQLiteRuntimeStore(os.Getenv(locationKey))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = selected.Close() })
			reader = selected
		} else {
			selected, err := store.NewPostgresStore(os.Getenv(locationKey))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = selected.Close() })
			reader = selected
		}
		_, spool := openCaptureFixture(t, os.Getenv(capturePathKey), event.Scope.Session.ConnectionID)
		if settled, err := spool.reconcilePublished(context.Background(), event,
			historicalPublicationBarrierReader{publicationReader: reader, phase: phase}); err != nil || !settled {
			t.Fatalf("historical child failed: settled=%t err=%v", settled, err)
		}
		if phase != "after_retirement" {
			t.Fatal("unreached historical interruption boundary")
		}
		historicalPublicationBoundary()
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newSessionPublicationFixture(t, backend)
			for _, phase := range []string{"before_read", "after_verified_read", "after_retirement"} {
				t.Run(phase, func(t *testing.T) {
					event := f.capture(t)
					command := f.command(t, event)
					if result, err := f.selected.CommitInboundPublication(f.ctx, command); err != nil || !result.Acknowledged {
						t.Fatalf("publication for interruption proof: %+v %v", result, err)
					}
					duplicate := event
					duplicate.OccurrenceID = uuid.NewString()
					path := filepath.Join(t.TempDir(), "incoming.db")
					db, spool := openCaptureFixture(t, path, event.Scope.Session.ConnectionID)
					if err := spool.capture(f.ctx, duplicate); err != nil {
						t.Fatal(err)
					}
					if err := db.Close(); err != nil {
						t.Fatal(err)
					}
					encoded, err := json.Marshal(duplicate)
					if err != nil {
						t.Fatal(err)
					}
					binary, err := os.Executable()
					if err != nil {
						t.Fatal(err)
					}
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					child := exec.CommandContext(ctx, binary, "-test.run=^TestWhatsAppHistoricalPublicationProcessDeathBothStores$", "-test.timeout=30s")
					child.Env = append(os.Environ(), phaseKey+"="+phase, eventKey+"="+string(encoded), backendKey+"="+backend,
						locationKey+"="+f.location, capturePathKey+"="+path)
					var stderr bytes.Buffer
					child.Stderr = &stderr
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
						if scanner.Text() == "HISTORICAL_PUBLICATION_READY" {
							ready = true
							break
						}
					}
					if !ready {
						t.Fatalf("child failed before historical boundary: %v; %s", scanner.Err(), stderr.String())
					}
					if err := child.Process.Kill(); err != nil {
						t.Fatal(err)
					}
					err = child.Wait()
					joined = true
					if err == nil {
						t.Fatal("process-death proof returned ordinary success")
					}
					_, spool = openCaptureFixture(t, path, event.Scope.Session.ConnectionID)
					pending, err := spool.pending(f.ctx)
					if err != nil {
						t.Fatal(err)
					}
					if phase == "after_retirement" {
						if len(pending) != 0 {
							t.Fatal("historically retired duplicate reappeared")
						}
					} else {
						if len(pending) != 1 || !pending[0].sameCapture(duplicate) {
							t.Fatal("interrupted historical read lost capture before retirement")
						}
						if settled, err := spool.reconcilePublished(f.ctx, pending[0], f.selected); err != nil || !settled {
							t.Fatalf("restart did not verify and complete original history: %t %v", settled, err)
						}
					}
					record, found, err := f.selected.LoadInboundPublicationByIdentity(f.ctx, "whatsapp", event.Scope.EntityID, command.Request.ProviderEventID)
					if err != nil || !found || record.OutputCount != 2 {
						t.Fatal("historical interruption altered selected-store publication", err)
					}
				})
			}
		})
	}
}
