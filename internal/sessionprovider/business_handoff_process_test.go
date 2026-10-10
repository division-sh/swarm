//go:build linux || darwin

package sessionprovider

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	inbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

type nativeBusinessProcessFixture struct {
	Backend, Location, BasePath, OperationID string
}

type nativeBusinessDeathStore struct {
	sessionBusinessStore
	phase string
}

func nativeBusinessDeathBoundary() {
	fmt.Println("NATIVE_BUSINESS_READY")
	for {
		time.Sleep(time.Hour)
	}
}

func (s nativeBusinessDeathStore) CommitInboundPublication(ctx context.Context, command inbound.CommitCommand) (inbound.CommitResult, error) {
	if s.phase == "before_commit" {
		nativeBusinessDeathBoundary()
	}
	result, err := s.sessionBusinessStore.CommitInboundPublication(ctx, command)
	if err == nil && result.Acknowledged && s.phase == "after_commit" {
		nativeBusinessDeathBoundary()
	}
	return result, err
}

func reopenNativeBusinessProcessFixture(t *testing.T, config nativeBusinessProcessFixture) *activeInputFixture {
	t.Helper()
	f := &activeInputFixture{basePath: config.BasePath, location: config.Location}
	if config.Backend == "sqlite" {
		f.selected, _ = storetest.StartSQLiteRuntimeStoreWithReopen(t, context.Background(), config.Location)
	} else {
		f.selected, _ = storetest.StartPostgresRuntimeStoreWithReopen(t, config.Location)
	}
	var err error
	f.operation, err = f.selected.GetChannelOnboarding(context.Background(), config.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	f.activation, err = f.selected.GetConnectedChannelActivation(context.Background(), f.operation.SlotKey)
	if err != nil {
		t.Fatal(err)
	}
	source := sourceartifactfixture.Require(t, context.Background(), f.selected)
	f.ctx = correlation.WithSourceArtifactFact(context.Background(), source)
	var found bool
	f.standing, found, err = f.selected.LoadReconciledStandingService(f.ctx, pipeline.StandingServiceCandidate{
		ServiceID: flowidentity.StandingServiceID("."), FlowPath: ".", BindingEnabled: true, Source: source})
	if err != nil || !found {
		t.Fatalf("original native standing responsibility: found=%t %v", found, err)
	}
	manifest := sessionInputManifestFixture(t)
	f.catalog, err = providertriggers.NewCatalogSnapshot(providertriggers.CatalogEntry{Manifest: manifest,
		Identity: providertriggers.PackIdentity{ID: "provider.whatsapp.input", Version: "1.0.0", ManifestHash: packs.ManifestHash(manifest.SourceBytes()), Provenance: "test"}})
	if err != nil {
		t.Fatal(err)
	}
	f.trigger, err = f.catalog.CompileAdmission(providertriggers.CompileAdmissionRequest{Alias: "whatsapp", Provider: "whatsapp"})
	if err != nil {
		t.Fatal(err)
	}
	f.source = captureSource{Coordinate: f.operation.Coordinate, CatalogGeneration: f.catalog.Generation()}
	f.state = sessionStateFixture(t, f.basePath, f.operation.SessionAccount.ConnectionID, f.operation.SessionAccount.AccountRef)
	f.restartBusinessConnection(t)
	return f
}

func runNativeBusinessDeathChild(t *testing.T, phase string, config nativeBusinessProcessFixture) {
	t.Helper()
	encoded, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, binary, "-test.run=^TestWhatsAppNativeBusinessHandoffProcessDeathBothStores$", "-test.timeout=30s")
	child.Env = append(os.Environ(), "SWARM_NATIVE_BUSINESS_DEATH_PHASE="+phase, "SWARM_NATIVE_BUSINESS_DEATH_FIXTURE="+string(encoded))
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
	var stdout bytes.Buffer
	ready := false
	for scanner.Scan() {
		if stdout.Len() < 32<<10 {
			fmt.Fprintln(&stdout, scanner.Text())
		}
		if scanner.Text() == "NATIVE_BUSINESS_READY" {
			ready = true
			break
		}
	}
	if !ready {
		waitErr := child.Wait()
		joined = true
		t.Fatalf("native process did not reach %s: scan=%v wait=%v; stdout=%s stderr=%s", phase, scanner.Err(), waitErr, stdout.String(), stderr.String())
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	err = child.Wait()
	joined = true
	if err == nil {
		t.Fatal("native process-death proof returned success")
	}
}

func TestWhatsAppNativeBusinessHandoffProcessDeathBothStores(t *testing.T) {
	if phase := os.Getenv("SWARM_NATIVE_BUSINESS_DEATH_PHASE"); phase != "" {
		var config nativeBusinessProcessFixture
		if err := json.Unmarshal([]byte(os.Getenv("SWARM_NATIVE_BUSINESS_DEATH_FIXTURE")), &config); err != nil {
			t.Fatal(err)
		}
		f := reopenNativeBusinessProcessFixture(t, config)
		if phase == "before_planning" {
			nativeBusinessDeathBoundary()
		}
		handoff := f.businessHandoff(t)
		handoff.store = nativeBusinessDeathStore{sessionBusinessStore: handoff.store, phase: phase}
		if err := handoff.drain(f.ctx); err != nil {
			t.Fatal("native child handoff", err)
		}
		if phase != "after_retirement" {
			t.Fatal("native child missed interruption boundary")
		}
		nativeBusinessDeathBoundary()
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, phase := range []string{"before_planning", "before_commit", "after_commit", "after_retirement"} {
			t.Run(backend+"/"+phase, func(t *testing.T) {
				f := newActiveInputFixture(t, backend)
				f.activate(t)
				event, admitted := f.receive(t, "unfinished native message")
				admitted.Close()
				if err := f.state.close(context.Background()); err != nil {
					t.Fatal(err)
				}
				runNativeBusinessDeathChild(t, phase, nativeBusinessProcessFixture{Backend: backend, Location: f.location, BasePath: f.basePath, OperationID: f.operation.OperationID})
				var err error
				f.operation, err = f.selected.GetChannelOnboarding(context.Background(), f.operation.OperationID)
				if err != nil {
					t.Fatal(err)
				}
				f.source.Coordinate = f.operation.Coordinate
				f.state = sessionStateFixture(t, f.basePath, f.operation.SessionAccount.ConnectionID, f.operation.SessionAccount.AccountRef)
				f.spool, err = newCaptureStore(f.ctx, f.state.database, event.Scope.Session.ConnectionID)
				if err != nil {
					t.Fatal(err)
				}
				rows, err := f.spool.pendingPublications(f.ctx)
				if err != nil {
					t.Fatal(err)
				}
				var frozen []byte
				if len(rows) == 2 {
					frozen = bytes.Clone(rows[1].requestBytes)
				}
				if phase == "after_retirement" {
					if len(rows) != 1 {
						t.Fatal("retired business reappeared or setup capture vanished")
					}
				} else if len(rows) != 2 || !rows[1].event.SameCapture(event) ||
					(phase != "before_planning" && rows[1].request == nil) {
					t.Fatal("process death lost original pending capture/request")
				}
				if phase == "before_planning" || phase == "before_commit" {
					f.restartBusinessConnection(t)
					if err := f.businessHandoff(t).drain(f.ctx); err != nil {
						t.Fatal("unfinished native restart", err)
					}
				} else if phase == "after_commit" {
					f.republishBusinessStanding(t)
					// Committed evidence needs neither a live SDK nor reminted input.
					if f.state.currentOccurrence() != nil {
						t.Fatal("history proof unexpectedly installed a live SDK")
					}
					if settled, err := f.spool.reconcilePublished(f.ctx, event, f.selected.(sessionBusinessStore)); err != nil || !settled {
						t.Fatalf("committed native restart: %t %v", settled, err)
					}
				} else {
					f.republishBusinessStanding(t)
				}
				requireNativeBusinessReceipt(t, f, event)
				if len(frozen) > 0 {
					identity, _ := event.PublicationIdentity()
					record, _, err := f.selected.(sessionBusinessStore).LoadInboundPublicationByIdentity(f.ctx, identity)
					actual, encodeErr := publicationRequestBytes(record.Request)
					if err != nil || encodeErr != nil || !bytes.Equal(actual, frozen) {
						t.Fatal("process restart changed staged request evidence", err, encodeErr)
					}
				}
			})
		}
	}
}
