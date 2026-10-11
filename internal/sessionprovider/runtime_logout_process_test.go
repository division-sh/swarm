//go:build linux || darwin

package sessionprovider

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

type logoutProcessFixture struct {
	Backend, Location, Directory, OperationID, Phase string
}

type logoutProcessDeathStore struct {
	effects.Store
	t        *testing.T
	phase    string
	original *clientOccurrence
	peer     *sdkPeer
}

func (s *logoutProcessDeathStore) AuthorizeExternalAttempt(ctx context.Context, a effects.Authority, req effects.AuthorizeRequest) (effects.Attempt, error) {
	attempt, err := s.Store.AuthorizeExternalAttempt(ctx, a, req)
	if err == nil && s.phase == "authorized" {
		if attempt.Kind != effects.KindChannelLogout || !attempt.AuthorizationAcknowledged || s.original.client.Store.Deleted {
			s.t.Fatal("prelaunch death boundary did not retain an acknowledged, non-destructive authorization", attempt)
		}
		select {
		case frame := <-s.peer.frames:
			s.t.Fatal("SDK unlink preceded the authorization boundary", frame.node)
		default:
		}
		logoutProcessDeathBoundary(a.ChannelLogout.TeardownID)
	}
	return attempt, err
}

func (s *logoutProcessDeathStore) SettleExternalAttempt(ctx context.Context, result effects.Settlement) error {
	if s.phase == "sdk_deleted_before_settlement" {
		if result.State != effects.StateSettled || !s.original.client.Store.Deleted || s.original.client.IsConnected() ||
			result.Evidence["remote_unlinked"] != true || result.Evidence["local_device_deleted"] != true || result.Evidence["original_joined"] != true {
			s.t.Fatal("death boundary did not follow genuine SDK unlink, deletion and join", result)
		}
		logoutProcessDeathBoundary(result.Authority.ChannelLogout.TeardownID)
	}
	return s.Store.SettleExternalAttempt(ctx, result)
}

func logoutProcessDeathBoundary(id string) {
	fmt.Println("NATIVE_LOGOUT_READY " + id)
	for {
		time.Sleep(time.Hour)
	}
}

func TestWhatsAppLogoutJournalProcessDeathBothStores(t *testing.T) {
	if encoded := os.Getenv("TEST_NATIVE_LOGOUT_DEATH"); encoded != "" {
		var config logoutProcessFixture
		if err := json.Unmarshal([]byte(encoded), &config); err != nil {
			t.Fatal(err)
		}
		runNativeLogoutDeathChild(t, config)
		return
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, phase := range []string{"authorized", "sdk_deleted_before_settlement"} {
			t.Run(backend+"/"+phase, func(t *testing.T) {
				f := newActiveInputFixture(t, backend)
				if err := f.state.close(f.ctx); err != nil {
					t.Fatal("parent did not release original private-state possession", err)
				}
				id := killNativeLogoutAtBoundary(t, logoutProcessFixture{Backend: backend, Location: f.location,
					Directory: f.basePath, OperationID: f.operation.OperationID, Phase: phase})
				ctx := f.ctx
				original, err := f.selected.GetChannelTeardown(ctx, id)
				if err != nil || original.Logout == nil || original.Phase != channelonboarding.TeardownAuthorityRetired {
					t.Fatal("process death lost its exact pending responsibility", original, err)
				}
				identity, _ := effects.ChannelLogoutOperationID(id)
				journal := f.selected.(effects.OutcomeStore)
				before, found, err := journal.GetExternalEffectOutcome(ctx, identity)
				wantBefore, wantAfter := effects.StateAuthorized, effects.StateTerminalFailure
				if phase == "sdk_deleted_before_settlement" {
					wantBefore, wantAfter = effects.StateLaunched, effects.StateOutcomeUncertain
				}
				if err != nil || !found || before.AttemptState != wantBefore {
					t.Fatal("process death did not retain exact journal launch evidence", before, err)
				}
				for range 2 {
					if _, err := f.selected.(effects.RecoveryStore).ReconcileExternalEffectAttempts(ctx,
						effects.NewRecoveryRequest(time.Now().UTC(), executionposture.Live)); err != nil {
						t.Fatal(err)
					}
				}
				after, found, err := journal.GetExternalEffectOutcome(ctx, identity)
				if err != nil || !found || after.AttemptState != wantAfter || after.OperationID != before.OperationID {
					t.Fatal("process recovery fabricated or replaced the logout outcome", after, err)
				}
				retained, err := f.selected.GetChannelTeardown(ctx, id)
				if err != nil || retained.Phase != channelonboarding.TeardownFailed || retained.Revision != original.Revision+1 ||
					retained.Logout == nil || *retained.Logout != *original.Logout {
					t.Fatal("process recovery lost or repeated frozen settlement", retained, err)
				}
				state, err := openSessionState(ctx, f.basePath, original.Logout.Account.ConnectionID, original.Logout.Account.AccountRef)
				if phase == "sdk_deleted_before_settlement" {
					if !errors.Is(err, errSessionAccount) || state != nil {
						t.Fatal("recovery restored already-deleted SDK bytes or lost cleanup possession", err)
					}
					return
				}
				if err != nil {
					t.Fatal("prelaunch recovery lost retained pairing", err)
				}
				t.Cleanup(func() {
					if err := state.close(context.Background()); err != nil {
						t.Error(err)
					}
				})
				device, err := state.device(ctx)
				if err != nil || device.ID == nil || device.ID.ToNonAD().String() != original.Logout.Account.AccountRef {
					t.Fatal("prelaunch recovery changed the original paired SDK identity", device, err)
				}
			})
		}
	}
}

func runNativeLogoutDeathChild(t *testing.T, config logoutProcessFixture) {
	t.Helper()
	f := &activeInputFixture{basePath: config.Directory, location: config.Location, peer: newSDKPeer(t)}
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
	manifest := sessionInputManifestFixture(t)
	f.catalog, err = providertriggers.NewCatalogSnapshot(providertriggers.CatalogEntry{Manifest: manifest,
		Identity: providertriggers.PackIdentity{ID: "provider.whatsapp.input", Version: "1.0.0", ManifestHash: packs.ManifestHash(manifest.SourceBytes()), Provenance: "test"}})
	if err != nil {
		t.Fatal(err)
	}
	f.channel = activeInputChannelFixture(t, f.catalog)
	source := sourceartifactfixture.Require(t, context.Background(), f.selected)
	f.ctx = authoractivity.WithScope(correlation.WithSourceArtifactFact(f.peer.ctx, source),
		authoractivity.BundleScope(f.operation.Coordinate.RuntimeInstanceID, f.operation.Coordinate.BundleHash))
	process := worklifetime.NewProcess()
	f.workOwner, err = process.NewRuntime(f.ctx, worklifetime.RuntimeIdentity{
		RuntimeInstanceID: f.operation.Coordinate.RuntimeInstanceID, BundleHash: f.operation.Coordinate.BundleHash})
	if err != nil {
		t.Fatal(err)
	}
	f.ctx = worklifetime.WithOccurrence(f.ctx, f.workOwner)
	f.state = sessionStateFixture(t, f.basePath, f.operation.SessionConnectionID, f.operation.SessionAccount.AccountRef)
	c := openRuntimeConnectionFixture(t, f)
	connectRuntimeConnectionFixture(t, f, c)
	op := reserveRuntimeLogoutFixture(t, f, c)
	ctx := logoutJournalFixture(t, f, op)
	owner := &logoutProcessDeathStore{Store: f.selected.(effects.Store), t: t, phase: config.Phase, original: c.state.currentOccurrence(), peer: f.peer}
	ctx = effects.WithController(ctx, effects.NewController(owner).WithExecutionPosture(executionposture.Live))
	if config.Phase == "authorized" {
		if err := c.DispatchSessionLogout(ctx, op); err != nil {
			t.Fatal(err)
		}
	} else {
		done := make(chan error, 1)
		go func() { done <- c.DispatchSessionLogout(ctx, op) }()
		frame := f.peer.next(t)
		if frame.node.Tag != "iq" || frame.node.Attrs["xmlns"] != "md" {
			t.Fatal("SDK death proof did not reach the actual unlink frame", frame.node)
		}
		f.peer.acknowledge(t, frame)
		if err := awaitOccurrenceProbe(t, f.peer.ctx, done); err != nil {
			t.Fatal(err)
		}
	}
	t.Fatal("SDK logout child missed its finite process-death boundary")
}

func killNativeLogoutAtBoundary(t *testing.T, config logoutProcessFixture) string {
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
	child := exec.CommandContext(ctx, binary, "-test.run=^TestWhatsAppLogoutJournalProcessDeathBothStores$", "-test.timeout=30s")
	child.Env = append(os.Environ(), "TEST_NATIVE_LOGOUT_DEATH="+string(encoded))
	var stderr, transcript bytes.Buffer
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
	for scanner.Scan() {
		line := scanner.Text()
		if transcript.Len() < 32<<10 {
			fmt.Fprintln(&transcript, line)
		}
		var id string
		if _, err := fmt.Sscanf(line, "NATIVE_LOGOUT_READY %s", &id); err != nil {
			continue
		}
		if err := child.Process.Kill(); err != nil {
			t.Fatal(err)
		}
		err = child.Wait()
		joined = true
		if err == nil {
			t.Fatal("logout process-death proof returned success")
		}
		return id
	}
	err = child.Wait()
	joined = true
	t.Fatalf("logout child missed %s boundary: scan=%v wait=%v stdout=%s stderr=%s", config.Phase, scanner.Err(), err, transcript.String(), stderr.String())
	return ""
}
