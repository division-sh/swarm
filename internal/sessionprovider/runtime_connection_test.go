//go:build linux || darwin

package sessionprovider

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	effects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/google/uuid"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

func openRuntimeConnectionFixture(t *testing.T, f *activeInputFixture) *RuntimeConnection {
	t.Helper()
	if err := f.state.close(f.ctx); err != nil {
		t.Fatal(err)
	}
	c, err := OpenRuntimeConnection(f.ctx, RuntimeConnectionOptions{Directory: f.basePath,
		Store: f.selected, OperationID: f.operation.OperationID, Plan: f.channel})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := c.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return c
}

func connectRuntimeConnectionFixture(t *testing.T, f *activeInputFixture, c *RuntimeConnection) {
	t.Helper()
	f.peer.attach(t, c.state.currentOccurrence().client)
	if err := c.Connect(f.ctx); err != nil || !c.state.currentOccurrence().client.WaitForConnection(5*time.Second) {
		t.Fatal("runtime-owned SDK connection did not connect", err)
	}
}

func TestWhatsAppRuntimeConnectionRetirementJoinsSDKBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			noise := *f.occurrence.client.Store.NoiseKey.Priv
			c := openRuntimeConnectionFixture(t, f)
			if _, err := c.ChannelExecution(f.ctx); err == nil {
				t.Fatal("installation without connected account granted execution")
			}
			connectRuntimeConnectionFixture(t, f, c)
			occurrence := c.state.currentOccurrence()
			provider, observed, err := c.ObserveSession(f.ctx)
			if err != nil || observed.Validate() != nil || !observed.Connected || observed.OccurrenceID != occurrence.occurrenceID ||
				observed.Admission != f.operation.SessionAccount || provider.RequireExecutable() != nil {
				t.Fatal("runtime health lost its exact connected SDK observation", err)
			}
			provider.CloseExecution()
			if *occurrence.client.Store.NoiseKey.Priv != noise || occurrence.occurrenceID == f.occurrence.occurrenceID {
				t.Fatal("runtime installation changed pairing or reused the SDK occurrence")
			}
			admitted, err := c.AdmitSessionAccount(f.ctx, f.operation.SessionAccount)
			if err != nil || admitted.Validate(f.ctx, f.operation.SessionAccount) != nil {
				t.Fatal("runtime connection lost exact native account admission", err)
			}
			admitted.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := f.workOwner.RetireAndWait(ctx); err != nil {
				t.Fatal(err)
			}
			if occurrence.ctx.Err() == nil || occurrence.client.IsConnected() || !c.state.closed {
				t.Fatal("runtime retirement completed before SDK, state and possession joined")
			}
			if _, err := c.ChannelExecution(context.Background()); err == nil {
				t.Fatal("retired connection granted a fresh execution handle")
			}
			if provider, observed, err := c.ObserveSession(context.Background()); err == nil || observed.Connected {
				provider.CloseExecution()
				t.Fatal("retired occurrence reported connected health")
			}
		})
	}
}

func TestWhatsAppRuntimeConnectionAdmissionRetainsRuntimeWorkBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			c := openRuntimeConnectionFixture(t, f)
			connectRuntimeConnectionFixture(t, f, c)
			admitted, err := c.AdmitSessionAccount(f.ctx, f.operation.SessionAccount)
			if err != nil {
				t.Fatal(err)
			}
			defer admitted.Close()
			canceled, cancel := context.WithCancel(context.Background())
			cancel()
			if err := f.workOwner.WaitForQuiescence(canceled); !errors.Is(err, context.Canceled) {
				t.Fatal("held native admission disappeared from runtime quiescence", err)
			}
			admitted.Close()
			ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			if err := f.workOwner.WaitForQuiescence(ctx); err != nil {
				t.Fatal("native account release did not settle runtime work", err)
			}
			if err := f.workOwner.Fence(); err != nil {
				t.Fatal(err)
			}
			if next, err := c.AdmitSessionAccount(f.ctx, f.operation.SessionAccount); err == nil {
				next.Close()
				t.Fatal("fenced runtime granted a fresh native admission")
			}
		})
	}
}

func TestWhatsAppRuntimeConnectionRefusesForeignSourceBeforePossessionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			for _, cell := range []string{"missing_runtime", "foreign_runtime", "missing_source", "canceled", "unknown_operation", "missing_plan"} {
				t.Run(cell, func(t *testing.T) {
					root := filepath.Join(t.TempDir(), "must-not-open")
					opts := RuntimeConnectionOptions{Directory: root, Store: f.selected, OperationID: f.operation.OperationID, Plan: f.channel}
					ctx := f.ctx
					switch cell {
					case "missing_runtime":
						ctx = context.Background()
					case "foreign_runtime":
						process := worklifetime.NewProcess()
						parent, err := process.NewRuntime(ctx, worklifetime.RuntimeIdentity{RuntimeInstanceID: uuid.NewString(), BundleHash: f.operation.Coordinate.BundleHash})
						if err != nil {
							t.Fatal(err)
						}
						defer process.Retire()
						defer parent.Retire()
						ctx = worklifetime.WithRuntimeOccurrence(ctx, parent)
					case "missing_source":
						ctx = worklifetime.WithRuntimeOccurrence(context.Background(), f.workOwner)
					case "canceled":
						var cancel context.CancelFunc
						ctx, cancel = context.WithCancel(ctx)
						cancel()
					case "unknown_operation":
						opts.OperationID = uuid.NewString()
					case "missing_plan":
						opts.Plan = packs.SatisfactionPlan{}
					}
					connection, err := OpenRuntimeConnection(ctx, opts)
					if connection != nil || err == nil {
						if connection != nil {
							_ = connection.Close(context.Background())
						}
						t.Fatal("foreign reservation reached private SDK possession", err)
					}
					if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
						t.Fatal("refused source created provider state", err)
					}
				})
			}
		})
	}
}

func TestWhatsAppRuntimeConnectionFailedCloseRetainsPossessionAndWork(t *testing.T) {
	f := newActiveInputFixture(t, "sqlite")
	c := openRuntimeConnectionFixture(t, f)
	connectRuntimeConnectionFixture(t, f, c)
	entered, finish := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(finish) })
	done := make(chan error, 1)
	go func() {
		done <- c.state.currentOccurrence().client.Store.EventBuffer.DoDecryptionTxn(context.Background(), func(context.Context) error {
			close(entered)
			<-finish
			return nil
		})
	}()
	<-entered
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.Close(canceled); !errors.Is(err, context.Canceled) {
		t.Fatal("unjoined runtime connection reported a completed close", err)
	}
	if f.workOwner.ActiveCount() == 0 || c.state.database == nil || c.state.closed {
		t.Fatal("failed SDK join dropped runtime or possession evidence")
	}
	other, err := openSessionState(context.Background(), f.basePath, f.operation.SessionAccount.ConnectionID, f.operation.SessionAccount.AccountRef)
	if !errors.Is(err, errSessionPossession) {
		if other != nil {
			_ = other.close(context.Background())
		}
		t.Fatal("failed close let a successor acquire pairing", err)
	}
	once.Do(func() { close(finish) })
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	if err := c.Close(ctx); err != nil || f.workOwner.ActiveCount() != 0 {
		t.Fatal("complete cleanup did not release runtime responsibility", err)
	}
}

func TestWhatsAppRuntimeConnectionSuppliesJournalBackedExecutionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newNativeConfirmationFixture(t, backend)
			c := openRuntimeConnectionFixture(t, f)
			connectRuntimeConnectionFixture(t, f, c)
			f.state, f.occurrence = c.state, c.state.currentOccurrence()
			handle, err := c.ChannelExecution(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			ctx := f.confirmationContext(t)
			toolID, tool, err := f.channel.ConnectorOperation("deliver")
			if err != nil {
				t.Fatal(err)
			}
			const text = "runtime-owned journal send"
			input := map[string]any{"destination": f.binding.ConversationRef, "text": text}
			done := make(chan error, 1)
			go func() {
				_, err := handle.DeliverChannelConfirmation(ctx, f.operation.OperationID, "deliver", toolID, tool, input, nil)
				done <- err
			}()
			frame := f.peer.next(t)
			journal := f.selected.(effects.OutcomeStore)
			launched, found, err := journal.GetExternalEffectOutcome(ctx, f.operation.ConfirmationOperationID)
			if err != nil || !found || launched.AttemptState != effects.StateLaunched {
				t.Fatal("installed SDK send preceded its journal launch", err)
			}
			requireDecryptedChannelSend(t, f, frame, text)
			f.peer.acknowledge(t, frame)
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-f.peer.ctx.Done():
				t.Fatal("runtime-owned SDK write did not settle")
			}
			settled, found, err := journal.GetExternalEffectOutcome(ctx, f.operation.ConfirmationOperationID)
			if err != nil || !found || !settled.TerminalSuccess() {
				t.Fatal("installed send lost the compiled settlement owner", err)
			}
			if _, err := handle.DeliverChannelConfirmation(ctx, f.operation.OperationID, "deliver", toolID, tool, input, nil); err == nil {
				t.Fatal("installed connection replayed a terminal send")
			}
		})
	}
}

func TestWhatsAppRuntimeConnectionReconstructionAndNilCloseRefuse(t *testing.T) {
	f := newActiveInputFixture(t, "sqlite")
	c := openRuntimeConnectionFixture(t, f)
	encoded, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	var restored RuntimeConnection
	if err := json.Unmarshal(encoded, &restored); err != nil {
		t.Fatal(err)
	}
	if err := restored.Connect(f.ctx); err == nil {
		t.Fatal("reconstructed connection acquired SDK state")
	}
	if _, err := restored.ChannelExecution(f.ctx); err == nil {
		t.Fatal("reconstructed connection granted execution")
	}
	if err := restored.Close(context.Background()); err == nil {
		t.Fatal("reconstructed close fabricated completion evidence")
	}
	if err := c.Close(nil); err == nil || c.state.closed {
		t.Fatal("nil close context abandoned joined responsibility")
	}
}

func TestWhatsAppRuntimeConnectionUninstalledIncomingNeverAcknowledges(t *testing.T) {
	f := newActiveInputFixture(t, "sqlite")
	c := openRuntimeConnectionFixture(t, f)
	connectRuntimeConnectionFixture(t, f, c)
	occurrence := c.state.currentOccurrence()
	device, err := c.state.device(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	message := encryptedMessageFixture(t, device, types.NewJID("100000000003", types.DefaultUserServer),
		&waE2E.Message{Conversation: proto.String("not acknowledged without an incoming owner")})
	f.peer.mu.Lock()
	socket := f.peer.peers[len(f.peer.peers)-1]
	f.peer.mu.Unlock()
	if err := socket.send(f.peer.ctx, message); err != nil {
		t.Fatal(err)
	}
	select {
	case <-occurrence.callbacks.drained:
	case <-f.peer.ctx.Done():
		t.Fatal("missing incoming implementation did not fence the SDK callback")
	}
	if !errors.Is(occurrence.callbacks.currentFailure(), errRuntimeIngressUnavailable) {
		t.Fatal("incoming implementation gap was not reported")
	}
	if err := c.Close(f.peer.ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case frame := <-f.peer.protocol:
		t.Fatalf("uninstalled incoming path acknowledged data: %+v", frame.node)
	default:
	}
	fixture, _ := openSDKStoreFixture(t, filepath.Join(c.state.directory.path, "provider.db"))
	if count, err := fixture.CallbackFailureCount(context.Background(), c.operation.SessionAccount.ConnectionID, occurrence.occurrenceID, "capture_failed"); err != nil || count != 1 {
		t.Fatal("unacknowledged incoming failure lost durable evidence", count, err)
	}
}
