//go:build linux || darwin

package sessionprovider

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func (f *activeInputFixture) businessHandoff(t *testing.T) *sessionBusinessHandoff {
	t.Helper()
	return &sessionBusinessHandoff{input: f.owner, trigger: f.trigger, alias: "whatsapp", bus: f.publicationBus(t),
		store: f.selected.(sessionBusinessStore), posture: executionposture.Live}
}

// Reopen the same provider-owned directory, then use the ordinary selected
// coordinate-rebind operation. No capture or pairing bytes are copied or edited.
func (f *activeInputFixture) restartBusinessConnection(t *testing.T) {
	t.Helper()
	if err := f.state.close(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.state = sessionStateFixture(t, f.basePath, f.operation.SessionAccount.ConnectionID, f.operation.SessionAccount.AccountRef)
	f.peer = newSDKPeer(t)
	coordinate := f.operation.Coordinate
	coordinate.RuntimeInstanceID = uuid.NewString()
	coordinate.ContextPublicationGeneration++
	var err error
	f.operation, err = f.selected.AdvanceChannelOnboarding(context.Background(), channelonboarding.AdvanceRequest{
		OperationID: f.operation.OperationID, ExpectedRevision: f.operation.Revision, Phase: f.operation.Phase,
		RebindCoordinate: &coordinate, Now: time.Now().UTC().Truncate(time.Microsecond)})
	if err != nil {
		t.Fatal(err)
	}
	f.activation, err = f.selected.GetConnectedChannelActivation(context.Background(), f.operation.SlotKey)
	if err != nil {
		t.Fatal(err)
	}
	f.source.Coordinate = coordinate
	f.installBusinessOccurrence(t)
}

func (f *activeInputFixture) installBusinessOccurrence(t *testing.T) {
	t.Helper()
	source, _ := correlation.SourceArtifactFactFromContext(f.ctx)
	f.ctx = authoractivity.WithScope(correlation.WithSourceArtifactFact(f.peer.ctx, source),
		authoractivity.BundleScope(f.operation.Coordinate.RuntimeInstanceID, source.BundleHash()))
	process := worklifetime.NewProcess()
	owner, err := process.NewRuntime(f.ctx, worklifetime.RuntimeIdentity{RuntimeInstanceID: f.operation.Coordinate.RuntimeInstanceID, BundleHash: source.BundleHash()})
	if err != nil {
		t.Fatal(err)
	}
	f.workOwner = owner
	f.ctx = worklifetime.WithOccurrence(f.ctx, owner)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := owner.RetireAndWait(ctx); err != nil {
			t.Error(err)
		}
		process.Retire()
		if _, err := process.Join(ctx); err != nil {
			t.Error(err)
		}
	})
	f.spool, err = newCaptureStore(f.ctx, f.state.database, f.operation.SessionAccount.ConnectionID)
	if err != nil {
		t.Fatal(err)
	}
	f.occurrence, err = f.state.newOccurrence(f.ctx, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	f.setScope(channelonboarding.SessionInputBusiness)
	f.bindCaptureCallbacks(t)
	f.peer.attach(t, f.occurrence.client)
	if err := f.occurrence.connect(); err != nil || !f.occurrence.client.WaitForConnection(5*time.Second) {
		t.Fatal("reopened native connection", err)
	}
	reader, err := newSessionInputReader(f.state, f.spool)
	if err != nil {
		t.Fatal(err)
	}
	f.owner, err = newSessionInputOwner(f.selected, reader, f.operation.OperationID)
	if err != nil {
		t.Fatal(err)
	}
}

func requireNativeBusinessReceipt(t *testing.T, f *activeInputFixture, event capturedEvent) {
	t.Helper()
	identity, err := event.PublicationIdentity()
	if err != nil {
		t.Fatal(err)
	}
	record, found, err := f.selected.(sessionBusinessStore).LoadInboundPublicationByIdentity(f.ctx, identity)
	if err != nil || !found || record.OutputCount != 2 {
		t.Fatalf("original native receipt: found=%t outputs=%d %v", found, record.OutputCount, err)
	}
	original, err := publicationCaptureProvenance(record.Request)
	if err != nil || !original.SameCapture(event) {
		t.Fatal("resumption rewrote original capture authority or bytes", err)
	}
	var normalized map[string]any
	if err := json.Unmarshal(record.Events[1].Event.Payload(), &normalized); err != nil || normalized["text"] != "unfinished native message" {
		t.Fatalf("native BODY projection changed: %v %v", normalized, err)
	}
	rows, err := f.spool.pending(f.ctx)
	if err != nil || len(rows) != 1 || !rows[0].SameCapture(f.claimEvent) {
		t.Fatalf("business drain lost setup history or retained completed business: %d %v", len(rows), err)
	}
}

func TestWhatsAppNativeBusinessHandoffRestartBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, phase := range []string{"captured", "staged"} {
			t.Run(backend+"/"+phase, func(t *testing.T) {
				f := newActiveInputFixture(t, backend)
				f.activate(t)
				event, admitted := f.receive(t, "unfinished native message")
				if phase == "staged" {
					eventBus := f.publicationBus(t)
					prepared, err := prepareSessionBusinessPublication(f.ctx, admitted, f.trigger, "whatsapp", eventBus, f.selected.(sessionBusinessStore), executionposture.Live)
					if err != nil {
						t.Fatal(err)
					}
					if err := f.spool.stagePublication(f.ctx, event, prepared.command.Request); err != nil {
						t.Fatal(err)
					}
					if err := eventBus.AbandonInboundDeliveryPlan(f.ctx, prepared.plan); err != nil {
						t.Fatal(err)
					}
				}
				admitted.Close()
				f.restartBusinessConnection(t)
				if f.occurrence.occurrenceID == event.OccurrenceID || f.operation.Coordinate.RuntimeInstanceID == event.Source.Coordinate.RuntimeInstanceID {
					t.Fatal("restart reused dead execution identity")
				}
				if _, err := f.owner.admit(f.ctx, SessionInputReference{ConnectionID: event.Scope.Session.ConnectionID, OccurrenceID: event.OccurrenceID,
					Conversation: event.Conversation, EventID: event.EventID, Kind: event.Kind}); err == nil {
					t.Fatal("fresh-input admission adopted a dead occurrence")
				}
				handoff := f.businessHandoff(t)
				if err := handoff.drain(f.ctx); err != nil {
					t.Fatal("native unfinished resumption", err)
				}
				requireNativeBusinessReceipt(t, f, event)
				if err := handoff.drain(f.ctx); err != nil {
					t.Fatal("repeated drain", err)
				}
				requireNativeBusinessReceipt(t, f, event)
			})
		}
	}
}

func TestWhatsAppNativeBusinessHandoffRefusalRetainsCaptureBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, change := range []string{"disconnected", "unbind", "retired_activation", "corrupt_capture", "canceled"} {
			t.Run(backend+"/"+change, func(t *testing.T) {
				f := newActiveInputFixture(t, backend)
				f.activate(t)
				event, admitted := f.receive(t, "unfinished native message")
				admitted.Close()
				handoff := f.businessHandoff(t)
				ctx := f.ctx
				switch change {
				case "disconnected":
					if err := f.state.retireOccurrence(ctx); err != nil {
						t.Fatal(err)
					}
				case "unbind":
					if _, _, err := f.identities.Unbind(ctx, f.operation.Interface.Selector, f.binding.Revision, uuid.NewString(), uuid.NewString(), time.Now().UTC()); err != nil {
						t.Fatal(err)
					}
				case "retired_activation":
					if _, err := f.selected.RetireConnectedChannelActivation(ctx, channelonboarding.RetireActivationRequest{
						SlotKey: f.operation.SlotKey, ExpectedActivationRevision: f.activation.Revision, Reason: "handoff refusal", Now: time.Now().UTC()}); err != nil {
						t.Fatal(err)
					}
				case "corrupt_capture":
					if err := f.state.close(ctx); err != nil {
						t.Fatal(err)
					}
					fixture, _ := openCaptureFixture(t, filepath.Join(f.state.directory.path, "provider.db"), event.Scope.Session.ConnectionID)
					if err := fixture.CorruptFirstCaptureConversation(ctx); err != nil {
						t.Fatal(err)
					}
					if err := fixture.Close(); err != nil {
						t.Fatal(err)
					}
					f.restartBusinessConnection(t)
					ctx, handoff = f.ctx, f.businessHandoff(t)
				case "canceled":
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				}
				if err := handoff.drain(ctx); err == nil {
					t.Fatal("refused handoff reported completion")
				}
				identity, _ := event.PublicationIdentity()
				if _, found, err := f.selected.(sessionBusinessStore).LoadInboundPublicationByIdentity(f.ctx, identity); err != nil || found {
					t.Fatalf("refusal persisted publication: %t %v", found, err)
				}
				if change != "corrupt_capture" {
					rows, err := f.spool.pending(f.ctx)
					if err != nil || len(rows) != 2 || !bytes.Equal(rows[1].Body, event.Body) || !rows[1].SameCapture(event) {
						t.Fatal("refused handoff changed pending evidence", err)
					}
				}
			})
		}
	}
}

func TestWhatsAppNativeBusinessHandoffRollbackRetryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			f.activate(t)
			event, admitted := f.receive(t, "unfinished native message")
			admitted.Close()
			handoff := f.businessHandoff(t)
			if err := storetest.SetSessionPublicationInsertFault(f.ctx, f.selected, true); err != nil {
				t.Fatal(err)
			}
			faultEnabled := true
			t.Cleanup(func() {
				if faultEnabled {
					if err := storetest.SetSessionPublicationInsertFault(context.Background(), f.selected, false); err != nil {
						t.Error(err)
					}
				}
			})
			if err := handoff.drain(f.ctx); err == nil {
				t.Fatal("rolled-back native handoff reported success")
			}
			rows, err := f.spool.pendingPublications(f.ctx)
			if err != nil || len(rows) != 2 || rows[1].request == nil || !rows[1].event.SameCapture(event) {
				t.Fatal("rollback erased original staged authority", err)
			}
			frozen := bytes.Clone(rows[1].requestBytes)
			identity, _ := event.PublicationIdentity()
			if _, found, err := f.selected.(sessionBusinessStore).LoadInboundPublicationByIdentity(f.ctx, identity); err != nil || found {
				t.Fatal("rollback left a publication receipt", err)
			}
			if err := storetest.SetSessionPublicationInsertFault(f.ctx, f.selected, false); err != nil {
				t.Fatal(err)
			}
			faultEnabled = false
			f.restartBusinessConnection(t)
			if err := f.businessHandoff(t).drain(f.ctx); err != nil {
				t.Fatal("original rollback retry", err)
			}
			record, found, err := f.selected.(sessionBusinessStore).LoadInboundPublicationByIdentity(f.ctx, identity)
			actual, encodeErr := publicationRequestBytes(record.Request)
			if err != nil || !found || encodeErr != nil || !bytes.Equal(actual, frozen) {
				t.Fatalf("retry changed staged request: found=%t read=%v encode=%v", found, err, encodeErr)
			}
			requireNativeBusinessReceipt(t, f, event)
		})
	}
}

func TestWhatsAppNativeBusinessRedeliveryAfterRebindBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			f.activate(t)
			event, admitted := f.receive(t, "unfinished native message")
			admitted.Close()
			f.restartBusinessConnection(t)
			var payload incomingPayload
			if err := json.Unmarshal(event.Body, &payload); err != nil {
				t.Fatal(err)
			}
			duplicate := f.receiveCaptureSDK(t, "unfinished native message", event.EventID, time.UnixMilli(payload.ProviderTimestampMS))
			rows, err := f.spool.pending(f.ctx)
			if err != nil || len(rows) != 2 || !rows[1].SameCapture(event) {
				t.Fatal("pending SDK redelivery overwrote original capture or created a second row", err)
			}
			if original, _ := event.PublicationFingerprint(); original == "" {
				t.Fatal("original fingerprint absent")
			} else if replay, _ := duplicate.PublicationFingerprint(); replay != original {
				t.Fatal("process-coordinate refresh changed stable publication identity")
			}
			if err := f.businessHandoff(t).drain(f.ctx); err != nil {
				t.Fatal("unfinished redelivery recovery", err)
			}
			f.receiveCaptureSDK(t, "unfinished native message", event.EventID, time.UnixMilli(payload.ProviderTimestampMS))
			if err := f.state.retireOccurrence(f.ctx); err != nil {
				t.Fatal(err)
			}
			if err := f.businessHandoff(t).drain(f.ctx); err != nil {
				t.Fatal("historical native redelivery reminted authority", err)
			}
			requireNativeBusinessReceipt(t, f, event)
		})
	}
}
