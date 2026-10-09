package whatsapp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/bus/bustest"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimeinbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

type sessionPublicationStore interface {
	sourceartifactfixture.Writer
	runtimeinbound.Runner
	runtimepipeline.StandingServicePersistence
	runtimebus.EventStore
	runtimepipeline.FlowConstructionPublicationReader
	runtimereplycontext.Store
	runtimerunlifecycle.OperationOwner
	runtimedelivery.Store
	runtimebus.FlowInstanceRoutePersistence
	runtimebus.FlowInstanceRouteRecordReader
	runtimebus.FlowInstanceRouteSetPersistence
	runtimebus.FlowInstanceRouteTopologyPersistence
	runtimebus.FlowInstanceRouteRollbackPersistence
	runtimebus.ActiveAgentDescriptorLister
	runtimebus.ActiveFlowInstanceDescriptorLister
	runtimebus.SelectedRunTargetOwnerLister
	runtimebus.PreparedPublishEventReader
	runtimebus.TargetFailureDeadLetterRecorder
	runtimebus.RunOriginReader
	runtimerunlifecycle.StandingRestartDispositionReader
	PipelineObligations() runtimepipelineobligation.Store
	SetEventPayloadAdmitter(runtimebus.PayloadAdmitter)
	RegisterAuthorActivityEventCatalog(runtimeauthoractivity.Scope, []runtimeauthoractivity.EventDescriptor) (*runtimeauthoractivity.EventCatalogLease, error)
}

type sessionPublicationFixture struct {
	ctx       context.Context
	selected  sessionPublicationStore
	db        *sql.DB
	bus       *runtimebus.EventBus
	manifest  providertriggers.Manifest
	catalog   *providertriggers.CatalogSnapshot
	identity  providertriggers.PackIdentity
	candidate runtimepipeline.StandingServiceCandidate
	standing  runtimepipeline.StandingServiceReconciliation
	sequence  int64
	plans     int
	location  string
}

// Session/account admission and enabled-binding selection are supplied fixture
// facts. Store bootstrap, standing admission, normalization, catalog output
// authorization, atomic commit and integrity readback are real. No SDK runs.
func newSessionPublicationFixture(t *testing.T, backend string) *sessionPublicationFixture {
	t.Helper()
	f := &sessionPublicationFixture{manifest: incomingNormalizationFixture(t)}
	if backend == "sqlite" {
		f.selected = storetest.StartSQLiteRuntimeStore(t)
	} else {
		location, db, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		f.selected = storetest.AdmitPostgresRuntimeStore(t, db)
		f.location = location
	}
	f.db = storetest.Database(f.selected)
	if backend == "sqlite" {
		var sequence int
		var name string
		if err := f.db.QueryRow(`PRAGMA database_list`).Scan(&sequence, &name, &f.location); err != nil {
			t.Fatal(err)
		}
	}
	source := sourceartifactfixture.Require(t, context.Background(), f.selected)
	runtimeID := uuid.NewString()
	process := worklifetime.NewProcess()
	owner, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{RuntimeInstanceID: runtimeID, BundleHash: source.BundleHash()})
	if err != nil {
		t.Fatal(err)
	}
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
	f.ctx = runtimecorrelation.WithSourceArtifactFact(context.Background(), source)
	f.ctx = runtimeauthoractivity.WithScope(f.ctx, runtimeauthoractivity.BundleScope(runtimeID, source.BundleHash()))
	f.ctx = worklifetime.WithOccurrence(f.ctx, owner)
	digest := sha256.Sum256(f.manifest.SourceBytes())
	f.identity = providertriggers.PackIdentity{ID: "provider.whatsapp.fixture", Version: "1.0.0",
		ManifestHash: fmt.Sprintf("sha256:%x", digest), Provenance: "platform"}
	f.catalog, err = providertriggers.NewCatalogSnapshot(providertriggers.CatalogEntry{
		Identity: f.identity, Manifest: f.manifest, Source: "isolated session publication fixture"})
	if err != nil {
		t.Fatal(err)
	}
	descriptors := []runtimeauthoractivity.EventDescriptor{{EventType: "inbound.whatsapp", Disposition: runtimeauthoractivity.StoryDifferent}}
	for _, kind := range []string{"message", "edit", "revoke"} {
		descriptors = append(descriptors, runtimeauthoractivity.EventDescriptor{EventType: "inbound.whatsapp." + kind, Disposition: runtimeauthoractivity.StoryDifferent})
	}
	scope, _ := runtimeauthoractivity.ScopeFromContext(f.ctx)
	lease, err := f.selected.RegisterAuthorActivityEventCatalog(scope, descriptors)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(lease.Release)
	// Payload-to-catalog binding is supplied by the existing event fixture owner;
	// the pack normalizer and exact output authorization are not replaced.
	payload := func(_ context.Context, event events.Event, flow string) (events.PayloadAdmission, error) {
		return eventtest.PayloadAdmission(event, flow, string(event.Type()))
	}
	f.selected.SetEventPayloadAdmitter(payload)
	authority, err := runtimedelivery.NewNormalExecutionAuthority(source, runtimeID, 1)
	if err != nil {
		t.Fatal(err)
	}
	f.bus, err = runtimebus.NewEventBusWithOptions(f.selected, runtimebus.EventBusOptions{
		ExecutionPosture: executionposture.Live, SourceArtifactFact: source, RuntimeInstanceID: runtimeID,
		WorkOwner: owner, ReceiverExecution: eventreceiver.NormalExecution(), DeliveryAuthority: authority,
		PipelineObligations: f.selected.PipelineObligations(), PayloadAdmitter: payload, ProviderOutputVerifier: f.catalog,
		Durable: runtimebus.DurableDependencies{ReplyContext: f.selected, RunLifecycle: f.selected, DeliveryLifecycle: f.selected,
			ConstructionPublications: f.selected,
			FlowRoutes:               f.selected, FlowRouteRecords: f.selected, FlowRouteSets: f.selected, FlowRouteTopology: f.selected,
			FlowRouteRollback: f.selected, ActiveAgents: f.selected, ActiveFlows: f.selected, TargetOwners: f.selected,
			PreparedEvents: f.selected, TargetFailureRecorder: f.selected, RunOrigins: f.selected, StandingRestarts: f.selected},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.bus.SetDeliveryContinuationOwner(bustest.NewDeliveryContinuationOwner(false)); err != nil {
		t.Fatal(err)
	}
	flow := "."
	f.candidate = runtimepipeline.StandingServiceCandidate{BindingEnabled: true, FlowPath: flow,
		ServiceID: runtimeflowidentity.StandingServiceID(flow), Source: source}
	f.standing, err = f.selected.ReconcileStandingService(f.ctx, f.candidate)
	if err != nil {
		t.Fatal(err)
	}
	f.sequence, err = f.selected.PublishStandingService(f.ctx, f.standing.ServiceID, f.standing.RunID, f.standing.Generation)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *sessionPublicationFixture) capture(t *testing.T) capturedEvent {
	return f.captureVariant(t, "message", false, false)
}

func (f *sessionPublicationFixture) captureVariant(t *testing.T, kind string, group, reply bool) capturedEvent {
	t.Helper()
	scope := captureFixture(t).Scope
	scope.PublicationBinding = runtimeinbound.BindingGeneration{ServiceID: f.standing.ServiceID,
		RunID: f.standing.RunID, Generation: f.standing.Generation}
	scope.Source.BundleHash = f.candidate.Source.BundleHash()
	message := incomingMessageFixture()
	message.Info.ID = "DELIVERY_" + uuid.NewString()
	if group {
		message.Info.Chat = types.NewJID("100000000003-1", types.GroupServer)
		message.Info.IsGroup = true
	}
	if reply {
		message.Message = &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{
			Text: proto.String("current reply"), ContextInfo: &waE2E.ContextInfo{
				StanzaID: proto.String("ORIGINAL"), QuotedMessage: &waE2E.Message{Conversation: proto.String("quoted content")}}}}
	}
	if kind != "message" {
		protocol := &waE2E.ProtocolMessage{Key: &waCommon.MessageKey{ID: proto.String("ORIGINAL")}}
		if kind == "edit" {
			protocol.Type = waE2E.ProtocolMessage_MESSAGE_EDIT.Enum()
			protocol.EditedMessage = message.Message
			message.IsEdit = true
		} else {
			protocol.Type = waE2E.ProtocolMessage_REVOKE.Enum()
		}
		message.Message = &waE2E.Message{ProtocolMessage: protocol}
	}
	event, err := captureSDKMessage(scope, uuid.NewString(), message)
	if err != nil {
		t.Fatal(err)
	}
	return event
}

func TestWhatsAppSessionPublicationRedeliveryLifecycleBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newSessionPublicationFixture(t, backend)
			for _, phase := range []string{"before_stage", "after_stage", "commit_result_lost", "after_retire"} {
				for _, fresh := range []bool{false, true} {
					for _, reopen := range []bool{false, true} {
						t.Run(fmt.Sprintf("%s/fresh=%t/reopen=%t", phase, fresh, reopen), func(t *testing.T) {
							event := f.capture(t)
							path := filepath.Join(t.TempDir(), "incoming.db")
							db, spool := openCaptureFixture(t, path, event.Scope.Session.ConnectionID)
							if err := spool.capture(f.ctx, event); err != nil {
								t.Fatal(err)
							}
							command := f.command(t, event)
							plans := f.plans
							if phase != "before_stage" {
								if err := spool.stagePublication(f.ctx, event, command.Request); err != nil {
									t.Fatal(err)
								}
							}
							if phase == "commit_result_lost" || phase == "after_retire" {
								if _, err := f.selected.CommitInboundPublication(f.ctx, command); err != nil {
									t.Fatal(err)
								}
							}
							if phase == "after_retire" {
								if err := spool.retirePublished(f.ctx, event, f.selected); err != nil {
									t.Fatal(err)
								}
							}
							if reopen {
								if err := db.Close(); err != nil {
									t.Fatal(err)
								}
								_, spool = openCaptureFixture(t, path, event.Scope.Session.ConnectionID)
							}
							duplicate := event
							if fresh {
								duplicate.OccurrenceID = uuid.NewString()
							}
							if err := spool.capture(f.ctx, duplicate); err != nil {
								t.Fatal(err)
							}
							pending, err := spool.pending(f.ctx)
							if err != nil || len(pending) != 1 {
								t.Fatalf("duplicate capture: count=%d err=%v", len(pending), err)
							}
							settled, err := spool.reconcilePublished(f.ctx, pending[0], f.selected)
							if err != nil {
								t.Fatal(err)
							}
							if phase == "before_stage" || phase == "after_stage" {
								if settled || !pending[0].sameCapture(event) {
									t.Fatal("uncommitted duplicate invented history or replaced occurrence")
								}
								if err := spool.stagePublication(f.ctx, pending[0], command.Request); err != nil {
									t.Fatal(err)
								}
								if result, err := f.selected.CommitInboundPublication(f.ctx, command); err != nil || !result.Acknowledged || !result.Record.Created {
									t.Fatalf("first commit = %+v, %v", result, err)
								}
								if err := spool.retirePublished(f.ctx, pending[0], f.selected); err != nil {
									t.Fatal(err)
								}
							} else if !settled {
								t.Fatal("committed duplicate failed historical acknowledgment")
							}
							result, err := f.selected.CommitInboundPublication(f.ctx, command)
							if err != nil || !result.Acknowledged || result.Record.Created || len(result.Publications) != 0 || f.plans != plans {
								t.Fatalf("duplicate created another publication/dispatch plan: %+v %v", result, err)
							}
							original, err := publicationCaptureProvenance(result.Record.Request)
							if err != nil || !original.sameCapture(event) {
								t.Fatal("history lost exact original capture occurrence", err)
							}
							pending, err = spool.pending(f.ctx)
							if err != nil || len(pending) != 0 {
								t.Fatal("verified duplicate remained pending", err)
							}
							if err := f.selected.ValidateInboundPublicationIntegrity(f.ctx); err != nil {
								t.Fatal(err)
							}
						})
					}
				}
			}
		})
	}
}

func TestWhatsAppSessionPublicationVariantsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newSessionPublicationFixture(t, backend)
			for _, kind := range []string{"message", "edit", "revoke"} {
				for _, group := range []bool{false, true} {
					for _, reply := range []bool{false, true} {
						if kind == "revoke" && reply {
							continue
						}
						t.Run(fmt.Sprintf("%s/group=%t/reply=%t", kind, group, reply), func(t *testing.T) {
							event := f.captureVariant(t, kind, group, reply)
							_, spool := openCaptureFixture(t, filepath.Join(t.TempDir(), "incoming.db"), event.Scope.Session.ConnectionID)
							command := f.command(t, event)
							if err := spool.capture(f.ctx, event); err != nil {
								t.Fatal(err)
							}
							if err := spool.stagePublication(f.ctx, event, command.Request); err != nil {
								t.Fatal(err)
							}
							if result, err := f.selected.CommitInboundPublication(f.ctx, command); err != nil || !result.Acknowledged || result.Record.OutputCount != 2 {
								t.Fatalf("variant commit = %+v %v", result, err)
							}
							if err := spool.retirePublished(f.ctx, event, f.selected); err != nil {
								t.Fatal(err)
							}
							record, found, err := f.selected.LoadInboundPublicationByIdentity(f.ctx, command.Request.Identity())
							if err != nil || !found || len(record.Events) != 2 || string(record.Events[1].Event.Type()) != "inbound.whatsapp."+kind ||
								!record.Events[1].Authorization.Valid() || !bytes.Equal(record.Events[0].Event.Payload(), event.Body) {
								t.Fatalf("variant readback = %+v found=%t err=%v", record, found, err)
							}
							if err := f.catalog.VerifyProviderOutputAuthorization(record.Events[1].Authorization); err != nil {
								t.Fatal(err)
							}
						})
					}
				}
			}
		})
	}
}

func (f *sessionPublicationFixture) rejectPublication(t *testing.T, backend string) func() {
	t.Helper()
	if backend == "sqlite" {
		if _, err := f.db.Exec(`CREATE TRIGGER reject_whatsapp_publication BEFORE INSERT ON inbound_publication_events
			BEGIN SELECT RAISE(ABORT,'publication rollback proof'); END`); err != nil {
			t.Fatal(err)
		}
		return func() {
			if _, err := f.db.Exec(`DROP TRIGGER reject_whatsapp_publication`); err != nil {
				t.Error(err)
			}
		}
	}
	if _, err := f.db.Exec(`CREATE FUNCTION reject_whatsapp_publication() RETURNS trigger LANGUAGE plpgsql AS
		$$ BEGIN RAISE EXCEPTION 'publication rollback proof'; END $$`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.Exec(`CREATE TRIGGER reject_whatsapp_publication BEFORE INSERT ON inbound_publication_events
		FOR EACH ROW EXECUTE FUNCTION reject_whatsapp_publication()`); err != nil {
		t.Fatal(err)
	}
	return func() {
		if _, err := f.db.Exec(`DROP TRIGGER reject_whatsapp_publication ON inbound_publication_events`); err != nil {
			t.Error(err)
		}
		if _, err := f.db.Exec(`DROP FUNCTION reject_whatsapp_publication()`); err != nil {
			t.Error(err)
		}
	}
}

func TestWhatsAppSessionPublicationRollbackAndHistoricalResetBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newSessionPublicationFixture(t, backend)
			event := f.capture(t)
			path := filepath.Join(t.TempDir(), "incoming.db")
			db, spool := openCaptureFixture(t, path, event.Scope.Session.ConnectionID)
			if err := spool.capture(f.ctx, event); err != nil {
				t.Fatal(err)
			}
			command := f.command(t, event)
			if err := spool.stagePublication(f.ctx, event, command.Request); err != nil {
				t.Fatal(err)
			}
			drop := f.rejectPublication(t, backend)
			result, err := f.selected.CommitInboundPublication(f.ctx, command)
			drop()
			if err == nil || result.Acknowledged {
				t.Fatal("rolled-back compound publication acknowledged capture", err)
			}
			if found, err := spool.reconcilePublished(f.ctx, event, f.selected); err != nil || found {
				t.Fatal("rollback produced historical completion", err)
			}
			if record, found, err := f.selected.LoadInboundPublicationByIdentity(f.ctx, command.Request.Identity()); err != nil || found {
				t.Fatalf("rollback left publication: %+v found=%t err=%v", record, found, err)
			}
			rows, err := spool.readPendingRows(f.ctx, db)
			if err != nil || len(rows) != 1 || !rows[0].event.sameCapture(event) || !equalCapturePublicationRequest(t, rows[0].request, command.Request) {
				t.Fatal("rollback lost original request responsibility", err)
			}
			if _, err := f.selected.CommitInboundPublication(f.ctx, command); err != nil {
				t.Fatal(err)
			}
			if err := spool.retirePublished(f.ctx, event, f.selected); err != nil {
				t.Fatal(err)
			}
			if _, err := f.selected.ResetStandingService(f.ctx, runtimepipeline.StandingServiceOperation{
				ServiceID: f.candidate.ServiceID, Actor: "historical-duplicate-proof", ExecutionPosture: executionposture.Live}); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			_, spool = openCaptureFixture(t, path, event.Scope.Session.ConnectionID)
			duplicate := event
			duplicate.OccurrenceID = uuid.NewString()
			if err := spool.capture(f.ctx, duplicate); err != nil {
				t.Fatal(err)
			}
			if settled, err := spool.reconcilePublished(f.ctx, duplicate, f.selected); err != nil || !settled || f.plans != 1 {
				t.Fatalf("reset caused re-planning/adoption: settled=%t plans=%d err=%v", settled, f.plans, err)
			}
			if result, err := f.selected.CommitInboundPublication(f.ctx, command); err != nil || !result.Acknowledged || result.Record.Created || len(result.Publications) != 0 {
				t.Fatalf("historical duplicate after reset = %+v err=%v", result, err)
			}
		})
	}
}

type publicationReadFault struct {
	publicationReader
	err error
}

func (f publicationReadFault) LoadInboundPublicationByIdentity(context.Context, runtimeinbound.Identity) (runtimeinbound.Record, bool, error) {
	return runtimeinbound.Record{}, false, f.err
}

func TestWhatsAppHistoricalPublicationFailuresRetainCaptureBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newSessionPublicationFixture(t, backend)
			event := f.capture(t)
			command := f.command(t, event)
			if _, err := f.selected.CommitInboundPublication(f.ctx, command); err != nil {
				t.Fatal(err)
			}
			duplicate := event
			duplicate.OccurrenceID = uuid.NewString()
			db, spool := openCaptureFixture(t, filepath.Join(t.TempDir(), "incoming.db"), event.Scope.Session.ConnectionID)
			if err := spool.capture(f.ctx, duplicate); err != nil {
				t.Fatal(err)
			}
			readFailure := errors.New("publication read unavailable")
			if settled, err := spool.reconcilePublished(f.ctx, duplicate, publicationReadFault{publicationReader: f.selected, err: readFailure}); !errors.Is(err, readFailure) || settled {
				t.Fatal("historical read error reported retirement", err)
			}
			if _, err := db.Exec(`CREATE TRIGGER reject_historical_retirement BEFORE DELETE ON whatsapp_incoming_capture
				BEGIN SELECT RAISE(ABORT,'historical retirement failure'); END`); err != nil {
				t.Fatal(err)
			}
			if settled, err := spool.reconcilePublished(f.ctx, duplicate, f.selected); err == nil || settled {
				t.Fatal("failed historical delete reported success", err)
			}
			pending, err := spool.pending(f.ctx)
			if err != nil || len(pending) != 1 || !pending[0].sameCapture(duplicate) {
				t.Fatal("failed historical retirement discarded original pending evidence", err)
			}
			if _, err := db.Exec(`DROP TRIGGER reject_historical_retirement`); err != nil {
				t.Fatal(err)
			}
			if settled, err := spool.reconcilePublished(f.ctx, duplicate, f.selected); err != nil || !settled {
				t.Fatal("exact historical retry failed", err)
			}
		})
	}
}

func TestWhatsAppHistoricalPublicationStableScopeRefusalsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newSessionPublicationFixture(t, backend)
			original := f.capture(t)
			command := f.command(t, original)
			if _, err := f.selected.CommitInboundPublication(f.ctx, command); err != nil {
				t.Fatal(err)
			}
			for _, cell := range []struct {
				name string
				edit func(*capturedEvent)
			}{
				{"text", func(e *capturedEvent) {
					payload := incomingPayloadFixture(t, *e)
					payload.Text = proto.String("changed")
					e.Body, _ = json.Marshal(payload)
				}},
				{"sender", func(e *capturedEvent) {
					payload := incomingPayloadFixture(t, *e)
					payload.Sender = "other@s.whatsapp.net"
					e.Body, _ = json.Marshal(payload)
				}},
				{"admission", func(e *capturedEvent) { e.Scope.Session.AdmissionID = uuid.NewString() }},
				{"admission_revision", func(e *capturedEvent) { e.Scope.Session.Revision++ }},
				{"source", func(e *capturedEvent) { e.Scope.Source.BundleIdentity = "other" }},
				{"principal", func(e *capturedEvent) { e.Scope.PrincipalID = uuid.NewString() }},
				{"operation", func(e *capturedEvent) { e.Scope.OnboardingOperation = uuid.NewString() }},
				{"binding", func(e *capturedEvent) { e.Scope.BindingRevision++ }},
			} {
				t.Run(cell.name, func(t *testing.T) {
					changed := original
					changed.OccurrenceID = uuid.NewString()
					cell.edit(&changed)
					_, spool := openCaptureFixture(t, filepath.Join(t.TempDir(), "incoming.db"), changed.Scope.Session.ConnectionID)
					if err := spool.capture(f.ctx, changed); err != nil {
						t.Fatal(err)
					}
					if settled, err := spool.reconcilePublished(f.ctx, changed, f.selected); !errors.Is(err, runtimeinbound.ErrRequestIdentityConflict) || settled {
						t.Fatalf("changed content/scope adopted original history: %t %v", settled, err)
					}
					pending, err := spool.pending(f.ctx)
					if err != nil || len(pending) != 1 || !pending[0].sameCapture(changed) || f.plans != 1 {
						t.Fatal("refusal lost evidence or planned another publication", err)
					}
				})
			}
			for _, cell := range []string{"connection", "account", "service", "run", "generation"} {
				t.Run(cell+"_namespace", func(t *testing.T) {
					changed := original
					switch cell {
					case "connection":
						changed.Scope.Session.ConnectionID = uuid.NewString()
					case "account":
						changed.Scope.Session.AccountRef = "other"
					case "service":
						changed.Scope.PublicationBinding.ServiceID = uuid.NewString()
					case "run":
						changed.Scope.PublicationBinding.RunID = uuid.NewString()
					case "generation":
						changed.Scope.PublicationBinding.Generation++
					}
					// The admitted connection owner checks its complete frozen scope;
					// an altered namespace cannot become an original-scope retry.
					if err := changed.requireOriginalScope(original.Scope); !errors.Is(err, errCaptureScopeChanged) {
						t.Fatal("changed namespace passed original admission scope", err)
					}
					identity, err := changed.publicationIdentity()
					if err != nil {
						t.Fatal(err)
					}
					if _, found, err := f.selected.LoadInboundPublicationByIdentity(f.ctx, identity); err != nil || found {
						t.Fatal("another namespace resolved original publication", err)
					}
				})
			}
		})
	}
}

func TestWhatsAppSessionPublicationOutputAuthorizationBeforeMutationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newSessionPublicationFixture(t, backend)
			event := f.capture(t)
			command := f.command(t, event)
			for _, field := range []string{"provider", "event", "pack", "version", "manifest", "generation"} {
				t.Run(field, func(t *testing.T) {
					original := command.Finalization.Events[1].Authorization
					provider, name, pack, version, manifest, generation := original.Provider(), original.Event(), original.PackID(), original.PackVersion(), original.ManifestHash(), original.Generation()
					switch field {
					case "provider":
						provider = "foreign"
					case "event":
						name = "inbound.whatsapp.foreign"
					case "pack":
						pack = "provider.whatsapp.foreign"
					case "version":
						version = "2.0.0"
					case "manifest":
						manifest = fmt.Sprintf("sha256:%x", sha256.Sum256([]byte("foreign manifest")))
					case "generation":
						generation = triggergeneration.FromCanonicalBytes([]byte("foreign generation"))
					}
					bad := runtimeprovideroutput.MustAuthorization(provider, name, pack, version, manifest, generation)
					batch := runtimebus.InboundDeliveryBatch{Provider: "whatsapp", Events: []runtimebus.InboundDeliveryEvent{
						{Event: command.Finalization.Events[0].Event, Kind: runtimeprovideroutput.KindRaw},
						{Event: command.Finalization.Events[1].Event, Kind: runtimeprovideroutput.KindNormalized, Authorization: bad},
					}}
					if _, err := f.bus.PrepareInboundDeliveryBatch(f.ctx, batch); err == nil {
						t.Fatal("foreign normalized authority reached publication planning")
					}
					if _, found, err := f.selected.LoadInboundPublicationByIdentity(f.ctx, command.Request.Identity()); err != nil || found {
						t.Fatal("refused output authority created a publication", err)
					}
				})
			}
		})
	}
}

func (f *sessionPublicationFixture) command(t *testing.T, event capturedEvent) runtimeinbound.CommitCommand {
	t.Helper()
	request := capturePublicationFixture(t, event)
	request.FlowPath = f.candidate.FlowPath
	request.ExpectedGeneration, request.ExpectedPublicationSequence = f.standing.Generation, f.sequence
	normalized, err := f.manifest.ProjectNormalizedPayload(event.Body)
	if err != nil || len(normalized) != 1 {
		t.Fatalf("normalized projection: %+v %v", normalized, err)
	}
	authorization, err := runtimeprovideroutput.NewAuthorization("whatsapp", string(normalized[0].Name),
		f.identity.ID, f.identity.Version, f.identity.ManifestHash, f.catalog.Generation())
	if err != nil {
		t.Fatal(err)
	}
	normalizedBody, err := json.Marshal(normalized[0].Payload)
	if err != nil {
		t.Fatal(err)
	}
	batch := runtimebus.InboundDeliveryBatch{Provider: "whatsapp", AuthorSubjectType: "chat", AuthorSubjectID: event.Conversation}
	routingSource, err := events.NewExternalIngressRoutingSource(request.FlowPath, events.RoutingSourceAuthorityProviderAdmissionPlan)
	if err != nil {
		t.Fatal(err)
	}
	for ordinal, output := range []struct {
		name          events.EventType
		body          []byte
		kind          runtimeprovideroutput.Kind
		authorization runtimeprovideroutput.Authorization
	}{{"inbound.whatsapp", event.Body, runtimeprovideroutput.KindRaw, runtimeprovideroutput.Authorization{}},
		{normalized[0].Name, normalizedBody, runtimeprovideroutput.KindNormalized, authorization}} {
		id, err := runtimeinbound.DeterministicEventID(request.PublicationID, ordinal)
		if err != nil {
			t.Fatal(err)
		}
		batch.Events = append(batch.Events, runtimebus.InboundDeliveryEvent{Kind: output.kind, Authorization: output.authorization,
			Event: eventtest.ExistingRunRootIngressWithRoutingSource(id, output.name, "inbound-gateway", "", output.body, 0, request.ResolvedRunID,
				events.EventEnvelope{}, routingSource, request.OriginalReceivedAt)})
	}
	plan, err := f.bus.PrepareInboundDeliveryBatch(f.ctx, batch)
	if err != nil {
		t.Fatal(err)
	}
	f.plans++
	t.Cleanup(func() {
		if err := f.bus.AbandonInboundDeliveryPlan(context.Background(), plan); err != nil {
			t.Error(err)
		}
	})
	command := runtimeinbound.CommitCommand{Request: request, Publications: plan.CommitCommands()}
	command.AuthorProjection, _ = runtimeauthoractivity.InboundProjectionFromContext(f.ctx)
	for ordinal, prepared := range plan.PreparedPublications() {
		manifest, _, _, err := runtimeinbound.CanonicalRecipientManifest(prepared.DeliveryRoutes())
		if err != nil {
			t.Fatal(err)
		}
		command.Finalization.Events = append(command.Finalization.Events, runtimeinbound.EventFinalization{
			Ordinal: ordinal, Event: prepared.Event, Kind: batch.Events[ordinal].Kind,
			Authorization: batch.Events[ordinal].Authorization, RecipientManifest: manifest})
	}
	payload, err := runtimeinbound.BuildEvidencePayload(request,
		[]string{batch.Events[0].Event.ID(), batch.Events[1].Event.ID()}, []string{string(batch.Events[0].Event.Type()), string(batch.Events[1].Event.Type())})
	if err != nil {
		t.Fatal(err)
	}
	command.Finalization.EvidenceEvent = eventtest.DiagnosticDirect(request.MarkerEventID, events.EventTypePlatformInboundRecord,
		"runtime", "", payload, 0, request.ResolvedRunID, "", events.EventEnvelope{}, request.OriginalReceivedAt)
	if err := command.Validate(); err != nil {
		t.Fatal(err)
	}
	return command
}

func TestWhatsAppSessionCaptureNormalizedPublicationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newSessionPublicationFixture(t, backend)
			event := f.capture(t)
			path := filepath.Join(t.TempDir(), "incoming.db")
			db, spool := openCaptureFixture(t, path, event.Scope.Session.ConnectionID)
			if err := spool.capture(f.ctx, event); err != nil {
				t.Fatal(err)
			}
			command := f.command(t, event)
			if err := spool.stagePublication(f.ctx, event, command.Request); err != nil {
				t.Fatal(err)
			}
			result, err := f.selected.CommitInboundPublication(f.ctx, command)
			if err != nil || !result.Acknowledged || !result.Record.Created || result.Record.OutputCount != 2 {
				t.Fatalf("real normalized commit: %+v %v", result, err)
			}
			if err := spool.retirePublished(f.ctx, event, f.selected); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			_, spool = openCaptureFixture(t, path, event.Scope.Session.ConnectionID)
			duplicate := event
			duplicate.OccurrenceID = uuid.NewString()
			if err := spool.capture(f.ctx, duplicate); err != nil {
				t.Fatal(err)
			}
			settled, err := spool.reconcilePublished(f.ctx, duplicate, f.selected)
			if err != nil || !settled || f.plans != 1 {
				t.Fatalf("historical reconciliation replanned or failed: %t plans=%d %v", settled, f.plans, err)
			}
			record, found, err := f.selected.LoadInboundPublicationByIdentity(f.ctx, command.Request.Identity())
			if err != nil || !found || record.OutputCount != 2 || len(record.Events) != 2 || !bytes.Equal(record.Events[0].Event.Payload(), event.Body) {
				t.Fatalf("verified historical event set changed: %+v %t %v", record, found, err)
			}
			changed := command
			changed.Request.RequestFingerprint, _ = runtimeinbound.SemanticFingerprint("different capture")
			if _, err := f.selected.CommitInboundPublication(f.ctx, changed); !errors.Is(err, runtimeinbound.ErrRequestIdentityConflict) {
				t.Fatal("canonical store accepted different content", err)
			}
		})
	}
}
