//go:build linux || darwin

package sessionprovider

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/bus/bustest"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/runtime/inboundpublication"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func (f *activeInputFixture) publicationBus(t *testing.T) *bus.EventBus {
	t.Helper()
	selected, ok := f.selected.(sessionPublicationStore)
	if !ok {
		t.Fatal("selected publication owners absent")
	}
	source, _ := correlation.SourceArtifactFactFromContext(f.ctx)
	scope, _ := authoractivity.ScopeFromContext(f.ctx)
	lease, err := selected.RegisterAuthorActivityEventCatalog(scope, []authoractivity.EventDescriptor{
		{EventType: "inbound.whatsapp", Disposition: authoractivity.StoryAuthored},
		{EventType: "inbound.whatsapp.message", Disposition: authoractivity.StoryAuthored},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(lease.Release)
	payload := func(_ context.Context, event events.Event, flow string) (events.PayloadAdmission, error) {
		return eventtest.PayloadAdmission(event, flow, string(event.Type()))
	}
	selected.SetEventPayloadAdmitter(payload)
	authority, err := deliverylifecycle.NewNormalExecutionAuthority(source, f.source.Coordinate.RuntimeInstanceID, 1)
	if err != nil {
		t.Fatal(err)
	}
	eventBus, err := bus.NewEventBusWithOptions(selected, bus.EventBusOptions{
		ContractBundle:   sessionBusinessSemanticFixture(t),
		ExecutionPosture: executionposture.Live, SourceArtifactFact: source, RuntimeInstanceID: f.source.Coordinate.RuntimeInstanceID,
		WorkOwner: f.workOwner, ReceiverExecution: eventreceiver.NormalExecution(), DeliveryAuthority: authority,
		PipelineObligations: selected.PipelineObligations(), PayloadAdmitter: payload, ProviderOutputVerifier: f.catalog,
		Durable: bus.DurableDependencies{ReplyContext: selected, RunLifecycle: selected, DeliveryLifecycle: selected,
			ConstructionPublications: selected, FlowRoutes: selected, FlowRouteRecords: selected, FlowRouteSets: selected,
			FlowRouteTopology: selected, FlowRouteRollback: selected, ActiveAgents: selected, ActiveFlows: selected, TargetOwners: selected,
			PreparedEvents: selected, TargetFailureRecorder: selected, RunOrigins: selected, StandingRestarts: selected},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := eventBus.SetDeliveryContinuationOwner(bustest.NewDeliveryContinuationOwner(false)); err != nil {
		t.Fatal(err)
	}
	return eventBus
}

// Compiled declaration/routing are fixture inputs, not public source admission.
// The selected binding, native authentication and commit owners remain real.
func sessionBusinessSemanticFixture(t *testing.T) semanticview.Source {
	t.Helper()
	root := &contracts.FlowContractView{Paths: contracts.FlowContractPaths{FlowPath: ".", SchemaFile: "schema.yaml"}, Path: ".",
		Events: map[string]contracts.EventCatalogEntry{
			"inbound.whatsapp":         {Payload: contracts.EventPayloadSpec{Type: "object"}},
			"inbound.whatsapp.message": {Payload: contracts.EventPayloadSpec{Type: "object"}},
		},
		Schema: contracts.FlowSchemaDocument{}}
	bundle := &contracts.WorkflowContractBundle{RootSchema: &root.Schema,
		FlowSources: map[string]contracts.FlowSource{".": {FlowPath: ".", Schema: "schema.yaml"}},
		FlowSchemas: map[string]contracts.FlowSchemaDocument{".": root.Schema},
		FlowTree:    flowmodel.Tree[contracts.FlowContractView]{Root: root, ByID: map[string]*contracts.FlowContractView{".": root}, ByPath: map[string]*contracts.FlowContractView{".": root}}}
	if err := contracts.CompileWorkflowSemantics(bundle); err != nil {
		t.Fatal(err)
	}
	return semanticviewtest.WithProviderIngress(semanticview.Wrap(bundle), map[string][]string{".": {"inbound.whatsapp", "inbound.whatsapp.message"}})
}

func TestWhatsAppBusinessCommitTemporalFencesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, mutation := range []string{"current", "unbind", "retire_activation", "release", "cancel", "missing_transfer", "generic_publisher", "rollback"} {
			t.Run(backend+"/"+mutation, func(t *testing.T) {
				f := newActiveInputFixture(t, backend)
				f.activate(t)
				eventBus := f.publicationBus(t)
				event, admitted := f.receive(t, "exact business content")
				prepared, err := prepareSessionBusinessPublication(f.ctx, admitted, f.trigger, "whatsapp", eventBus, f.selected.(sessionBusinessStore), executionposture.Live)
				if err != nil {
					t.Fatal("prepare", err)
				}
				t.Cleanup(func() { _ = eventBus.AbandonInboundDeliveryPlan(context.Background(), prepared.plan) })
				if err := f.spool.stagePublication(f.ctx, event, prepared.command.Request); err != nil {
					t.Fatal(err)
				}
				now := time.Now().UTC().Truncate(time.Microsecond)
				switch mutation {
				case "unbind":
					_, _, err = f.identities.Unbind(f.ctx, f.operation.Interface.Selector, f.binding.Revision, uuid.NewString(), uuid.NewString(), now)
				case "retire_activation":
					_, err = f.selected.RetireConnectedChannelActivation(f.ctx, channelonboarding.RetireActivationRequest{
						SlotKey: f.operation.SlotKey, ExpectedActivationRevision: f.activation.Revision, Reason: "commit fence proof", Now: now})
				case "release":
					ownedContext, release := prepared.command.WithNativeLifetime(f.ctx)
					defer release()
					admitted.Close()
					if ownedContext.Err() == nil {
						t.Fatal("commit admission waited for an asynchronous cancellation callback")
					}
				case "cancel":
					f.occurrence.cancel()
				case "missing_transfer":
					prepared.command.Admission = providertriggers.PublicationAdmission{}
				case "rollback":
					fault := &sessionPublicationFixture{db: storetest.Database(f.selected)}
					defer fault.rejectPublication(t, backend)()
				}
				if err != nil {
					t.Fatal("mutate", err)
				}
				var result inboundpublication.CommitResult
				if mutation == "generic_publisher" {
					_, err = f.selected.(bus.CommitPublicationOwner).CommitPublication(f.ctx, prepared.command.Publications[0])
				} else {
					result, err = f.selected.(sessionBusinessStore).CommitInboundPublication(f.ctx, prepared.command)
				}
				if mutation == "current" {
					if err != nil || !result.Acknowledged || !result.Record.Created || result.Record.OutputCount != 2 {
						t.Fatalf("commit: %+v %v", result, err)
					}
					return
				}
				if err == nil || result.Acknowledged {
					t.Fatalf("stale native commit acknowledged=%t created=%t: %v", result.Acknowledged, result.Record.Created, err)
				}
				if _, found, err := f.selected.(sessionBusinessStore).LoadInboundPublicationByIdentity(f.ctx, prepared.command.Request.Identity()); err != nil || found {
					t.Fatalf("stale commit left receipt: %t %v", found, err)
				}
				requireNoSessionPublicationEvents(t, f, prepared.command)
				pending, err := f.spool.readPendingRows(f.ctx, f.spool.db)
				if err != nil {
					t.Fatal(err)
				}
				retained := false
				for _, row := range pending {
					if row.event.sameCapture(event) && row.request != nil && row.request.PublicationID == prepared.command.Request.PublicationID {
						retained = true
					}
				}
				if !retained {
					t.Fatal("failed commit lost its original pending capture/request")
				}
			})
		}
	}
}

func requireNoSessionPublicationEvents(t *testing.T, f *activeInputFixture, command inboundpublication.CommitCommand) {
	t.Helper()
	reader := f.selected.(interface {
		EventExists(context.Context, string) (bool, error)
	})
	ids := []string{command.Request.MarkerEventID}
	for _, event := range command.Finalization.Events {
		ids = append(ids, event.Event.ID())
	}
	for _, id := range ids {
		if found, err := reader.EventExists(f.ctx, id); err != nil || found {
			t.Fatalf("failed publication left event %s: found=%t %v", id, found, err)
		}
	}
}

func TestWhatsAppBusinessCommitCancellationWhileLockedBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, shutdown := range []string{"release", "retire_occurrence"} {
			t.Run(backend+"/"+shutdown, func(t *testing.T) {
				f := newActiveInputFixture(t, backend)
				f.activate(t)
				eventBus := f.publicationBus(t)
				_, admitted := f.receive(t, "blocked publication")
				prepared, err := prepareSessionBusinessPublication(f.ctx, admitted, f.trigger, "whatsapp", eventBus, f.selected.(sessionBusinessStore), executionposture.Live)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = eventBus.AbandonInboundDeliveryPlan(context.Background(), prepared.plan) })
				db := storetest.Database(f.selected)
				probe := storetest.CollectTransactions(t, f.selected, storetest.TransactionProbeOptions{})
				lock, err := db.BeginTx(f.ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer lock.Rollback()
				if backend == "postgres" {
					var revision int64
					if err := lock.QueryRowContext(f.ctx, `SELECT operation_revision FROM channel_onboarding_operations WHERE operation_id=$1 FOR UPDATE`, f.operation.OperationID).Scan(&revision); err != nil {
						t.Fatal(err)
					}
				} else if _, err := lock.ExecContext(f.ctx, `UPDATE channel_onboarding_operations SET updated_at=updated_at WHERE operation_id=?`, f.operation.OperationID); err != nil {
					t.Fatal(err)
				}
				done := make(chan error, 1)
				go func() {
					result, err := f.selected.(sessionBusinessStore).CommitInboundPublication(f.ctx, prepared.command)
					if result.Acknowledged {
						err = fmt.Errorf("blocked native publication was acknowledged")
					}
					done <- err
				}()
				waitSessionPublicationLock(t, db, backend, probe)
				if shutdown == "release" {
					admitted.Close()
				} else {
					f.occurrence.fence()
				}
				select {
				case err := <-done:
					if err == nil {
						t.Fatal("native cancellation was not refused")
					}
				case <-time.After(5 * time.Second):
					_ = lock.Rollback()
					<-done
					t.Fatal("native cancellation waited for the unrelated owner lock")
				}
				if err := lock.Rollback(); err != nil {
					t.Fatal(err)
				}
				admitted.Close()
				if shutdown == "retire_occurrence" {
					if err := f.occurrence.join(f.ctx); err != nil {
						t.Fatal(err)
					}
				}
				if _, found, err := f.selected.(sessionBusinessStore).LoadInboundPublicationByIdentity(f.ctx, prepared.command.Request.Identity()); err != nil || found {
					t.Fatalf("canceled transaction left receipt: %t %v", found, err)
				}
			})
		}
	}
}

func waitSessionPublicationLock(t *testing.T, db *sql.DB, backend string, probe *storetest.TransactionCollector) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		blocked := probe.Snapshot().Total.BeginAttempts > 0
		if backend == "postgres" {
			if err := db.QueryRow(`SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%channel_onboarding_operations%' AND pid<>pg_backend_pid())`).Scan(&blocked); err != nil {
				t.Fatal(err)
			}
		}
		if blocked {
			return
		}
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatal("publication never reached the held owner lock")
		}
	}
}

func TestWhatsAppBusinessCommitRuntimeDispatchAndHistoryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			f.activate(t)
			eventBus := f.publicationBus(t)
			event, admitted := f.receive(t, "committed original content")
			prepared, err := prepareSessionBusinessPublication(f.ctx, admitted, f.trigger, "whatsapp", eventBus, f.selected.(sessionBusinessStore), executionposture.Live)
			if err != nil {
				t.Fatal(err)
			}
			if err := f.spool.stagePublication(f.ctx, event, prepared.command.Request); err != nil {
				t.Fatal(err)
			}
			result, err := prepared.commitAndDispatch()
			if err != nil || !result.Acknowledged || !result.Record.Created || len(result.Publications) != 2 {
				t.Fatalf("runtime commit/dispatch: acknowledged=%t created=%t outputs=%d %v", result.Acknowledged, result.Record.Created, len(result.Publications), err)
			}
			duplicate, err := prepareSessionBusinessPublication(f.ctx, admitted, f.trigger, "whatsapp", eventBus, f.selected.(sessionBusinessStore), executionposture.Live)
			if err != nil {
				t.Fatal(err)
			}
			if duplicate.history == nil || len(duplicate.plan.CommitCommands()) != 0 {
				t.Fatal("committed duplicate was planned again")
			}
			replayed, err := duplicate.commitAndDispatch()
			if err != nil || !replayed.Acknowledged || replayed.Record.Created || len(replayed.Publications) != 0 {
				t.Fatalf("duplicate redispatched: acknowledged=%t created=%t publications=%d %v", replayed.Acknowledged, replayed.Record.Created, len(replayed.Publications), err)
			}
			record, found, err := f.selected.(sessionBusinessStore).LoadInboundPublicationByIdentity(f.ctx, prepared.command.Request.Identity())
			if err != nil || !found || len(record.Events) != 2 || !record.OriginalReceivedAt.Equal(event.ReceivedAt) {
				t.Fatalf("verified receipt: found=%t outputs=%d %v", found, len(record.Events), err)
			}
			var normalized map[string]any
			if err := json.Unmarshal(record.Events[1].Event.Payload(), &normalized); err != nil || normalized["text"] != "committed original content" {
				t.Fatalf("normalization changed captured content: %v %v", normalized, err)
			}
			admitted.Close()
			if err := f.state.retireOccurrence(f.ctx); err != nil {
				t.Fatal(err)
			}
			settled, err := f.spool.reconcilePublished(f.ctx, event, f.selected.(sessionBusinessStore))
			if err != nil || !settled {
				t.Fatalf("original committed history required live SDK: %t %v", settled, err)
			}
			original, err := publicationCaptureProvenance(record.Request)
			if err != nil || !bytes.Equal(original.Body, event.Body) || !original.sameCapture(event) {
				t.Fatalf("receipt lost original capture: %v", err)
			}
		})
	}
}

func TestWhatsAppBusinessCommitSerializesUnbindBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			f.activate(t)
			eventBus := f.publicationBus(t)
			event, admitted := f.receive(t, "commit elected before unbind")
			prepared, err := prepareSessionBusinessPublication(f.ctx, admitted, f.trigger, "whatsapp", eventBus, f.selected.(sessionBusinessStore), executionposture.Live)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = eventBus.AbandonInboundDeliveryPlan(context.Background(), prepared.plan) })
			probe := storetest.CollectTransactions(t, f.selected, storetest.TransactionProbeOptions{Delay: 500 * time.Millisecond, DelayScope: storetest.DelayAllCommits})
			type result struct {
				commit inboundpublication.CommitResult
				err    error
			}
			committed := make(chan result, 1)
			go func() {
				commit, err := f.selected.(sessionBusinessStore).CommitInboundPublication(f.ctx, prepared.command)
				committed <- result{commit, err}
			}()
			waitSessionCommitPhase(t, probe, "injected_commit_delay")
			unbound := make(chan error, 1)
			go func() {
				_, _, err := f.identities.Unbind(f.ctx, f.operation.Interface.Selector, f.binding.Revision, uuid.NewString(), uuid.NewString(), time.Now().UTC())
				unbound <- err
			}()
			select {
			case err := <-unbound:
				t.Fatalf("unbind passed the held publication transaction: %v", err)
			case receipt := <-committed:
				if receipt.err != nil || !receipt.commit.Acknowledged || !receipt.commit.Record.Created {
					t.Fatalf("elected commit failed: %t %v", receipt.commit.Acknowledged, receipt.err)
				}
			case <-f.ctx.Done():
				t.Fatal("publication did not settle")
			}
			if err := <-unbound; err != nil {
				t.Fatal(err)
			}
			record, found, err := f.selected.(sessionBusinessStore).LoadInboundPublicationByIdentity(f.ctx, prepared.command.Request.Identity())
			if err != nil || !found || record.OutputCount != 2 {
				t.Fatalf("unbind changed committed receipt: %t %v", found, err)
			}
			if _, err := verifyHistoricalCapture(event, record); err != nil {
				t.Fatal(err)
			}
			request, err := f.trigger.AdmitSessionInput(f.ctx, admitted)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := f.trigger.ProjectPublication(request, f.source.Coordinate.BundleHash, "."); err == nil {
				t.Fatal("unbind left original authority executable")
			}
		})
	}
}

func waitSessionCommitPhase(t *testing.T, probe *storetest.TransactionCollector, phase storetest.TransactionActivePhase) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		for class, count := range probe.Snapshot().ActiveByClass {
			if class.Phase == phase && count > 0 {
				return
			}
		}
		select {
		case <-timer.C:
			t.Fatal("native publication did not reach the observed commit phase")
		case <-tick.C:
		}
	}
}
