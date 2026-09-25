package runtime_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimeinbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

type inboundCommittedFinalizerProbe struct{ persisted int }

func (p *inboundCommittedFinalizerProbe) NotifyLifecycle(_ context.Context, signal lifecycleprobe.Signal) {
	if signal.Kind == lifecycleprobe.EventPersisted {
		p.persisted++
		if p.persisted == 1 {
			panic("first committed inbound notification failed")
		}
	}
}

type inboundExactStandingRecoveryOwner struct {
	occurrence *worklifetime.StandingOccurrence
}

func (o inboundExactStandingRecoveryOwner) BeginStandingRunRecovery(ctx context.Context, runID string, origin runtimerunlifecycle.RunOrigin) (*worklifetime.Lease, error) {
	identity := o.occurrence.Identity()
	if identity.RunID != runID || identity.ServiceID != origin.ServiceID() || identity.Generation != uint64(origin.Generation()) {
		return nil, errors.New("standing recovery requested a different generation")
	}
	return o.occurrence.Begin(ctx)
}

// The selected store performs the real commit; this boundary injects only the
// cleanup error returned after its acknowledged result.
type inboundPostCommitFaultStore struct {
	runtimepkg.InboundPersistence
	fault              error
	commits            int
	hideLoads          int
	missingAck         bool
	corruptRecord      bool
	corruptCreatedFact string
}

func (s *inboundPostCommitFaultStore) CommitInboundPublication(ctx context.Context, command runtimeinbound.CommitCommand) (runtimeinbound.CommitResult, error) {
	s.commits++
	if s.missingAck {
		return runtimeinbound.CommitResult{}, nil
	}
	result, err := s.InboundPersistence.CommitInboundPublication(ctx, command)
	if err != nil || !result.Acknowledged {
		return result, err
	}
	if s.corruptRecord {
		result.Record.RequestFingerprint = strings.Repeat("f", 64)
	}
	if result.Record.Created {
		switch s.corruptCreatedFact {
		case "acknowledgement_mode":
			if result.Record.AcknowledgementMode == runtimeinbound.AcknowledgementAfterPublish {
				result.Record.AcknowledgementMode = runtimeinbound.AcknowledgementDurableBeforeDispatch
			} else {
				result.Record.AcknowledgementMode = runtimeinbound.AcknowledgementAfterPublish
			}
		case "publication_sequence":
			result.Record.ExpectedPublicationSequence++
		}
	}
	return result, s.fault
}

func (s *inboundPostCommitFaultStore) LoadInboundPublicationByIdentity(ctx context.Context, provider, entityID, eventID string) (runtimeinbound.Record, bool, error) {
	if s.hideLoads > 0 {
		s.hideLoads--
		return runtimeinbound.Record{}, false, nil
	}
	return s.InboundPersistence.LoadInboundPublicationByIdentity(ctx, provider, entityID, eventID)
}

func inboundAcknowledgedSelectedFixture(t *testing.T, backend, runID, entityID, flowInstance, slug, agentID string) (context.Context, operatorChannelInboundSelectedStore, *sql.DB, runtimepkg.InboundTarget) {
	t.Helper()
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
	if backend == "postgres" {
		_, db, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		selected := storetest.AdmitPostgresRuntimeStore(t, db)
		target := seedPostgresInboundGatewayRuntime(t, ctx, db, selected, runID, entityID, flowInstance, slug, "telegram", "telegram-secret", agentID)
		return ctx, selected, db, target
	}
	selected := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
	db := storetest.DatabaseForTest(selected)
	target := seedSQLiteInboundGatewayRuntime(t, ctx, selected, runID, entityID, flowInstance, slug, "telegram", "telegram-secret", agentID)
	return ctx, selected, db, target
}

func inboundAcknowledgedTelegramRequest(slug string, updateID int, text string) *http.Request {
	body := []byte(fmt.Sprintf(`{"update_id":%d,"message":{"message_id":7,"from":{"id":41},"chat":{"id":42,"type":"private"},"text":%q}}`, updateID, text))
	return newSignedTelegramRequest("/webhooks/"+slug+"/telegram", "telegram-secret", body)
}

func serveAcknowledgedTelegram(t *testing.T, gateway *runtimepkg.InboundGateway, bus *runtimebus.EventBus, target runtimepkg.InboundTarget, ctx context.Context, updateID int, text string) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	handleBoundedProviderDelivery(t, gateway, bus, target, recorder, inboundAcknowledgedTelegramRequest(target.Alias, updateID, text).WithContext(ctx), "telegram", "telegram-secret")
	return recorder
}

func TestInboundCommittedSiblingFinalizationRecoversDurablePipelineBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			const runID = "75100000-0000-0000-0000-000000000001"
			const entityID = "75100000-0000-0000-0000-000000000002"
			const agentID = "committed-inbound-observer"
			ctx, selected, db, target := inboundAcknowledgedSelectedFixture(t, backend, runID, entityID, "committed-inbound-instance", "committed-inbound", agentID)
			probe := &inboundCommittedFinalizerProbe{}
			bus, err := newBoundedInboundTestEventBus(t, selected, runtimebus.EventBusOptions{TestLifecycleProbe: probe}, "inbound.telegram", "inbound.telegram.text_message")
			if err != nil {
				t.Fatal(err)
			}
			_ = subscribeInboundGatewayAgent(t, bus, runID, agentID, target.FlowInstance, events.EventType("inbound.telegram"))
			gateway := newTestInboundGateway(t, bus, nil, nil, selected)
			first := serveAcknowledgedTelegram(t, gateway, bus, target, ctx, 8291, "hello")
			if first.Code != http.StatusServiceUnavailable || probe.persisted != 2 {
				t.Fatalf("commit status=%d notifications=%d body=%s", first.Code, probe.persisted, first.Body.String())
			}
			record, found, err := selected.LoadInboundPublicationByIdentity(ctx, "telegram", entityID, "8291")
			if err != nil || !found || len(record.Events) != 2 {
				t.Fatalf("committed batch=%+v found=%t err=%v", record, found, err)
			}
			originReader, ok := selected.(interface {
				LoadRunOrigin(context.Context, string) (runtimerunlifecycle.RunOrigin, error)
			})
			if !ok {
				t.Fatalf("selected store %T lacks run origin", selected)
			}
			origin, err := originReader.LoadRunOrigin(ctx, runID)
			if err != nil {
				t.Fatal(err)
			}
			process := worklifetime.NewProcess()
			runtimeOwner, err := process.NewRuntime(ctx, worklifetime.RuntimeIdentity{RuntimeInstanceID: authorActivityTestRuntimeInstanceID, BundleHash: authorActivityTestSourceArtifactFact.BundleHash()})
			if err != nil {
				t.Fatal(err)
			}
			standing, err := runtimeOwner.NewStanding(ctx, worklifetime.StandingIdentity{ServiceID: origin.ServiceID(), RunID: runID, Generation: uint64(origin.Generation())})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := standing.RetireAndWait(context.Background()); err != nil {
					t.Errorf("retire standing recovery owner: %v", err)
				}
				if _, err := runtimeOwner.RetireAndWait(context.Background()); err != nil {
					t.Errorf("retire runtime recovery owner: %v", err)
				}
				process.Retire()
				if _, err := process.Join(context.Background()); err != nil {
					t.Errorf("join recovery process: %v", err)
				}
			})
			bus.SetStandingRunWorkOwner(inboundExactStandingRecoveryOwner{occurrence: standing})
			if err := runtimepipeline.NewRecoveryManagerWith(bus).Recover(ctx); err != nil {
				t.Fatalf("recover committed batch: %v", err)
			}
			for _, item := range record.Events {
				query := `SELECT COUNT(*) FROM event_receipts WHERE event_id = ? AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`
				if backend == "postgres" {
					query = `SELECT COUNT(*) FROM event_receipts WHERE event_id = $1::uuid AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`
				}
				var receipts int
				if err := db.QueryRowContext(ctx, query, item.EventID).Scan(&receipts); err != nil || receipts != 1 {
					t.Fatalf("recovered pipeline receipt for %s=%d err=%v, want 1", item.EventID, receipts, err)
				}
			}
			duplicate := serveAcknowledgedTelegram(t, gateway, bus, target, ctx, 8291, "hello")
			if duplicate.Code != http.StatusOK {
				t.Fatalf("duplicate status=%d body=%s", duplicate.Code, duplicate.Body.String())
			}
			unsubscribeAndWaitForInboundBusQuiescence(t, bus, runID, agentID, target.FlowInstance)
		})
	}
}

func TestInboundAcknowledgedPublicationCleanupRespondsAndDoesNotRedeliverBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			const runID = "75000000-0000-0000-0000-000000000001"
			const entityID = "75000000-0000-0000-0000-000000000002"
			const agentID = "acknowledged-telegram-observer"
			const providerEventID = "8201"
			ctx, selected, db, target := inboundAcknowledgedSelectedFixture(t, backend, runID, entityID, "ack-telegram-instance", "ack-telegram", agentID)
			bus, err := newBoundedInboundTestEventBus(t, selected, runtimebus.EventBusOptions{}, "inbound.telegram", "inbound.telegram.text_message")
			if err != nil {
				t.Fatal(err)
			}
			ch := subscribeInboundGatewayAgent(t, bus, runID, agentID, target.FlowInstance, events.EventType("inbound.telegram"))
			fault := errors.New("private post-commit cleanup fault")
			wrapped := &inboundPostCommitFaultStore{InboundPersistence: selected, fault: fault}
			gateway := newTestInboundGateway(t, bus, nil, nil, wrapped)
			first := serveAcknowledgedTelegram(t, gateway, bus, target, ctx, 8201, "hello")
			if first.Code != http.StatusAccepted || !strings.Contains(first.Body.String(), `"status":"accepted"`) || strings.Contains(first.Body.String(), fault.Error()) || wrapped.commits != 1 {
				t.Fatalf("fresh status/body/commits=%d/%s/%d", first.Code, first.Body.String(), wrapped.commits)
			}
			record, found, err := selected.LoadInboundPublicationByIdentity(ctx, "telegram", entityID, providerEventID)
			if err != nil || !found || len(record.Events) == 0 {
				t.Fatalf("durable publication=%+v found=%v err=%v", record, found, err)
			}
			var rawEventID string
			for _, item := range record.Events {
				if item.EventName == "inbound.telegram" {
					rawEventID = item.EventID
				}
			}
			if rawEventID == "" || requireInboundBusEvent(t, ch, "acknowledged fresh dispatch").ID() != rawEventID {
				t.Fatalf("missing exact raw dispatch %s", rawEventID)
			}
			waitForInboundBusQuiescence(t, bus)
			wrapped.hideLoads = 1 // Force the selected-store duplicate COMMIT branch.
			duplicate := serveAcknowledgedTelegram(t, gateway, bus, target, ctx, 8201, "hello")
			if duplicate.Code != http.StatusOK || !strings.Contains(duplicate.Body.String(), `"status":"duplicate"`) || strings.Contains(duplicate.Body.String(), fault.Error()) || wrapped.commits != 2 {
				t.Fatalf("duplicate status/body/commits=%d/%s/%d", duplicate.Code, duplicate.Body.String(), wrapped.commits)
			}
			retry := serveAcknowledgedTelegram(t, gateway, bus, target, ctx, 8201, "hello")
			if retry.Code != http.StatusOK || wrapped.commits != 2 {
				t.Fatalf("retry status/commits=%d/%d", retry.Code, wrapped.commits)
			}
			waitForInboundBusQuiescence(t, bus)
			requireNoInboundBusEvent(t, ch, "acknowledged duplicate must not redeliver")
			var deliveries int
			if backend == "postgres" {
				deliveries = countPostgresAgentDeliveriesForEvent(t, ctx, db, rawEventID, agentID)
				if got := countPostgresInboundMarkers(t, ctx, db, providerEventID, entityID, "telegram"); got != 1 {
					t.Fatalf("marker count=%d, want 1", got)
				}
			} else {
				deliveries = countSQLiteAgentDeliveriesForEvent(t, ctx, selected.(*store.SQLiteRuntimeStore), rawEventID, agentID)
				if got := countSQLiteInboundMarkers(t, ctx, selected.(*store.SQLiteRuntimeStore), providerEventID, entityID, "telegram"); got != 1 {
					t.Fatalf("marker count=%d, want 1", got)
				}
			}
			if deliveries != 1 {
				t.Fatalf("agent deliveries=%d, want 1", deliveries)
			}
			wrapped.corruptRecord = true
			hostile := serveAcknowledgedTelegram(t, gateway, bus, target, ctx, 8203, "hostile result")
			if hostile.Code != http.StatusServiceUnavailable || wrapped.commits != 3 || strings.Contains(hostile.Body.String(), fault.Error()) {
				t.Fatalf("mismatched result status/body/commits=%d/%s/%d", hostile.Code, hostile.Body.String(), wrapped.commits)
			}
			if _, found, err := selected.LoadInboundPublicationByIdentity(ctx, "telegram", entityID, "8203"); err != nil || !found {
				t.Fatalf("hostile-result commit missing: found=%v err=%v", found, err)
			}
			waitForInboundBusQuiescence(t, bus)
			requireNoInboundBusEvent(t, ch, "mismatched result must not dispatch")
			wrapped.corruptRecord = false
			wrapped.missingAck = true
			missing := serveAcknowledgedTelegram(t, gateway, bus, target, ctx, 8202, "missing ack")
			if missing.Code != http.StatusServiceUnavailable || wrapped.commits != 4 || strings.Contains(missing.Body.String(), fault.Error()) {
				t.Fatalf("missing ack status/body/commits=%d/%s/%d", missing.Code, missing.Body.String(), wrapped.commits)
			}
			if _, found, err := selected.LoadInboundPublicationByIdentity(ctx, "telegram", entityID, "8202"); err != nil || found {
				t.Fatalf("missing-ack request persisted: found=%v err=%v", found, err)
			}
			waitForInboundBusQuiescence(t, bus)
			requireNoInboundBusEvent(t, ch, "missing ack must not dispatch")
			unsubscribeAndWaitForInboundBusQuiescence(t, bus, runID, agentID, target.FlowInstance)
		})
	}
}

func TestInboundAcknowledgedCreatedResultRejectsChangedExecutionFactsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, fact := range []string{"acknowledgement_mode", "publication_sequence"} {
			t.Run(backend+"/"+fact, func(t *testing.T) {
				const runID = "77000000-0000-0000-0000-000000000001"
				const entityID = "77000000-0000-0000-0000-000000000002"
				const agentID = "hostile-result-telegram-observer"
				ctx, selected, _, target := inboundAcknowledgedSelectedFixture(t, backend, runID, entityID, "hostile-result-instance", "hostile-result", agentID)
				bus, err := newBoundedInboundTestEventBus(t, selected, runtimebus.EventBusOptions{}, "inbound.telegram", "inbound.telegram.text_message")
				if err != nil {
					t.Fatal(err)
				}
				ch := subscribeInboundGatewayAgent(t, bus, runID, agentID, target.FlowInstance, events.EventType("inbound.telegram"))
				fault := errors.New("private post-commit cleanup fault")
				wrapped := &inboundPostCommitFaultStore{InboundPersistence: selected, fault: fault, corruptCreatedFact: fact}
				gateway := newTestInboundGateway(t, bus, nil, nil, wrapped)
				response := serveAcknowledgedTelegram(t, gateway, bus, target, ctx, 8401, "created result")
				if response.Code != http.StatusServiceUnavailable || wrapped.commits != 1 || strings.Contains(response.Body.String(), fault.Error()) {
					t.Fatalf("mismatched %s status/body/commits=%d/%s/%d", fact, response.Code, response.Body.String(), wrapped.commits)
				}
				record, found, err := selected.LoadInboundPublicationByIdentity(ctx, "telegram", entityID, "8401")
				if err != nil || !found || len(record.Events) == 0 || record.ExpectedPublicationSequence != target.PublicationSequence {
					t.Fatalf("durable unmodified record=%+v found=%v err=%v", record, found, err)
				}
				waitForInboundBusQuiescence(t, bus)
				requireNoInboundBusEvent(t, ch, "mismatched created result must not dispatch")
				unsubscribeAndWaitForInboundBusQuiescence(t, bus, runID, agentID, target.FlowInstance)
			})
		}
	}
}

func TestInboundAcknowledgedOperatorClaimCleanupRespondsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			const runID = "76000000-0000-0000-0000-000000000001"
			const entityID = "76000000-0000-0000-0000-000000000002"
			ctx, selected, db, target := inboundAcknowledgedSelectedFixture(t, backend, runID, entityID, "ack-operator-instance", "ack-operator", "")
			plan := compileEmbeddedTelegramOperatorChannelPlan(t)
			identity, err := plan.InterfaceIdentity()
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC().Truncate(time.Microsecond)
			principal, err := selected.EnsureOperatorPrincipal(ctx, now)
			if err != nil {
				t.Fatal(err)
			}
			proofs, err := operatorchannel.NewFileProofStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			credentials, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "provider-credentials.json"))
			if err != nil {
				t.Fatal(err)
			}
			if err := credentials.Set(ctx, "channel.telegram.provider", "telegram-provider-token"); err != nil {
				t.Fatal(err)
			}
			credentialOwner, err := runtimecredentials.NewSnapshotOwner(credentials)
			if err != nil {
				t.Fatal(err)
			}
			providerEvidence, err := credentialOwner.SealCurrentValue(ctx, "channel.telegram.provider")
			if err != nil {
				t.Fatal(err)
			}
			channelService, err := operatorchannel.NewService(selected, proofs, credentialOwner, []operatorchannel.InterfaceIdentity{identity}, operatorchannel.NewOperationID())
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := channelService.Bootstrap(ctx, now); err != nil {
				t.Fatal(err)
			}
			operation, err := selected.BeginChannelBinding(ctx, operatorchannel.BeginRequest{
				OperationID: operatorchannel.NewOperationID(), Kind: operatorchannel.OperationConnect,
				PrincipalID: principal.ID, Interface: identity, ExpectedRevision: 0,
				RequestKeyHash: "ack-operator-key", RequestHash: "ack-operator-body",
				ProviderCredential: providerEvidence,
				RequestedAt:        now, ExpiresAt: now.Add(operatorchannel.DefaultChallengeTTL),
			})
			if err != nil {
				t.Fatal(err)
			}
			bus, err := newBoundedInboundTestEventBus(t, selected, runtimebus.EventBusOptions{}, "inbound.telegram", "inbound.telegram.text_message")
			if err != nil {
				t.Fatal(err)
			}
			fault := errors.New("private operator claim cleanup fault")
			wrapped := &inboundPostCommitFaultStore{InboundPersistence: selected, fault: fault}
			gateway := newTestInboundGateway(t, bus, nil, nil, wrapped)
			gateway.SetChannelPlans([]packs.SatisfactionPlan{plan})
			first := serveAcknowledgedTelegram(t, gateway, bus, target, ctx, 8301, operation.Challenge)
			var response operatorChannelInboundResponse
			if err := json.Unmarshal(first.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			if first.Code != http.StatusAccepted || response.ClaimDisposition != operatorchannel.DispositionConsumedBinding || response.OperationID != operation.OperationID || len(response.EventIDs) != 0 || wrapped.commits != 1 || strings.Contains(first.Body.String(), fault.Error()) {
				t.Fatalf("claim status/response/commits=%d/%+v/%d body=%s", first.Code, response, wrapped.commits, first.Body.String())
			}
			requireOperatorChannelOperationState(t, selected, principal.ID, operation.OperationID, operatorchannel.StateAwaitingConfirmation, 2)
			record, found, err := selected.LoadInboundPublicationByIdentity(ctx, "telegram", entityID, "8301")
			if err != nil || !found || len(record.Events) != 0 {
				t.Fatalf("durable claim publication=%+v found=%v err=%v", record, found, err)
			}
			duplicate := serveAcknowledgedTelegram(t, gateway, bus, target, ctx, 8301, operation.Challenge)
			if duplicate.Code != http.StatusOK || wrapped.commits != 1 || strings.Contains(duplicate.Body.String(), fault.Error()) {
				t.Fatalf("claim duplicate status/body/commits=%d/%s/%d", duplicate.Code, duplicate.Body.String(), wrapped.commits)
			}
			wrapped.hideLoads = 1
			wrapped.corruptRecord = true
			hostile := serveAcknowledgedTelegram(t, gateway, bus, target, ctx, 8301, operation.Challenge)
			if hostile.Code != http.StatusServiceUnavailable || wrapped.commits != 2 || strings.Contains(hostile.Body.String(), fault.Error()) {
				t.Fatalf("mismatched claim status/body/commits=%d/%s/%d", hostile.Code, hostile.Body.String(), wrapped.commits)
			}
			wrapped.corruptRecord = false
			wrapped.missingAck = true
			missing := serveAcknowledgedTelegram(t, gateway, bus, target, ctx, 8302, operation.Challenge)
			if missing.Code != http.StatusServiceUnavailable || wrapped.commits != 3 {
				t.Fatalf("claim missing-ack status/body/commits=%d/%s/%d", missing.Code, missing.Body.String(), wrapped.commits)
			}
			if _, found, err := selected.LoadInboundPublicationByIdentity(ctx, "telegram", entityID, "8302"); err != nil || found {
				t.Fatalf("missing-ack claim persisted: found=%v err=%v", found, err)
			}
			requireOperatorChannelOperationState(t, selected, principal.ID, operation.OperationID, operatorchannel.StateAwaitingConfirmation, 2)
			var deliveries int
			if err := db.QueryRow(`SELECT COUNT(*) FROM event_deliveries`).Scan(&deliveries); err != nil || deliveries != 0 {
				t.Fatalf("operator claim deliveries=%d err=%v, want 0", deliveries, err)
			}
		})
	}
}
