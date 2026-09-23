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
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimeinbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

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

func TestInboundAcknowledgedPublicationCleanupRespondsAndDoesNotRedeliverBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			const runID = "75000000-0000-0000-0000-000000000001"
			const entityID = "75000000-0000-0000-0000-000000000002"
			const agentID = "acknowledged-telegram-observer"
			const providerEventID = "8201"
			ctx, selected, db, target := inboundAcknowledgedSelectedFixture(t, backend, runID, entityID, "ack-telegram-instance", "ack-telegram", agentID)
			bus, err := newScopedTestEventBus(t, selected, runtimebus.EventBusOptions{}, "inbound.telegram", "inbound.telegram.text_message")
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
				bus, err := newScopedTestEventBus(t, selected, runtimebus.EventBusOptions{}, "inbound.telegram", "inbound.telegram.text_message")
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
			bus, err := newScopedTestEventBus(t, selected, runtimebus.EventBusOptions{}, "inbound.telegram", "inbound.telegram.text_message")
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
