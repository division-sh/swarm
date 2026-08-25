package runtime_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
	"github.com/division-sh/swarm/internal/yamlsource"
)

type operatorChannelInboundSelectedStore interface {
	runtimepkg.InboundPersistence
	runtimebus.EventStore
	operatorchannel.Store
}

func TestInboundGatewaySignedTelegramOperatorChannelClaimSelectedStoreParity(t *testing.T) {
	t.Run("postgres", func(t *testing.T) {
		_, db, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		selected := storetest.AdmitPostgresRuntimeStore(t, db)
		runOperatorChannelInboundSupportedSurface(t, selected, db, false,
			"73000000-0000-0000-0000-000000000001",
			"73000000-0000-0000-0000-000000000002",
			"operator-channel-telegram-postgres")
	})
	t.Run("sqlite", func(t *testing.T) {
		selected := storetest.StartSQLiteRuntimeStore(t)
		runOperatorChannelInboundSupportedSurface(t, selected, storetest.DatabaseForTest(selected), true,
			"74000000-0000-0000-0000-000000000001",
			"74000000-0000-0000-0000-000000000002",
			"operator-channel-telegram-sqlite")
	})
}

func runOperatorChannelInboundSupportedSurface(t *testing.T, selected operatorChannelInboundSelectedStore, db *sql.DB, sqlite bool, runID, entityID, flowInstance string) {
	t.Helper()
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
	if sqlite {
		seedSQLiteInboundGatewayRuntime(t, ctx, selected.(*store.SQLiteRuntimeStore), runID, entityID, flowInstance, "operator-channel", "telegram", "telegram-secret", "operator-channel-observer")
	} else {
		seedPostgresInboundGatewayRuntime(t, ctx, db, selected.(*store.PostgresStore), runID, entityID, flowInstance, "operator-channel", "telegram", "telegram-secret", "operator-channel-observer")
	}

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
	channelService, err := operatorchannel.NewService(selected, proofs, []operatorchannel.InterfaceIdentity{identity}, operatorchannel.NewOperationID())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := channelService.Bootstrap(ctx, now); err != nil {
		t.Fatal(err)
	}
	operation, err := selected.BeginChannelBinding(ctx, operatorchannel.BeginRequest{
		OperationID: operatorchannel.NewOperationID(), Kind: operatorchannel.OperationConnect,
		PrincipalID: principal.ID, Interface: identity, ExpectedRevision: 0,
		RequestKeyHash: "signed-telegram-connect-key", RequestHash: "signed-telegram-connect-body",
		RequestedAt: now, ExpiresAt: now.Add(operatorchannel.DefaultChallengeTTL),
	})
	if err != nil {
		t.Fatal(err)
	}

	bus, err := newScopedTestEventBus(t, selected, runtimebus.EventBusOptions{},
		"inbound.telegram", "inbound.telegram.text_message", "inbound.telegram.callback_action")
	if err != nil {
		t.Fatal(err)
	}
	gateway := newTestInboundGateway(t, bus, nil, nil, selected)
	gateway.SetChannelPlans([]packs.SatisfactionPlan{plan})

	negativeBodies := []struct {
		name            string
		body            string
		normalizedEvent string
	}{
		{
			name:            "private callback is not text ownership proof",
			body:            fmt.Sprintf(`{"update_id":7301,"callback_query":{"id":"callback-1","from":{"id":41},"data":"%s","message":{"message_id":7,"chat":{"id":42,"type":"private"}}}}`, operation.Challenge),
			normalizedEvent: "inbound.telegram.callback_action",
		},
		{
			name:            "group callback is not text ownership proof",
			body:            fmt.Sprintf(`{"update_id":7302,"callback_query":{"id":"callback-2","from":{"id":41},"data":"%s","message":{"message_id":8,"chat":{"id":-42,"type":"group"}}}}`, operation.Challenge),
			normalizedEvent: "inbound.telegram.callback_action",
		},
		{
			name:            "supergroup callback is not text ownership proof",
			body:            fmt.Sprintf(`{"update_id":7303,"callback_query":{"id":"callback-3","from":{"id":41},"data":"%s","message":{"message_id":9,"chat":{"id":-43,"type":"supergroup"}}}}`, operation.Challenge),
			normalizedEvent: "inbound.telegram.callback_action",
		},
		{
			name: "group missing sender cannot prove account",
			body: fmt.Sprintf(`{"update_id":7304,"message":{"message_id":10,"chat":{"id":-42,"type":"group"},"text":"%s"}}`, operation.Challenge),
		},
		{
			name: "supergroup missing sender cannot prove account",
			body: fmt.Sprintf(`{"update_id":7305,"message":{"message_id":11,"chat":{"id":-43,"type":"supergroup"},"text":"%s"}}`, operation.Challenge),
		},
		{
			name: "group anonymous admin sender chat cannot prove sender",
			body: fmt.Sprintf(`{"update_id":7306,"message":{"message_id":12,"from":{"id":1087968824},"sender_chat":{"id":-100100,"type":"supergroup"},"chat":{"id":-42,"type":"group"},"text":"%s"}}`, operation.Challenge),
		},
		{
			name: "supergroup anonymous admin sender chat cannot prove sender",
			body: fmt.Sprintf(`{"update_id":7307,"message":{"message_id":13,"from":{"id":1087968824},"sender_chat":{"id":-100101,"type":"supergroup"},"chat":{"id":-43,"type":"supergroup"},"text":"%s"}}`, operation.Challenge),
		},
		{
			name: "group linked channel sender chat cannot prove sender",
			body: fmt.Sprintf(`{"update_id":7308,"message":{"message_id":14,"from":{"id":777000},"sender_chat":{"id":-100200,"type":"channel","title":"Ops","username":"ops"},"chat":{"id":-42,"type":"group"},"text":"%s"}}`, operation.Challenge),
		},
		{
			name: "supergroup send as chat cannot prove sender",
			body: fmt.Sprintf(`{"update_id":7309,"message":{"message_id":15,"from":{"id":777000},"sender_chat":{"id":-100201,"type":"channel","title":"Ops"},"chat":{"id":-43,"type":"supergroup"},"text":"%s"}}`, operation.Challenge),
		},
	}
	for _, tc := range negativeBodies {
		t.Run(tc.name, func(t *testing.T) {
			response := publishOperatorChannelTelegramUpdate(t, gateway, bus, selected, ctx, runID, entityID, []byte(tc.body))
			wantEvents := []string{"inbound.telegram"}
			if tc.normalizedEvent != "" {
				wantEvents = append(wantEvents, tc.normalizedEvent)
			}
			if response.ClaimDisposition != "" || response.OperationID != "" || !slices.Equal(response.EventNames, wantEvents) {
				t.Fatalf("non-proof response = %#v, want events %v without setup claim", response, wantEvents)
			}
			requireOperatorChannelOperationState(t, selected, principal.ID, operation.OperationID, operatorchannel.StateAwaitingClaim, 1)
		})
	}

	validClaims := []struct {
		kind         operatorchannel.OperationKind
		chatKind     string
		conversation int64
		scope        operatorchannel.ConversationScope
	}{
		{kind: operatorchannel.OperationConnect, chatKind: "private", conversation: 42, scope: operatorchannel.ConversationScopeDirect},
		{kind: operatorchannel.OperationRebind, chatKind: "group", conversation: -42, scope: operatorchannel.ConversationScopeShared},
		{kind: operatorchannel.OperationRebind, chatKind: "supergroup", conversation: -43, scope: operatorchannel.ConversationScopeShared},
	}
	for index, claim := range validClaims {
		if index > 0 {
			operation, err = selected.BeginChannelBinding(ctx, operatorchannel.BeginRequest{
				OperationID: operatorchannel.NewOperationID(), Kind: claim.kind,
				PrincipalID: principal.ID, Interface: identity, ExpectedRevision: int64(index),
				RequestKeyHash: fmt.Sprintf("signed-telegram-rebind-key-%d", index), RequestHash: fmt.Sprintf("signed-telegram-rebind-body-%d", index),
				RequestedAt: now.Add(time.Duration(index) * time.Minute), ExpiresAt: now.Add(time.Duration(index)*time.Minute + operatorchannel.DefaultChallengeTTL),
			})
			if err != nil {
				t.Fatal(err)
			}
		}
		validBody := []byte(fmt.Sprintf(`{"update_id":%d,"message":{"message_id":%d,"from":{"id":41,"first_name":"Operator"},"chat":{"id":%d,"type":%q},"text":%q}}`, 7310+index, 16+index, claim.conversation, claim.chatKind, operation.Challenge))
		response := publishOperatorChannelTelegramUpdate(t, gateway, bus, selected, ctx, runID, entityID, validBody)
		if response.ClaimDisposition != operatorchannel.DispositionConsumedBinding || response.OperationID != operation.OperationID || len(response.EventIDs) != 0 || len(response.EventNames) != 0 {
			t.Fatalf("%s claim response = %#v, want consumed operation with zero business events", claim.chatKind, response)
		}
		claimed := requireOperatorChannelOperationState(t, selected, principal.ID, operation.OperationID, operatorchannel.StateAwaitingConfirmation, 2)
		if claimed.ExternalAccountRef != "41" || claimed.ConversationRef != fmt.Sprint(claim.conversation) || claimed.ConversationScope != claim.scope {
			t.Fatalf("%s claimed Telegram identity = %#v", claim.chatKind, claimed)
		}
		readback, err := channelService.Readback(ctx)
		if err != nil || len(readback) != 1 || readback[0].PendingOperation == nil {
			t.Fatalf("%s Telegram claimant readback = %#v, %v", claim.chatKind, readback, err)
		}
		if got, want := readback[0].PendingOperation.AccountPresentation, operatorchannel.MaskPresentation(claimed.ExternalAccountRef); got != want || got == "" {
			t.Fatalf("%s Telegram claimant presentation = %q, want safe fallback %q", claim.chatKind, got, want)
		}
		confirmed, binding, err := selected.ConfirmChannelBinding(ctx, operatorchannel.ConfirmRequest{
			OperationID: operation.OperationID, PrincipalID: principal.ID, ExpectedRevision: claimed.Revision,
			Approve: true, ConfirmedAt: now.Add(time.Duration(index+1)*time.Minute - time.Second),
		})
		if err != nil || confirmed.State != operatorchannel.StateBound || binding.Revision != int64(index+1) || binding.ConversationScope != claim.scope {
			t.Fatalf("%s confirmation = operation:%#v binding:%#v err:%v", claim.chatKind, confirmed, binding, err)
		}
	}
}

type operatorChannelInboundResponse struct {
	EventIDs         []string `json:"event_ids"`
	EventNames       []string `json:"event_names"`
	ClaimDisposition string   `json:"operator_channel_claim_disposition"`
	OperationID      string   `json:"operator_channel_operation_id"`
}

func publishOperatorChannelTelegramUpdate(t *testing.T, gateway *runtimepkg.InboundGateway, bus *runtimebus.EventBus, selected runtimepkg.InboundPersistence, ctx context.Context, runID, entityID string, body []byte) operatorChannelInboundResponse {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := newSignedTelegramRequest("/webhooks/operator-channel/telegram", "telegram-secret", body).WithContext(ctx)
	handleBoundedProviderDelivery(t, gateway, bus, selected, recorder, request, runID, entityID, "telegram", "telegram-secret")
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("signed Telegram status = %d, want 202 body=%s", recorder.Code, recorder.Body.String())
	}
	var response operatorChannelInboundResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode signed Telegram response: %v", err)
	}
	waitForInboundBusQuiescence(t, bus)
	return response
}

func requireOperatorChannelOperationState(t *testing.T, selected operatorchannel.Store, principalID, operationID string, state operatorchannel.OperationState, revision int64) operatorchannel.Operation {
	t.Helper()
	operations, err := selected.ListOperatorChannelOperations(context.Background(), principalID)
	if err != nil {
		t.Fatal(err)
	}
	for _, operation := range operations {
		if operation.OperationID == operationID {
			if operation.State != state || operation.Revision != revision {
				t.Fatalf("operation = %#v, want state %s revision %d", operation, state, revision)
			}
			return operation
		}
	}
	t.Fatalf("operation %s is missing", operationID)
	return operatorchannel.Operation{}
}

func compileEmbeddedTelegramOperatorChannelPlan(t *testing.T) packs.SatisfactionPlan {
	t.Helper()
	repoRoot := filepath.Join("..", "..")
	snapshot, err := yamlsource.LoadFile(filepath.Join(repoRoot, "platform-spec.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var spec runtimecontracts.PlatformSpecDocument
	if err := snapshot.Decode(&spec); err != nil {
		t.Fatal(err)
	}
	registry, err := packs.NewInterfaceRegistry(spec)
	if err != nil {
		t.Fatal(err)
	}
	plans, err := packs.CompileChannelInventory(registry, packfixture.ChannelPacks(t), packfixture.TriggerCatalog(t).PackDescriptors(), packfixture.ConnectorRegistry(t).PackDescriptors())
	if err != nil || len(plans) != 1 {
		t.Fatalf("compile embedded channel inventory = %#v, %v", plans, err)
	}
	return plans[0]
}
