package runtime_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeagentidentitytest "github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type providerRawSettlementProofStore interface {
	runtimepkg.InboundPersistence
	runtimebus.EventStore
	LoadOperatorEvent(context.Context, string) (operatorread.OperatorEventFull, error)
	ListOperatorEvents(context.Context, operatorread.OperatorEventListOptions) (operatorread.OperatorEventListResult, error)
}

type providerRawSettlementCase struct {
	provider  string
	eventName string
	explicit  bool
	request   func(secret, providerEventID string) *http.Request
}

func TestInboundGatewayProviderRawSettlementSQLitePostgres(t *testing.T) {
	providers := providerRawSettlementCases()
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, db := openProviderRawSettlementStore(t, backend)
			for providerIndex, provider := range providers {
				t.Run(provider.provider, func(t *testing.T) {
					runID := uuid.NewString()
					entityID := uuid.NewString()
					flowInstance := fmt.Sprintf("bounded-inbound-%s-%s", provider.provider, backend)
					secret := provider.provider + "-raw-settlement-secret"
					ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
					target := seedProviderRawSettlementRuntime(t, ctx, selected, db, runID, entityID, flowInstance, provider.provider, secret, "")
					source := providerRawSettlementSemanticSource(target.FlowID, flowInstance, provider.eventName)
					for _, realSubscriber := range []bool{false, true} {
						outcome := "zero_consumer"
						if realSubscriber {
							outcome = "real_subscriber"
						}
						t.Run(outcome, func(t *testing.T) {
							providerEventID := providerRawSettlementEventID(provider.provider, backend, outcome, providerIndex)
							agentID := ""
							if realSubscriber {
								agentID = provider.provider + "-" + backend + "-raw-subscriber"
								seedProviderRawSettlementAgent(t, ctx, selected, target.FlowID, flowInstance, entityID, agentID, provider.eventName)
							}
							bus, err := newScopedTestEventBus(t, selected, runtimebus.EventBusOptions{ContractBundle: source}, provider.eventName)
							if err != nil {
								t.Fatalf("NewEventBus: %v", err)
							}
							var deliveries <-chan *runtimebus.LocalDelivery
							if realSubscriber {
								deliveries = subscribeProviderRawSettlementAgent(t, bus, target.FlowID, flowInstance, agentID, provider.eventName)
							}
							gateway := newTestInboundGateway(t, bus, nil, nil, selected)
							response := httptest.NewRecorder()
							handleProviderRawSettlementDelivery(t, gateway, bus, target, response, provider.request(secret, providerEventID), provider, secret)
							if response.Code != http.StatusAccepted {
								t.Fatalf("status = %d, want 202 body=%s", response.Code, response.Body.String())
							}
							record, found, err := selected.LoadInboundPublicationByIdentity(ctx, provider.provider, entityID, providerEventID)
							if err != nil || !found || len(record.Events) != 1 {
								t.Fatalf("LoadInboundPublicationByIdentity = found:%t events:%d err:%v response:%s", found, len(record.Events), err, response.Body.String())
							}
							rawEventID := record.Events[0].EventID
							if realSubscriber {
								got := requireInboundBusEvent(t, deliveries, provider.provider+" raw delivery")
								if got.ID() != rawEventID {
									t.Fatalf("dispatched raw event = %s, want %s", got.ID(), rawEventID)
								}
								runtimebustest.UnsubscribeIdentity(bus, providerRawSettlementAgentIdentity(t, target.FlowID, flowInstance, agentID))
								waitForInboundBusQuiescence(t, bus)
							} else {
								waitForInboundBusQuiescence(t, bus)
							}
							observed, err := selected.LoadOperatorEvent(ctx, rawEventID)
							if err != nil {
								t.Fatalf("LoadOperatorEvent: %v", err)
							}
							if len(observed.DeadLetters) != 0 {
								t.Fatalf("raw event dead letters = %#v, want none", observed.DeadLetters)
							}
							if realSubscriber {
								if len(observed.Deliveries) != 1 || observed.NoDelivery != nil {
									t.Fatalf("real-subscriber settlement = deliveries:%#v no_delivery:%#v", observed.Deliveries, observed.NoDelivery)
								}
							} else if len(observed.Deliveries) != 0 || observed.NoDelivery == nil || observed.NoDelivery.Reason != events.NoDeliveryNoSubscriberByDesign.Code() {
								t.Fatalf("zero-consumer settlement = deliveries:%#v no_delivery:%#v", observed.Deliveries, observed.NoDelivery)
							}
							assertProviderRawSettlementListReadback(t, ctx, selected, runID, rawEventID, realSubscriber)

							duplicate := httptest.NewRecorder()
							handleProviderRawSettlementDelivery(t, gateway, bus, target, duplicate, provider.request(secret, providerEventID), provider, secret)
							assertProviderRawSettlementDuplicate(t, duplicate, rawEventID)

							if !realSubscriber {
								replayAgentID := provider.provider + "-" + backend + "-current-topology-only"
								seedProviderRawSettlementAgent(t, ctx, selected, target.FlowID, flowInstance, entityID, replayAgentID, provider.eventName)
								replayDeliveries := subscribeProviderRawSettlementAgent(t, bus, target.FlowID, flowInstance, replayAgentID, provider.eventName)
								if _, err := bus.RecoverPersistedPipeline(ctx, runtimepipelineobligation.ClaimedWork{
									Event: record.Events[0].Event, Scope: runtimepipelineobligation.ScopeSubscribed,
								}, nil); err != nil {
									t.Fatalf("RecoverPersistedPipeline: %v", err)
								}
								requireNoInboundBusEvent(t, replayDeliveries, "consumerless raw committed replay")
								runtimebustest.UnsubscribeIdentity(bus, providerRawSettlementAgentIdentity(t, target.FlowID, flowInstance, replayAgentID))
								waitForInboundBusQuiescence(t, bus)
							}
						})
					}
				})
			}
		})
	}
}

func handleProviderRawSettlementDelivery(t *testing.T, gateway *runtimepkg.InboundGateway, bus *runtimebus.EventBus, target runtimepkg.InboundTarget, response http.ResponseWriter, request *http.Request, provider providerRawSettlementCase, secret string) {
	t.Helper()
	if !provider.explicit {
		handleBoundedProviderDelivery(t, gateway, bus, target, response, request, provider.provider, secret)
		return
	}
	plan, err := testProviderTriggerCatalog(t).CompileAdmission(providertriggers.CompileAdmissionRequest{
		Alias: target.Alias, Provider: provider.provider, SigningSecret: secret,
		Declaration: providertriggers.AdmissionDeclaration{
			Kind: "raw", Event: provider.eventName, Payload: "json",
			Authentication: providertriggers.RawAuthenticationDeclaration{
				Kind: "hmac_sha256", Header: "X-Partner-Signature", Prefix: "sha256=", Encoding: "hex",
			},
			DeliveryID: providertriggers.RawDeliveryIDDeclaration{Source: "header", Header: "X-Partner-Delivery"},
		},
	})
	if err != nil {
		t.Fatalf("compile explicit raw admission: %v", err)
	}
	target.Provider = provider.provider
	target.SigningSecret = secret
	target.AdmissionPlan = plan
	gateway.HandleResolvedWebhook(response, request, target, nil)
}

func assertProviderRawSettlementDuplicate(t testing.TB, response *httptest.ResponseRecorder, wantEventID string) {
	t.Helper()
	var payload struct {
		Status   string   `json:"status"`
		EventIDs []string `json:"event_ids"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode duplicate response: %v body=%s", err, response.Body.String())
	}
	if response.Code != http.StatusOK || payload.Status != "duplicate" || len(payload.EventIDs) != 1 || payload.EventIDs[0] != wantEventID {
		t.Fatalf("duplicate response = status:%d payload:%#v, want exact event %s", response.Code, payload, wantEventID)
	}
}

func assertProviderRawSettlementListReadback(t testing.TB, ctx context.Context, selected providerRawSettlementProofStore, runID, eventID string, realSubscriber bool) {
	t.Helper()
	page, err := selected.ListOperatorEvents(ctx, operatorread.OperatorEventListOptions{
		Filter: operatorread.OperatorEventListFilter{RunID: runID}, Limit: 500,
	})
	if err != nil {
		t.Fatalf("ListOperatorEvents: %v", err)
	}
	matches := 0
	for _, observed := range page.Events {
		if observed.EventID != eventID {
			continue
		}
		matches++
		if len(observed.DeadLetters) != 0 {
			t.Fatalf("listed raw event dead letters = %#v", observed.DeadLetters)
		}
		if realSubscriber {
			if len(observed.Deliveries) != 1 || observed.NoDelivery != nil {
				t.Fatalf("listed real-subscriber settlement = deliveries:%#v no_delivery:%#v", observed.Deliveries, observed.NoDelivery)
			}
		} else if len(observed.Deliveries) != 0 || observed.NoDelivery == nil || observed.NoDelivery.Reason != events.NoDeliveryNoSubscriberByDesign.Code() {
			t.Fatalf("listed zero-consumer settlement = deliveries:%#v no_delivery:%#v", observed.Deliveries, observed.NoDelivery)
		}
	}
	if matches != 1 {
		t.Fatalf("event.list occurrences for %s = %d, want one", eventID, matches)
	}
}

func seedProviderRawSettlementAgent(t *testing.T, ctx context.Context, selected providerRawSettlementProofStore, flowID, flowInstance, entityID, agentID, eventName string) {
	t.Helper()
	agent := runtimemanager.PersistedAgent{
		Config: runtimeTestAgentConfig(t, runtimeactors.AgentConfig{
			ExecutionMode: "live", ResolvedLLMBackend: "anthropic", ID: agentID,
			Identity: providerRawSettlementAgentIdentity(t, flowID, flowInstance, agentID),
			Role:     "observer", FlowID: flowID, Type: "stub", Model: "regular",
			FlowPath: flowInstance, EntityID: entityID, Subscriptions: []string{eventName}, Config: []byte(`{}`),
		}),
		Status: "active", HiredBy: "test", StartedAt: time.Now().UTC(),
	}
	var err error
	switch typed := selected.(type) {
	case *store.PostgresStore:
		err = storetest.UpsertAgentFixture(t, ctx, typed, agent)
	case *store.SQLiteRuntimeStore:
		err = storetest.UpsertAgentFixture(t, ctx, typed, agent)
	default:
		t.Fatalf("unsupported provider raw agent store %T", selected)
	}
	if err != nil {
		t.Fatalf("UpsertAgent(%s): %v", agentID, err)
	}
}

func subscribeProviderRawSettlementAgent(t testing.TB, bus *runtimebus.EventBus, flowID, flowInstance, agentID, eventName string) <-chan *runtimebus.LocalDelivery {
	t.Helper()
	admission, err := semanticview.AdmitFlowOwnedAgentSubscriptions(nil, semanticview.FlowOwnedAgentSubscriptionRequest{
		AgentID: agentID, FlowID: flowID, FlowPath: flowInstance, Subscriptions: []string{eventName},
	})
	if err != nil {
		t.Fatalf("admit provider raw subscriber: %v", err)
	}
	return runtimebustest.SubscribeIdentity(t, bus, providerRawSettlementAgentIdentity(t, flowID, flowInstance, agentID), admission)
}

func providerRawSettlementAgentIdentity(t testing.TB, flowID, flowInstance, agentID string) runtimeagentidentity.Identity {
	t.Helper()
	return runtimeagentidentitytest.Runtime(t, agentID, "runtime-test/provider-raw-settlement", flowID, flowInstance, flowInstance)
}

func openProviderRawSettlementStore(t *testing.T, backend string) (providerRawSettlementProofStore, *sql.DB) {
	t.Helper()
	if backend == "postgres" {
		_, db, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		return storetest.AdmitPostgresRuntimeStore(t, db), db
	}
	selected := storetest.StartSQLiteRuntimeStoreWithContext(t, testAuthorActivityContext(context.Background()))
	return selected, storetest.DatabaseForTest(selected)
}

func seedProviderRawSettlementRuntime(t *testing.T, ctx context.Context, selected providerRawSettlementProofStore, db *sql.DB, runID, entityID, flowInstance, provider, secret, agentID string) runtimepkg.InboundTarget {
	t.Helper()
	switch typed := selected.(type) {
	case *store.PostgresStore:
		return seedPostgresInboundGatewayRuntime(t, ctx, db, typed, runID, entityID, flowInstance, "customer-a", provider, secret, agentID)
	case *store.SQLiteRuntimeStore:
		return seedSQLiteInboundGatewayRuntime(t, ctx, typed, runID, entityID, flowInstance, "customer-a", provider, secret, agentID)
	default:
		t.Fatalf("unsupported provider raw settlement store %T", selected)
		return runtimepkg.InboundTarget{}
	}
}

func providerRawSettlementSemanticSource(flowID, flowInstance, eventName string) semanticview.Source {
	pin := runtimecontracts.FlowInputEventPin{Event: eventName, Source: runtimecontracts.FlowInputPinSourceExternal}
	schema := runtimecontracts.FlowSchemaDocument{
		Name: flowID, Mode: runtimecontracts.FlowModeStatic,
		Pins: runtimecontracts.FlowPins{Inputs: runtimecontracts.FlowInputPins{EventPins: []runtimecontracts.FlowInputEventPin{pin}}},
	}
	flow := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: flowID, Flow: flowID, Mode: runtimecontracts.FlowModeStatic},
		Path:  flowInstance, Schema: schema,
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{flow}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Package:     runtimecontracts.ProjectPackageDocument{Name: "provider_raw_settlement", Version: "1.0.0"},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{flowID: schema},
		FlowTree: runtimecontracts.FlowTree{
			Root: &root, ByID: map[string]*runtimecontracts.FlowContractView{flowID: &root.Children[0]},
		},
	}
	if err := runtimecontracts.CompileWorkflowSemantics(bundle); err != nil {
		panic(err)
	}
	return semanticview.Wrap(bundle)
}

func providerRawSettlementEventID(provider, backend, outcome string, index int) string {
	if provider == "telegram" {
		return strconv.Itoa(800000000 + index*100 + len(backend)*10 + len(outcome))
	}
	return strings.ReplaceAll(fmt.Sprintf("raw-%s-%s-%s-%d", provider, backend, outcome, index), "_", "-")
}

func providerRawSettlementCases() []providerRawSettlementCase {
	return []providerRawSettlementCase{
		{provider: "github", eventName: "inbound.github.push", request: func(secret, id string) *http.Request {
			body := []byte(`{"zen":"raw settlement"}`)
			req := httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/github", strings.NewReader(string(body)))
			req.Header.Set("X-Hub-Signature-256", githubWebhookSignature(secret, body))
			req.Header.Set("X-GitHub-Delivery", id)
			req.Header.Set("X-GitHub-Event", "push")
			return req
		}},
		{provider: "slack", eventName: "inbound.slack.message_channels", request: func(secret, id string) *http.Request {
			body := []byte(fmt.Sprintf(`{"type":"event_callback","event_id":%q,"event":{"type":"message.channels","text":"hello"}}`, id))
			timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
			req := httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/slack", strings.NewReader(string(body)))
			req.Header.Set("X-Slack-Request-Timestamp", timestamp)
			req.Header.Set("X-Slack-Signature", slackWebhookSignature(secret, timestamp, body))
			return req
		}},
		{provider: "stripe", eventName: "inbound.stripe", request: func(secret, id string) *http.Request {
			body := []byte(fmt.Sprintf(`{"id":%q,"type":"invoice.paid","data":{"object":{"id":"in_raw"}}}`, id))
			timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
			req := httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/stripe", strings.NewReader(string(body)))
			req.Header.Set("Stripe-Signature", stripeWebhookSignature(secret, timestamp, body))
			return req
		}},
		{provider: "twilio", eventName: "inbound.twilio", request: func(secret, id string) *http.Request {
			requestURL := "https://example.com/webhooks/customer-a/twilio?tenant=raw"
			return newSignedTwilioRequest(requestURL, secret, url.Values{"Body": {"hello"}, "From": {"+15551234567"}, "MessageSid": {id}, "To": {"+15557654321"}})
		}},
		{provider: "shopify", eventName: "inbound.shopify", request: func(secret, id string) *http.Request {
			body := []byte(`{"id":123,"line_items":[]}`)
			req := newSignedShopifyRequest("/webhooks/customer-a/shopify", secret, body)
			req.Header.Set("X-Shopify-Webhook-Id", id)
			req.Header.Set("X-Shopify-Topic", "orders/create")
			return req
		}},
		{provider: "telegram", eventName: "inbound.telegram", request: func(secret, id string) *http.Request {
			body := []byte(fmt.Sprintf(`{"update_id":%s,"poll":{"id":"raw-only"}}`, id))
			return newSignedTelegramRequest("/webhooks/customer-a/telegram", secret, body)
		}},
		{provider: "typeform", eventName: "inbound.typeform", request: func(secret, id string) *http.Request {
			body := []byte(fmt.Sprintf(`{"event_id":%q,"event_type":"form_response","form_response":{"token":"raw"}}`, id))
			return newSignedTypeformRequest("/webhooks/customer-a/typeform", secret, body)
		}},
		{provider: "intercom", eventName: "inbound.intercom", request: func(secret, id string) *http.Request {
			body := []byte(fmt.Sprintf(`{"id":%q,"topic":"conversation.user.created","data":{"item":{"id":"raw"}}}`, id))
			return newSignedIntercomRequest("/webhooks/customer-a/intercom", secret, body)
		}},
		{provider: "partner_events", eventName: "inbound.partner", explicit: true, request: func(secret, id string) *http.Request {
			body := []byte(`{"value":"explicit raw settlement"}`)
			mac := hmac.New(sha256.New, []byte(secret))
			_, _ = mac.Write(body)
			req := httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/partner_events", strings.NewReader(string(body)))
			req.Header.Set("X-Partner-Signature", "sha256="+hex.EncodeToString(mac.Sum(nil)))
			req.Header.Set("X-Partner-Delivery", id)
			return req
		}},
	}
}
