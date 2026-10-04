package serveapp

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimeinbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	"github.com/division-sh/swarm/internal/servedparity"
	"gopkg.in/yaml.v3"
)

func TestProviderAliasesDeliverOnlyLocalAndConnectedConsumersBothStores(t *testing.T) {
	for _, scenario := range []providerAliasScenario{
		{name: "raw-connected", rawConnected: true, rootObserver: true, alphaReceiver: "alpha-receiver"},
		{name: "raw-connected-only", rawConnected: true, noLocalConsumers: true, alphaReceiver: "alpha-receiver"},
		{name: "consumerless-raw", noLocalConsumers: true, alphaReceiver: "alpha-receiver"},
		{name: "consumerless-ack-loss", noLocalConsumers: true, ackLoss: true, alphaReceiver: "alpha-receiver"},
		{name: "root-and-provider-connections", rootObserver: true, publicConnected: true, alphaReceiver: "alpha-receiver"},
		{name: "provider-edge-removed", rootObserver: true, publicConnected: true, omitAlphaProviderEdge: true, alphaReceiver: "alpha-receiver"},
		{name: "root-present", rootObserver: true, alphaReceiver: "alpha-receiver"},
		{name: "agent-consumers", rootObserver: true, agents: true, alphaReceiver: "alpha-receiver"},
		{name: "ack-loss", rootObserver: true, ackLoss: true, alphaReceiver: "alpha-receiver"},
		{name: "agent-replay", rootObserver: true, agents: true, replay: true, alphaReceiver: "alpha-receiver"},
		{name: "root-absent", alphaReceiver: "alpha-receiver"},
		{name: "edge-removed", rootObserver: true},
		{name: "edge-redirected", rootObserver: true, alphaReceiver: "redirected-receiver"},
		{name: "edge-to-root", rootObserver: true, alphaReceiver: "."},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			runProviderAliasAuthorityScenario(t, scenario)
		})
	}
}

func runProviderAliasAuthorityScenario(t *testing.T, scenario providerAliasScenario) {
	t.Helper()
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			configureStandingLifecycleCredentials(t)
			credentials, err := runtimecredentials.NewFileStore(os.Getenv("SWARM_CREDENTIALS_FILE"))
			if err != nil {
				t.Fatal(err)
			}
			for _, alias := range []string{"alpha", "beta"} {
				if err := credentials.Set(context.Background(), "webhook_signing."+alias, alias+"-secret"); err != nil {
					t.Fatal(err)
				}
			}
			var fault *providerPublicationAckLoss
			if scenario.ackLoss {
				fault = &providerPublicationAckLoss{}
				previous := projectRuntimePersistenceForServe
				projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
					persistence := previous(owner)
					fault.Runner = persistence.deps.InboundStore
					persistence.deps.InboundStore = fault
					return persistence
				}
				t.Cleanup(func() { projectRuntimePersistenceForServe = previous })
			}
			root := writeProviderAliasAuthorityFixture(t, scenario)
			rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, root)
			baseURL := strings.TrimSuffix(rt.Endpoint, "/v1/rpc")
			for aliasIndex, alias := range []string{"alpha", "beta"} {
				for shapeIndex, shape := range []string{"text", "callback"} {
					t.Run(alias+"/"+shape, func(t *testing.T) {
						id := 9100 + aliasIndex*10 + shapeIndex
						var update string
						normalized := "inbound.telegram.text_message"
						if shape == "text" {
							update = fmt.Sprintf(`{"update_id":%d,"message":{"message_id":7,"from":{"id":41},"chat":{"id":42,"type":"private"},"text":"hello"}}`, id)
						} else {
							normalized = "inbound.telegram.callback_action"
							update = fmt.Sprintf(`{"update_id":%d,"callback_query":{"id":"query-%d","from":{"id":41},"message":{"message_id":7,"chat":{"id":42,"type":"private"}},"data":"opaque-action-token"}}`, id, id)
						}
						// Failed authentication must not claim the delivery identity: the exact
						// same update succeeds only after presenting the bound signing secret.
						for _, secret := range []string{"", "wrong-secret", map[string]string{"alpha": "beta-secret", "beta": "alpha-secret"}[alias]} {
							status, _ := postProviderAliasUpdate(t, baseURL, alias, secret, update)
							if status != http.StatusUnauthorized {
								t.Fatalf("unauthenticated %s status=%d, want 401", alias, status)
							}
						}
						status, response := postProviderAliasUpdate(t, baseURL, alias, alias+"-secret", update)
						if fault != nil {
							if status != http.StatusServiceUnavailable {
								t.Fatalf("lost acknowledgment status=%d body=%s", status, response)
							}
							status, response = postProviderAliasUpdate(t, baseURL, alias, alias+"-secret", update)
							if status != http.StatusOK {
								t.Fatalf("committed retry status=%d body=%s", status, response)
							}
						} else if status != http.StatusAccepted {
							t.Fatalf("authenticated %s status=%d body=%s", alias, status, response)
						}
						var receipt struct {
							PublicationID     string   `json:"publication_id"`
							EntityID          string   `json:"entity_id"`
							EventIDs          []string `json:"event_ids"`
							ActionDisposition string   `json:"operator_channel_action_disposition"`
						}
						if err := json.Unmarshal(response, &receipt); err != nil {
							t.Fatalf("decode receipt=%s: %v", response, err)
						}
						if shape == "callback" {
							if len(receipt.EventIDs) != 0 || (!scenario.ackLoss && receipt.ActionDisposition != "pending") {
								t.Fatalf("callback receipt=%s, want operator action intent and no business events", response)
							}
						} else if len(receipt.EventIDs) != 2 {
							t.Fatalf("text receipt=%s, want raw and normalized identities", response)
						}
						if fault != nil {
							fault.mu.Lock()
							commits, recorded := fault.commits, fault.record.EventIDs()
							fault.mu.Unlock()
							if commits != aliasIndex*2+shapeIndex+1 || !reflect.DeepEqual(recorded, receipt.EventIDs) {
								t.Fatalf("reconciliation changed committed batch: commits=%d recorded=%v receipt=%v", commits, recorded, receipt.EventIDs)
							}
						}
						if shape == "callback" {
							requireProviderAliasActionIntent(t, rt, receipt.PublicationID, receipt.EntityID, scenario.source(alias), id)
						} else {
							seen := map[string]bool{}
							for _, eventID := range receipt.EventIDs {
								name := requireProviderAliasDeliveries(t, rt.Endpoint, eventID, scenario.source(alias), normalized, scenario.receiver(alias), scenario.agents, scenario.rawConnected, scenario.noLocalConsumers, false)
								requireProviderAliasStoredSource(t, rt, eventID, scenario.source(alias))
								if scenario.replay && name == normalized {
									requireProviderAliasAgentReplay(t, rt, eventID, alias)
								}
								if seen[name] {
									t.Fatalf("duplicate event kind %s", name)
								}
								seen[name] = true
							}
							if !seen["inbound.telegram"] || !seen[normalized] {
								t.Fatalf("event kinds=%v", seen)
							}
						}
						duplicateStatus, duplicateBody := postProviderAliasUpdate(t, baseURL, alias, alias+"-secret", update)
						var duplicate struct {
							PublicationID string   `json:"publication_id"`
							EventIDs      []string `json:"event_ids"`
						}
						if err := json.Unmarshal(duplicateBody, &duplicate); err != nil || duplicateStatus != http.StatusOK || duplicate.PublicationID != receipt.PublicationID || !reflect.DeepEqual(duplicate.EventIDs, receipt.EventIDs) {
							t.Fatalf("duplicate status=%d body=%s err=%v", duplicateStatus, duplicateBody, err)
						}
					})
				}
			}
			if scenario.publicConnected {
				requireProviderAliasRootConnection(t, rt, scenario.alphaReceiver)
			}
		})
	}
}

func requireProviderAliasActionIntent(t *testing.T, rt servedControlProofRuntime, publicationID, entityID, flow string, updateID int) {
	t.Helper()
	var reader interface {
		LoadInboundPublicationByIdentity(context.Context, string, string, string) (runtimeinbound.Record, bool, error)
	}
	if rt.SQLite != nil {
		reader = rt.SQLite
	} else {
		reader = rt.Postgres
	}
	record, found, err := reader.LoadInboundPublicationByIdentity(context.Background(), "telegram", entityID, fmt.Sprint(updateID))
	if err != nil || !found {
		t.Fatalf("read callback publication: found=%t err=%v", found, err)
	}
	if record.PublicationID != publicationID || record.FlowPath != flow || record.EntityID != entityID || record.OutputCount != 0 || len(record.Events) != 0 {
		t.Fatalf("callback lost exact provider alias or published business events: %+v", record)
	}
	query := `SELECT interface_key, state, disposition FROM operator_channel_action_intents WHERE publication_id=?`
	if rt.Postgres != nil {
		query = `SELECT interface_key, state, disposition FROM operator_channel_action_intents WHERE publication_id=$1::uuid`
	}
	var interfaceKey, state string
	var disposition sql.NullString
	if err := rt.DB.QueryRowContext(context.Background(), query, publicationID).Scan(&interfaceKey, &state, &disposition); err != nil {
		t.Fatalf("read callback action intent: %v", err)
	}
	if interfaceKey == "" || (state != "pending" && state != "settled") || (state == "settled" && (!disposition.Valid || disposition.String != "rejected")) {
		t.Fatalf("callback action intent interface=%q state=%q disposition=%v", interfaceKey, state, disposition)
	}
}

type providerAliasScenario struct {
	name                  string
	rawConnected          bool
	rootObserver          bool
	rootProvider          bool
	noLocalConsumers      bool
	publicConnected       bool
	omitAlphaProviderEdge bool
	agents                bool
	ackLoss               bool
	replay                bool
	alphaReceiver         string
}

func (s providerAliasScenario) source(alias string) string {
	if s.rootProvider && alias == "alpha" {
		return "."
	}
	return alias
}

func (s providerAliasScenario) receiver(alias string) string {
	if alias == "alpha" {
		if s.omitAlphaProviderEdge {
			return ""
		}
		return s.alphaReceiver
	}
	return "beta-receiver"
}

func requireProviderAliasRootConnection(t *testing.T, rt servedControlProofRuntime, receiver string) {
	t.Helper()
	for _, name := range []string{"inbound.telegram.text_message", "inbound.telegram.callback_action"} {
		payload := map[string]any{
			"conversation_reference": "42", "conversation_scope": "direct", "external_account_reference": "41",
			"provider_message_reference": 7,
		}
		if name == "inbound.telegram.text_message" {
			payload["text"] = "root publication"
		} else {
			payload["interaction_reference"] = "root-action"
			payload["token"] = "opaque-action-token"
		}
		published := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
			"bundle_hash": rt.BundleHash, "event_name": name, "payload": payload, "idempotency_key": "root-" + name,
		})
		requireProviderAliasDeliveries(t, rt.Endpoint, published.EventID, ".", name, receiver, false, false, false, true)
		// A public root event must not borrow the provider's authenticated
		// declaring-flow source or acquire either standing provider's consumers.
		var stored operatorread.OperatorEventFull
		var err error
		if rt.SQLite != nil {
			stored, err = rt.SQLite.LoadOperatorEvent(context.Background(), published.EventID)
		} else {
			stored, err = rt.Postgres.LoadOperatorEvent(context.Background(), published.EventID)
		}
		if err != nil {
			t.Fatal(err)
		}
		event, err := stored.EventSnapshot()
		if err != nil {
			t.Fatal(err)
		}
		if event.RoutingSource().Kind() != events.RoutingSourceRoot || event.RunID() != published.RunID {
			t.Fatalf("public root source identity changed: %#v", event.RoutingSource())
		}
		requireProviderAliasPipelineSettlement(t, rt, event.RunID())
	}
}

func postProviderAliasUpdate(t *testing.T, baseURL, alias, secret, body string) (int, json.RawMessage) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, baseURL+"/webhooks/"+alias+"/telegram", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if secret != "" {
		req.Header.Set("X-Telegram-Bot-Api-Secret-Token", secret)
	}
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, raw
}

func requireProviderAliasDeliveries(t *testing.T, endpoint, eventID, alias, normalized, receiver string, agents, rawConnected, noLocalConsumers, publicRoot bool) string {
	t.Helper()
	type readback struct {
		EventName  string `json:"event_name"`
		Deliveries []struct {
			SubscriberID string                              `json:"subscriber_id"`
			Status       string                              `json:"status"`
			Target       operatorread.OperatorDeliveryTarget `json:"target"`
		} `json:"deliveries"`
		NoDelivery *operatorread.OperatorNoDelivery `json:"no_delivery"`
		EntityID   string                           `json:"entity_id"`
	}
	var last readback
	deadline := time.Now().Add(servedProofPollDeadline)
	for time.Now().Before(deadline) {
		var event readback
		requireServedJSONRPCResult(t, endpoint, "event.get", map[string]any{"event_id": eventID}, &event)
		last = event
		want := []string{identitytest.FlowNode(t, alias, "observer").Key()}
		if noLocalConsumers {
			want = []string{}
		}
		if agents {
			want = append(want, alias+"-agent")
		}
		switch event.EventName {
		case normalized:
			if receiver != "" {
				want = append(want, identitytest.FlowNode(t, receiver, "observer").Key())
			}
		case "inbound.telegram":
			if rawConnected && receiver != "" {
				want = append(want, identitytest.FlowNode(t, receiver, "observer").Key())
			}
		default:
			t.Fatalf("unexpected event identity %q", event.EventName)
		}
		got := make([]string, 0, len(event.Deliveries))
		complete := true
		for _, delivery := range event.Deliveries {
			got = append(got, delivery.SubscriberID)
			wantFlow := alias
			connected := receiver != "" && delivery.SubscriberID == identitytest.FlowNode(t, receiver, "observer").Key()
			if connected {
				wantFlow = receiver
			}
			if delivery.Target.FlowID != wantFlow || delivery.Target.FlowInstance == "" {
				t.Fatalf("recipient %s target=%#v, want complete owner for %s", delivery.SubscriberID, delivery.Target, wantFlow)
			}
			if connected || publicRoot {
				if delivery.Target.Kind != "entityless_receiver" || delivery.Target.EntityID != "" {
					t.Fatalf("stateless connected receiver borrowed an entity: %#v", delivery.Target)
				}
			} else if delivery.Target.Kind != "existing_entity" || delivery.Target.EntityID == "" {
				t.Fatalf("local standing consumer lost its exact entity: %#v", delivery.Target)
			}
			complete = complete && delivery.Status == "delivered"
		}
		sort.Strings(got)
		sort.Strings(want)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s %s recipients=%v want=%v; root, other alias and unwired consumers must be absent", alias, event.EventName, got, want)
		}
		if len(want) == 0 && event.EventName == "inbound.telegram" &&
			(event.NoDelivery == nil || event.NoDelivery.Reason != "no_subscriber_by_design" || event.EntityID != "") {
			t.Fatalf("consumerless raw source acquired a recipient or false settlement: %#v", event)
		}
		if complete {
			return event.EventName
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("provider delivery did not settle: %#v", last)
	return ""
}

func writeProviderAliasAuthorityFixture(t *testing.T, scenario providerAliasScenario) string {
	t.Helper()
	root := t.TempDir()
	names := []string{"inbound.telegram", "inbound.telegram.text_message", "inbound.telegram.callback_action"}
	imports := "imports:\n  provider_trigger_events:\n"
	nodes := "observer:\n  execution_type: system_node\n  subscribes_to: [" + strings.Join(names, ", ") + "]\n  event_handlers:\n"
	for _, name := range names {
		if name != "inbound.telegram" {
			imports += "    - {provider: telegram, event: " + name + "}\n"
		}
		nodes += "    " + name + ":\n      guard: {id: admit, check: true}\n"
	}
	pins := "pins:\n  inputs: [" + strings.Join(names, ", ") + "]\n"
	rootSchema := "name: provider-authority\n" + imports + "pins:\n  inputs: [" + strings.Join(names[1:], ", ") + "]\nconnect:\n"
	if scenario.publicConnected {
		rootSchema = strings.Replace(rootSchema, "\nconnect:\n", "\n  outputs: ["+strings.Join(names[1:], ", ")+"]\nconnect:\n", 1)
		for _, name := range names[1:] {
			rootSchema += "  - {event: " + name + ", from: ., to: " + scenario.alphaReceiver + "}\n"
		}
	}
	rootNodes := strings.Replace(nodes, "subscribes_to: ["+strings.Join(names, ", ")+"]", "subscribes_to: ["+strings.Join(names[1:], ", ")+"]", 1)
	rootNodes = strings.Replace(rootNodes, "    inbound.telegram:\n      guard: {id: admit, check: true}\n", "", 1)
	connectedNames := names[1:]
	if scenario.rawConnected {
		connectedNames = names
	}
	files := map[string]string{}
	if scenario.rootObserver {
		files["nodes.yaml"] = rootNodes
	}
	for _, alias := range []string{"alpha", "beta"} {
		for _, name := range connectedNames {
			if receiver := scenario.receiver(alias); receiver != "" {
				rootSchema += "  - {event: " + name + ", from: " + alias + ", to: " + receiver + "}\n"
			}
		}
		files[alias+"/schema.yaml"] = "name: " + alias + "\nactivation: standing\nstages: []\n" + imports + pins + "  outputs: [" + strings.Join(connectedNames, ", ") + "]\ningress:\n  alias: " + alias + "\n  providers:\n    - {provider: telegram, signing_secret: webhook_signing." + alias + "}\n"
		files[alias+"/entities.yaml"] = "service: {}\n"
		files[alias+"/nodes.yaml"] = nodes
		if scenario.noLocalConsumers {
			delete(files, alias+"/nodes.yaml")
		}
		files[alias+"-receiver/schema.yaml"] = "name: " + alias + "-receiver\n"
		hasReceiver := scenario.receiver("alpha") == alias+"-receiver" || scenario.receiver("beta") == alias+"-receiver" || (scenario.publicConnected && scenario.alphaReceiver == alias+"-receiver")
		if hasReceiver {
			files[alias+"-receiver/schema.yaml"] += imports + "pins:\n  inputs: [" + strings.Join(connectedNames, ", ") + "]\n"
		}
		if scenario.rawConnected {
			raw, err := yaml.Marshal(map[string]any{"inbound.telegram": providertriggers.RawEventCatalogEntry().Payload.Properties})
			if err != nil {
				t.Fatal(err)
			}
			files[alias+"-receiver/events.yaml"] = string(raw)
		}
		receiverNodes := "observer:\n  execution_type: system_node\n  subscribes_to: [" + strings.Join(connectedNames, ", ") + "]\n  event_handlers:\n"
		for _, name := range connectedNames {
			receiverNodes += "    " + name + ":\n      guard: {id: admit, check: true}\n"
		}
		if hasReceiver {
			files[alias+"-receiver/nodes.yaml"] = receiverNodes
		}
	}
	if scenario.alphaReceiver == "redirected-receiver" {
		files["redirected-receiver/schema.yaml"] = strings.Replace(files["beta-receiver/schema.yaml"], "name: beta-receiver", "name: redirected-receiver", 1)
		files["redirected-receiver/nodes.yaml"] = files["beta-receiver/nodes.yaml"]
	}
	if scenario.agents {
		for _, flow := range []string{"alpha", "beta", "."} {
			id := flow + "-agent"
			flowNames := names
			if flow == "." {
				id = "root-agent"
				flowNames = names[1:]
			}
			files[filepath.Join(flow, "agents.yaml")] = id + ":\n  role: observer\n  intent: prompts/observer.md\n  model: regular\n  subscriptions: [" + strings.Join(flowNames, ", ") + "]\n  mock: {kind: python, module: mocks/observer.py}\n"
			files[filepath.Join(flow, "prompts/observer.md")] = "Observe the admitted message. Do not emit events or call tools.\n"
			files[filepath.Join(flow, "mocks/observer.py")] = "def handle(input):\n    return {\"text\": \"Observed.\", \"usage\": {\"input_tokens\": 1, \"output_tokens\": 1}}\n"
		}
	}
	files["schema.yaml"] = rootSchema
	if scenario.rootProvider {
		_, connections, ok := strings.Cut(rootSchema, "connect:\n")
		if !ok {
			t.Fatal("missing fixture connections")
		}
		files["schema.yaml"] = files["alpha/schema.yaml"] + "connect:\n" + strings.ReplaceAll(connections, "from: alpha,", "from: .,")
		files["nodes.yaml"] = files["alpha/nodes.yaml"]
		files["entities.yaml"] = files["alpha/entities.yaml"]
		delete(files, "alpha/schema.yaml")
		delete(files, "alpha/nodes.yaml")
		delete(files, "alpha/entities.yaml")
	}
	for path, body := range files {
		target := filepath.Join(root, path)
		if err := os.MkdirAll(filepath.Dir(target), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(target, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

type providerPublicationAckLoss struct {
	runtimeinbound.Runner
	mu      sync.Mutex
	commits int
	record  runtimeinbound.Record
}

func (f *providerPublicationAckLoss) HasCurrentChannelInputDraft(ctx context.Context, text operatorchannel.InboundText, now time.Time) (bool, error) {
	owner, ok := f.Runner.(interface {
		HasCurrentChannelInputDraft(context.Context, operatorchannel.InboundText, time.Time) (bool, error)
	})
	if !ok {
		return false, errors.New("inbound store lacks channel input draft classification")
	}
	return owner.HasCurrentChannelInputDraft(ctx, text, now)
}

func (f *providerPublicationAckLoss) CommitInboundPublication(ctx context.Context, command runtimeinbound.CommitCommand) (runtimeinbound.CommitResult, error) {
	result, err := f.Runner.CommitInboundPublication(ctx, command)
	if err != nil {
		return result, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commits++
	f.record = result.Record
	return runtimeinbound.CommitResult{}, errors.New("injected loss after complete provider batch commit")
}

func requireProviderAliasStoredSource(t *testing.T, rt servedControlProofRuntime, eventID, flow string) {
	t.Helper()
	var reader interface {
		LoadOperatorEvent(context.Context, string) (operatorread.OperatorEventFull, error)
	}
	if rt.SQLite != nil {
		reader = rt.SQLite
	} else {
		reader = rt.Postgres
	}
	stored, err := reader.LoadOperatorEvent(context.Background(), eventID)
	if err != nil {
		t.Fatal(err)
	}
	event, err := stored.EventSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	source := event.RoutingSource()
	if source.Kind() != events.RoutingSourceExternalIngress || source.Authority() != events.RoutingSourceAuthorityProviderAdmissionPlan ||
		source.Route().FlowID != flow || source.Route().EntityID == "" || source.Route().FlowInstance != "" {
		t.Fatalf("stored gateway source changed authority: %#v", source)
	}
	requireProviderAliasPipelineSettlement(t, rt, event.RunID())
}

func requireProviderAliasPipelineSettlement(t *testing.T, rt servedControlProofRuntime, runID string) {
	t.Helper()
	// Delivery readback precedes final publication settlement. The typed owner,
	// not an empty delivery count or sleep, proves the fixture is fully settled.
	ctx, cancel := context.WithTimeout(context.Background(), servedProofPollDeadline)
	defer cancel()
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()
	for {
		summary, err := rt.Runtime.Bus.PipelineObligationOwner().SummarizeRun(ctx, runID)
		if err != nil {
			t.Fatalf("read provider pipeline settlement: %v", err)
		}
		if summary.TerminalNonSuccess != 0 {
			t.Fatalf("provider publication did not settle successfully: %+v", summary)
		}
		if !summary.HasOpenWork() && summary.Acknowledged > 0 {
			return
		}
		select {
		case <-tick.C:
		case <-ctx.Done():
			t.Fatalf("provider publication settlement did not complete: %+v", summary)
		}
	}
}

func requireProviderAliasAgentReplay(t *testing.T, rt servedControlProofRuntime, eventID, alias string) {
	t.Helper()
	deniedKey := "unrelated-replay-" + eventID
	denied := requireServedJSONRPCError(t, rt.Endpoint, "event.replay", map[string]any{
		"event_id": eventID, "subscribers": []string{"root-agent"}, "idempotency_key": deniedKey,
	})
	if denied.Data["code"] != "EVENT_REPLAY_SUBSCRIBER_NOT_ORIGINAL" {
		t.Fatalf("non-original root replay was not refused: %#v", denied)
	}
	if count := servedEventPublishAPIIdempotencyCount(t, rt.DB, rt.Backend, "event.replay", deniedKey); count != 0 {
		t.Fatalf("denied replay wrote %d completion rows", count)
	}
	params := map[string]any{
		"event_id": eventID, "subscribers": []string{alias + "-agent"}, "idempotency_key": "provider-replay-" + eventID,
	}
	var result struct {
		ReplayEventID string   `json:"replay_event_id"`
		Subscribers   []string `json:"subscribers_replayed"`
	}
	requireServedJSONRPCResult(t, rt.Endpoint, "event.replay", params, &result)
	if result.ReplayEventID == "" || !reflect.DeepEqual(result.Subscribers, []string{alias + "-agent"}) {
		t.Fatalf("wrong replay selection: %#v", result)
	}
	requireProviderAliasStoredSource(t, rt, result.ReplayEventID, alias)
	deadline := time.Now().Add(servedProofPollDeadline)
	for {
		var event operatorread.OperatorEventFull
		requireServedJSONRPCResult(t, rt.Endpoint, "event.get", map[string]any{"event_id": result.ReplayEventID}, &event)
		if len(event.Deliveries) != 1 || event.Deliveries[0].SubscriberID != alias+"-agent" || event.Deliveries[0].Target.FlowID != alias || event.Deliveries[0].Target.EntityID == "" {
			t.Fatalf("replay recreated node, root, or connected branch: %#v", event.Deliveries)
		}
		if event.Deliveries[0].Status == "delivered" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("replay did not settle: %#v", event.Deliveries)
		}
		time.Sleep(20 * time.Millisecond)
	}
	var duplicate struct {
		ReplayEventID string `json:"replay_event_id"`
	}
	requireServedJSONRPCResult(t, rt.Endpoint, "event.replay", params, &duplicate)
	if duplicate.ReplayEventID != result.ReplayEventID {
		t.Fatalf("keyed replay changed identity: %#v / %#v", result, duplicate)
	}
}
