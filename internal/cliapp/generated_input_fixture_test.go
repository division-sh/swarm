package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	runtimeroot "github.com/division-sh/swarm/internal/runtime"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeeventschema "github.com/division-sh/swarm/internal/runtime/eventschema"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestScenarioGeneratedPayloadGrammarIsScalarAndExplicit(t *testing.T) {
	doc, err := parseScenarioDocument([]byte(`
name: generated
steps:
  - publish: item_received
    payload: generate
`))
	if err != nil {
		t.Fatalf("parse generated payload: %v", err)
	}
	if len(doc.Steps) != 1 || !doc.Steps[0].GeneratePayload || doc.Steps[0].Payload != nil {
		t.Fatalf("generated step = %#v, want explicit generated mode", doc.Steps)
	}

	for _, tc := range []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "mapping sentinel",
			raw: `steps:
  - publish: item_received
    payload: {generate: true}
`,
			want: "mapping sentinels are unsupported",
		},
		{
			name: "unknown scalar",
			raw: `steps:
  - publish: item_received
    payload: generated
`,
			want: `must be exactly "generate"`,
		},
		{
			name: "alias",
			raw: `steps:
  - publish: item_received
    payload: &generated generate
  - publish: item_received
    payload: *generated
`,
			want: "payload aliases are not supported",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseScenarioDocument([]byte(tc.raw)); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("parse error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestGeneratedInputFixturePlanResolvesExactPinAndIsContextDeterministic(t *testing.T) {
	bundle := generatedInputFixtureBundle()
	runner := scenarioRunner{bundle: bundle, source: semanticview.Wrap(bundle)}
	evaluator := mustScenarioExpressionEvaluatorWithSeed(t, "scenario-a")

	first, err := runner.compileGeneratedInputFixture(scenarioTestFile{FlowID: "alpha"}, evaluator, "entry")
	if err != nil {
		t.Fatalf("compile alpha fixture: %v", err)
	}
	second, err := runner.compileGeneratedInputFixture(scenarioTestFile{FlowID: "alpha"}, evaluator, "shared.received")
	if err != nil {
		t.Fatalf("compile alpha fixture by event: %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("pin and event identity produced different plans:\npin=%#v\nevent=%#v", first, second)
	}
	if first.flowID != "alpha" || first.pinName != "entry" || first.eventKey == "" || first.schemaDigest == "" || len(first.canonicalSchema) == 0 {
		t.Fatalf("alpha plan omitted exact evidence: %#v", first)
	}
	alphaPayload, err := first.materializePayload()
	if err != nil {
		t.Fatalf("materialize alpha: %v", err)
	}
	alphaText, ok := alphaPayload["alpha"].(string)
	if !ok || alphaText != "000000000000" {
		t.Fatalf("alpha payload = %#v, want canonical 12-zero ordinary string", alphaPayload)
	}
	if id, ok := alphaPayload["id"].(string); !ok || id == "" {
		t.Fatalf("alpha payload = %#v, want context-derived UUID", alphaPayload)
	}

	beta, err := runner.compileGeneratedInputFixture(scenarioTestFile{FlowID: "beta"}, evaluator, "entry")
	if err != nil {
		t.Fatalf("compile beta fixture: %v", err)
	}
	betaPayload, err := beta.materializePayload()
	if err != nil {
		t.Fatalf("materialize beta: %v", err)
	}
	if !reflect.DeepEqual(betaPayload, map[string]any{"beta": float64(2)}) {
		t.Fatalf("beta payload = %#v, want isolated beta schema", betaPayload)
	}
	if first.identity == beta.identity || first.eventKey == beta.eventKey {
		t.Fatalf("flow-scoped plans were not isolated: alpha=%#v beta=%#v", first, beta)
	}

	otherScenario, err := runner.compileGeneratedInputFixture(scenarioTestFile{FlowID: "alpha"}, mustScenarioExpressionEvaluatorWithSeed(t, "scenario-b"), "entry")
	if err != nil {
		t.Fatalf("compile other scenario: %v", err)
	}
	if first.identity == otherScenario.identity || string(first.payload) == string(otherScenario.payload) {
		t.Fatalf("scenario identity did not stamp context-derived witness: first=%s other=%s", first.payload, otherScenario.payload)
	}
}

func TestGeneratedInputFixtureCompilesOnlyRequestedInputAndNamesAmbiguousPins(t *testing.T) {
	bundle := generatedInputFixtureBundle()
	runner := scenarioRunner{bundle: bundle, source: semanticview.Wrap(bundle)}
	evaluator := mustScenarioExpressionEvaluatorWithSeed(t, "requested-only")
	if _, err := runner.compileGeneratedInputFixture(scenarioTestFile{FlowID: "alpha"}, evaluator, "entry"); err != nil {
		t.Fatalf("unsupported sibling blocked requested fixture: %v", err)
	}

	alpha := bundle.FlowTree.ByID["alpha"]
	alpha.Schema.Pins.Inputs.EventPins = append(alpha.Schema.Pins.Inputs.EventPins,
		runtimecontracts.FlowInputEventPin{Name: "entry-copy", Event: "shared.received"},
	)
	alpha.Schema.Pins.Inputs.Events = append(alpha.Schema.Pins.Inputs.Events, "shared.received")
	bundle.FlowSchemas["alpha"] = alpha.Schema
	_, err := runner.compileGeneratedInputFixture(scenarioTestFile{FlowID: "alpha"}, evaluator, "shared.received")
	if err == nil || !strings.Contains(err.Error(), "entry (alpha/shared.received)") || !strings.Contains(err.Error(), "entry-copy (alpha/shared.received)") {
		t.Fatalf("ambiguity error = %v, want every candidate pin", err)
	}
}

func TestGeneratedInputFixturePublishesResolvedEventThroughPublicRPC(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	setCLIAPITestToken(t, "test-token")
	contractsPath := writeServedEventPublishFollowUpFixture(t)
	bundle := loadWorkflowValidationBundleAt(t, contractsPath)
	bundleHash, err := runtimecontracts.BundleHash(bundle)
	if err != nil {
		t.Fatalf("bundle hash: %v", err)
	}

	var calls []jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		var rpc jsonRPCRequest
		if err := json.NewDecoder(request.Body).Decode(&rpc); err != nil {
			t.Fatalf("decode RPC: %v", err)
		}
		calls = append(calls, rpc)
		if rpc.Method != eventPublishMethod {
			t.Fatalf("method = %q, want %s", rpc.Method, eventPublishMethod)
		}
		if rpc.Params["event_name"] != "item.received" || rpc.Params["bundle_hash"] != bundleHash {
			t.Fatalf("event.publish params = %#v", rpc.Params)
		}
		if !reflect.DeepEqual(rpc.Params["payload"], map[string]any{}) {
			t.Fatalf("generated payload = %#v, want schema-valid root input", rpc.Params["payload"])
		}
		writeJSONRPCResult(t, w, rpc.ID, eventPublishTestResult(true))
	}))
	defer server.Close()
	client, err := newCLIAPIClient(rootCommandOptions{apiServer: strings.TrimSuffix(server.URL, "/")})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	runner := scenarioRunner{client: client, bundle: bundle, source: semanticview.Wrap(bundle), bundleHash: bundleHash}
	state := &scenarioRunState{SetupEntities: map[string]scenarioSetupEntityBinding{}}
	step := scenarioStep{Action: "publish", PublishEvent: "item_received", GeneratePayload: true}
	if err := runner.runPublishStep(context.Background(), scenarioTestFile{}, mustScenarioExpressionEvaluatorWithSeed(t, "public-rpc"), state, step); err != nil {
		t.Fatalf("run generated publish step: %v", err)
	}
	if len(calls) != 1 {
		t.Fatalf("RPC calls = %#v, want one public event.publish", calls)
	}
}

func TestGeneratedInputFixtureLoadsComposedTelegramSchemaAndPublishesNormalizedEvent(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	setCLIAPITestToken(t, "test-token")
	contractsPath := canonicalrouting.CopyExample(t, canonicalrouting.TelegramAgent)
	bundle := loadWorkflowValidationBundleAt(t, contractsPath)
	const eventName = "inbound.telegram.text_message"
	if bare := semanticview.ResolveEventSchema(semanticview.Wrap(bundle), "telegram-chat", eventName); bare.HasSchema {
		t.Fatalf("bare authored bundle unexpectedly owns imported schema: %#v", bare)
	}

	configPath := contractsPath + "/swarm.yaml"
	writeRuntimeConfigText(t, configPath, withTestProviderTriggerPlatformInventory(t, "llm:\n  backend: mock\n"))
	configResult, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: contractsPath, ExplicitPath: configPath})
	if err != nil {
		t.Fatalf("load unified config: %v", err)
	}
	configured, err := LoadConfiguredProviderTriggerPacks(contractsPath, configResult)
	if err != nil {
		t.Fatalf("load configured provider trigger packs: %v", err)
	}
	source, err := runtimeroot.SourceWithProviderTriggerEvents(semanticview.Wrap(bundle), configured.Catalog)
	if err != nil {
		t.Fatalf("compose effective provider source: %v", err)
	}
	resolved := semanticview.ResolveEventSchema(source, "telegram-chat", eventName)
	if !resolved.HasSchema {
		t.Fatalf("composed source did not resolve imported Telegram schema: %#v", resolved)
	}
	census := semanticview.BuildAuthoredEventEndpointCensus(source)
	association := census.ResolveDeclaredInputEndpoint("telegram-chat", "telegram_text_message")
	endpoint, ok := association.Endpoint()
	if !ok || endpoint.PinName != "telegram_text_message" || endpoint.Event.EventKey() != eventName {
		t.Fatalf("exact input census association = %#v, want telegram_text_message -> %s", association, eventName)
	}

	bundleHash, err := runtimecontracts.BundleHash(bundle)
	if err != nil {
		t.Fatalf("bundle hash: %v", err)
	}
	wantPayload := map[string]any{
		"conversation_reference":     "0",
		"external_account_reference": "0",
		"provider_message_reference": float64(1),
		"text":                       "0",
	}
	var call jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if err := json.NewDecoder(request.Body).Decode(&call); err != nil {
			t.Fatalf("decode RPC: %v", err)
		}
		if call.Method != eventPublishMethod || call.Params["event_name"] != eventName || call.Params["bundle_hash"] != bundleHash {
			t.Fatalf("event.publish params = %#v", call.Params)
		}
		if !reflect.DeepEqual(call.Params["payload"], wantPayload) {
			t.Fatalf("generated Telegram payload = %#v, want %#v", call.Params["payload"], wantPayload)
		}
		writeJSONRPCResult(t, w, call.ID, eventPublishTestResult(true))
	}))
	defer server.Close()
	client, err := newCLIAPIClient(rootCommandOptions{apiServer: strings.TrimSuffix(server.URL, "/")})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	runner := scenarioRunner{client: client, bundle: bundle, source: source, bundleHash: bundleHash}
	state := &scenarioRunState{SetupEntities: map[string]scenarioSetupEntityBinding{}}
	step := scenarioStep{Action: "publish", PublishEvent: "telegram_text_message", GeneratePayload: true}
	if err := runner.runPublishStep(context.Background(), scenarioTestFile{FlowID: "telegram-chat"}, mustScenarioExpressionEvaluatorWithSeed(t, "telegram-normalized"), state, step); err != nil {
		t.Fatalf("run generated Telegram publish step: %v", err)
	}
	if err := runtimeeventschema.ValidatePayloadAgainstSchema(resolved.Schema.Schema, wantPayload); err != nil {
		t.Fatalf("generated Telegram payload failed imported schema validation: %v", err)
	}
}

func TestAuthoredTelegramInputFixturesUseEffectivePublishSchemaAndPublicRPC(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	setCLIAPITestToken(t, "test-token")
	contractsPath := canonicalrouting.CopyExample(t, canonicalrouting.TelegramAgent)
	bundle := loadWorkflowValidationBundleAt(t, contractsPath)
	configPath := contractsPath + "/swarm.yaml"
	writeRuntimeConfigText(t, configPath, withTestProviderTriggerPlatformInventory(t, "llm:\n  backend: mock\n"))
	configResult, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: contractsPath, ExplicitPath: configPath})
	if err != nil {
		t.Fatalf("load unified config: %v", err)
	}
	configured, err := LoadConfiguredProviderTriggerPacks(contractsPath, configResult)
	if err != nil {
		t.Fatalf("load configured provider trigger packs: %v", err)
	}
	source, err := runtimeroot.SourceWithProviderTriggerEvents(semanticview.Wrap(bundle), configured.Catalog)
	if err != nil {
		t.Fatalf("compose effective provider source: %v", err)
	}
	bundleHash, err := runtimecontracts.BundleHash(bundle)
	if err != nil {
		t.Fatalf("bundle hash: %v", err)
	}
	wantPayload := map[string]any{
		"conversation_reference":     "123",
		"external_account_reference": "456",
		"provider_message_reference": float64(789),
		"text":                       "authored fixture",
	}
	fixturePath := filepath.Join(contractsPath, "flows", "telegram-chat", "tests", "fixtures", "telegram.yaml")
	writeWorkflowValidationFixtureFile(t, fixturePath, `
conversation_reference: "123"
external_account_reference: "456"
provider_message_reference: 789
text: authored fixture
`)
	scenarioPath := filepath.Join(contractsPath, "flows", "telegram-chat", "tests", "scenario.yaml")

	for _, test := range []struct {
		name    string
		payload any
	}{
		{name: "inline", payload: map[string]any{
			"conversation_reference":     "123",
			"external_account_reference": "456",
			"provider_message_reference": float64(789),
			"text":                       "authored fixture",
		}},
		{name: "file", payload: map[string]any{"from": "fixtures/telegram.yaml"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var call jsonRPCRequest
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				if err := json.NewDecoder(request.Body).Decode(&call); err != nil {
					t.Fatalf("decode RPC: %v", err)
				}
				if call.Method != eventPublishMethod || call.Params["event_name"] != "inbound.telegram.text_message" || call.Params["bundle_hash"] != bundleHash {
					t.Fatalf("event.publish params = %#v", call.Params)
				}
				if !reflect.DeepEqual(call.Params["payload"], wantPayload) {
					t.Fatalf("authored Telegram payload = %#v, want %#v", call.Params["payload"], wantPayload)
				}
				writeJSONRPCResult(t, w, call.ID, eventPublishTestResult(true))
			}))
			defer server.Close()
			client, err := newCLIAPIClient(rootCommandOptions{apiServer: strings.TrimSuffix(server.URL, "/")})
			if err != nil {
				t.Fatalf("client: %v", err)
			}
			runner := scenarioRunner{
				client:       client,
				bundle:       bundle,
				source:       source,
				bundleHash:   bundleHash,
				contractsDir: contractsPath,
			}
			step := scenarioStep{Action: "publish", PublishEvent: "telegram_text_message", Payload: test.payload}
			state := &scenarioRunState{SetupEntities: map[string]scenarioSetupEntityBinding{}}
			if err := runner.runPublishStep(
				context.Background(),
				scenarioTestFile{FlowID: "telegram-chat", Path: scenarioPath},
				mustScenarioExpressionEvaluatorWithSeed(t, "authored-telegram-"+test.name),
				state,
				step,
			); err != nil {
				t.Fatalf("run %s Telegram publish step: %v", test.name, err)
			}
		})
	}
}

func TestSwarmTestGeneratesConfiguredTelegramInputThroughProductionCommand(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	setCLIAPITestToken(t, "test-token")
	contractsPath := canonicalrouting.CopyExample(t, canonicalrouting.TelegramAgent)
	configPath := filepath.Join(contractsPath, "swarm.yaml")
	writeRuntimeConfigText(t, configPath, withTestProviderTriggerPlatformInventory(t, "llm:\n  backend: mock\n"))
	scenarioPath := filepath.Join(contractsPath, "flows", "telegram-chat", "tests", "generated-telegram.yaml")
	writeWorkflowValidationFixtureFile(t, scenarioPath, `
name: generated Telegram normalized input
steps:
  - publish: telegram_text_message
    payload: generate
`)
	bundleHash := servedEventPublishFixtureBundleHash(t, contractsPath)
	wantPayload := map[string]any{
		"conversation_reference":     "0",
		"external_account_reference": "0",
		"provider_message_reference": float64(1),
		"text":                       "0",
	}
	var calls []jsonRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		var rpc jsonRPCRequest
		if err := json.NewDecoder(request.Body).Decode(&rpc); err != nil {
			t.Fatalf("decode RPC: %v", err)
		}
		calls = append(calls, rpc)
		switch rpc.Method {
		case eventPublishMethod:
			if rpc.Params["event_name"] != "inbound.telegram.text_message" || rpc.Params["bundle_hash"] != bundleHash {
				t.Fatalf("event.publish params = %#v", rpc.Params)
			}
			if !reflect.DeepEqual(rpc.Params["payload"], wantPayload) {
				t.Fatalf("generated Telegram payload = %#v, want %#v", rpc.Params["payload"], wantPayload)
			}
			writeJSONRPCResult(t, w, rpc.ID, eventPublishTestResult(true))
		case "run.diagnose":
			writeJSONRPCResult(t, w, rpc.ID, scenarioRunDiagnoseTestResult("run-1", true))
		default:
			t.Fatalf("unexpected method %q", rpc.Method)
		}
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), contractsPath, []string{
		"test",
		"--contracts", contractsPath,
		"--config", configPath,
		"--timeout", "2s",
		"--poll-interval", "10ms",
		filepath.ToSlash(filepath.Join("flows", "telegram-chat", "tests", "generated-telegram.yaml")),
	}, &stdout, &stderr, testRootCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	assertScenarioTestMethods(t, calls, []string{eventPublishMethod, "run.diagnose", "run.diagnose"})
	if !strings.Contains(stdout.String(), "swarm test ok: scenarios=1") || strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}

func TestGeneratedInputFixtureRejectsPayloadOmissionAndGeneratedInvalidBase(t *testing.T) {
	bundle := loadWorkflowValidationBundleAt(t, writeServedEventPublishFollowUpFixture(t))
	runner := scenarioRunner{bundle: bundle, source: semanticview.Wrap(bundle)}
	evaluator := mustScenarioExpressionEvaluatorWithSeed(t, "missing-payload")
	if _, _, err := runner.buildPublishPayload(scenarioTestFile{}, evaluator, scenarioStep{PublishEvent: "item.received"}); err == nil || !strings.Contains(err.Error(), "payload is required") {
		t.Fatalf("omitted payload error = %v", err)
	}
	if _, err := invalidBasePublishStep(map[string]any{"publish": "item.received", "payload": "generate"}); err == nil || !strings.Contains(err.Error(), "generated invalid-case bases are not supported") {
		t.Fatalf("generated invalid base error = %v", err)
	}
	withoutSource := scenarioRunner{bundle: bundle}
	if _, err := withoutSource.compileGeneratedInputFixture(scenarioTestFile{}, evaluator, "item.received"); err == nil || !strings.Contains(err.Error(), "effective semantic source") {
		t.Fatalf("missing effective source error = %v", err)
	}
}

func mustScenarioExpressionEvaluatorWithSeed(t *testing.T, seed string) *scenarioExpressionEvaluator {
	t.Helper()
	evaluator, err := newScenarioExpressionEvaluator(seed, nil)
	if err != nil {
		t.Fatalf("new scenario evaluator: %v", err)
	}
	return evaluator
}

func generatedInputFixtureBundle() *runtimecontracts.WorkflowContractBundle {
	alphaEntry := runtimecontracts.EventCatalogEntry{
		Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{
			"alpha": {ExactSchema: exactToolInputSchema(runtimecontracts.MustToolInputSchema(
				runtimecontracts.ToolSchemaString,
				runtimecontracts.ToolSchemaMinLength(12),
				runtimecontracts.ToolSchemaMaxLength(12),
			))},
			"id": {ExactSchema: exactToolInputSchema(runtimecontracts.MustToolInputSchema(
				runtimecontracts.ToolSchemaString,
				runtimecontracts.ToolSchemaFormat("uuid"),
			))},
		}},
		Required: []string{"alpha", "id"},
	}
	betaEntry := runtimecontracts.EventCatalogEntry{
		Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{
			"beta": {ExactSchema: exactToolInputSchema(runtimecontracts.MustToolInputSchema(
				runtimecontracts.ToolSchemaInteger,
				runtimecontracts.ToolSchemaMinimum(2),
			))},
		}},
		Required: []string{"beta"},
	}
	unsupportedEntry := runtimecontracts.EventCatalogEntry{
		Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{
			"code": {ExactSchema: exactToolInputSchema(runtimecontracts.MustToolInputSchema(
				runtimecontracts.ToolSchemaString,
				runtimecontracts.ToolSchemaMinLength(1),
				runtimecontracts.ToolSchemaPattern("^[A-Z]+$"),
			))},
		}},
		Required: []string{"code"},
	}
	alpha := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "alpha", Flow: "alpha", PackageKey: "flows/alpha"},
		Path:  "alpha",
		Schema: runtimecontracts.FlowSchemaDocument{Pins: runtimecontracts.FlowPins{Inputs: runtimecontracts.FlowInputPins{
			Events: []string{"shared.received", "unsupported.received"},
			EventPins: []runtimecontracts.FlowInputEventPin{
				{Name: "entry", Event: "shared.received"},
				{Name: "unsupported", Event: "unsupported.received"},
			},
		}}},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"shared.received":      alphaEntry,
			"unsupported.received": unsupportedEntry,
		},
	}
	beta := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "beta", Flow: "beta", PackageKey: "flows/beta"},
		Path:  "beta",
		Schema: runtimecontracts.FlowSchemaDocument{Pins: runtimecontracts.FlowPins{Inputs: runtimecontracts.FlowInputPins{
			Events:    []string{"shared.received"},
			EventPins: []runtimecontracts.FlowInputEventPin{{Name: "entry", Event: "shared.received"}},
		}}},
		Events: map[string]runtimecontracts.EventCatalogEntry{"shared.received": betaEntry},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{alpha, beta}}
	root.Children[0].Parent = &root
	root.Children[1].Parent = &root
	return &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"alpha": &root.Children[0],
				"beta":  &root.Children[1],
			},
			ByPath: map[string]*runtimecontracts.FlowContractView{
				"alpha": &root.Children[0],
				"beta":  &root.Children[1],
			},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"alpha": root.Children[0].Schema,
			"beta":  root.Children[1].Schema,
		},
	}
}

func exactToolInputSchema(schema runtimecontracts.ToolInputSchema) *runtimecontracts.ToolInputSchema {
	return &schema
}
