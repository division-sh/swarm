package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/packadmission"
	runtimeactivityidentity "github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func VerifyS1ScopedActivityDispatchUsesExactDeclarationForTest(t *testing.T, open func(*testing.T, semanticview.Source) WorkflowActivityNativeFixtureForTest, compose func(*testing.T, semanticview.Source) semanticview.Source) {
	t.Helper()
	if compose == nil {
		t.Fatal("local activity journey requires the real effective-source compiler")
	}
	verifyS1ScopedActivityDispatch(t, open, compose, false)
	t.Run("registered_account_credentials", func(t *testing.T) {
		files := map[string]string{
			"manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
			"schema.yaml":   "stages: []\npins:\n  inputs: [inbound.telegram.text_message]\n",
			"policy.yaml":   "operator_account: '420079'\n",
		}
		for _, role := range []string{"manifest", "trigger", "connector", "channel"} {
			body, err := os.ReadFile(filepath.Join(WorkflowRepoRoot(), "packs", "providers", "telegram", role+".yaml"))
			if err != nil {
				t.Fatal(err)
			}
			files["telegram/"+role+".yaml"] = string(body)
		}
		bundle := loadWorkflowTempBundle(t, files)
		source := compose(t, semanticview.Wrap(bundle))
		rooted, err := bundle.RootedSource()
		if err != nil {
			t.Fatal(err)
		}
		providers, err := packadmission.AdmitProviderSource(rooted, bundle.Platform)
		if err != nil {
			t.Fatal(err)
		}
		scope, _ := providers.Scope(".")
		provider, _ := scope.Provider("telegram")
		plan, _ := provider.ChannelPlan()
		profile, found := plan.OnboardingProfile()
		if !found {
			t.Fatal("registered credential control requires its exact provider role")
		}
		role := profile.ProviderCredential()
		connector, _ := provider.ConnectorDescriptor()
		channelIdentity, err := plan.InterfaceIdentity()
		if err != nil {
			t.Fatal(err)
		}
		fixture := open(t, source)
		ctx := fixture.Context
		selected := fixture.AccountRegistrationOwners
		if selected == nil {
			t.Fatal("activity fixture lacks its existing deployment registration owners")
		}
		principal, err := selected.EnsureOperatorPrincipal(ctx, time.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}
		accounts := selected.DeploymentChannelOperations()
		file, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "account-credentials.json"))
		if err != nil {
			t.Fatal(err)
		}
		writer, err := channelonboarding.NewCredentialWriter(file)
		if err != nil {
			t.Fatal(err)
		}
		operations := []channelonboarding.Operation{}
		for _, account := range []string{"420079", "420080"} {
			id := uuid.NewString()
			admitted, err := writer.Admit(ctx, channelonboarding.CredentialWriteRequest{StoreKey: "telegram." + account, Value: account + ":fixture-token", Receipt: id})
			if err != nil {
				t.Fatal(err)
			}
			op, err := accounts.ReserveChannelOnboarding(ctx, channelonboarding.StartRequest{DeploymentRegistration: true,
				ExpectedAccount: account, OperationID: id, PrincipalID: principal.ID, RequestKeyHash: id, RequestHash: id,
				Provider: "telegram", Interface: channelIdentity, Verb: channelonboarding.VerbConnect,
				CredentialReservations: []channelonboarding.CredentialReservation{{Role: role, StoreKey: admitted.StoreKey}},
				Posture:                channelonboarding.ActivationWebhookRegistration, Ceremony: channelonboarding.CeremonyAccountLink, RequestedAt: time.Now().UTC()})
			if err != nil {
				t.Fatal(err)
			}
			for _, phase := range []channelonboarding.Phase{channelonboarding.PhaseCredentialsAdmitted, channelonboarding.PhaseActivatingProvider,
				channelonboarding.PhaseAwaitingExternalIdentity, channelonboarding.PhaseAwaitingExternalIdentity,
				channelonboarding.PhaseAwaitingOperatorConfirmation, channelonboarding.PhaseSucceeded} {
				req := channelonboarding.AdvanceRequest{OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: phase, Now: time.Now().UTC()}
				if phase == channelonboarding.PhaseCredentialsAdmitted {
					req.ReplaceCredentialAdmissions = true
					req.CredentialAdmissions = []channelonboarding.CredentialAdmission{{Role: role, StoreKey: admitted.StoreKey,
						Kind: channelonboarding.CredentialAdmissionWritten, Receipt: admitted.Receipt, ValueSeal: admitted.ValueSeal}}
				}
				if phase == channelonboarding.PhaseAwaitingExternalIdentity {
					if op.Phase == phase {
						req.ConfirmAccount = true
					} else {
						req.AccountReference = account
					}
				}
				op, err = accounts.AdvanceChannelOnboarding(ctx, req)
				if err != nil {
					t.Fatal("registered credential fixture", phase, err)
				}
			}
			operations = append(operations, op)
		}
		pc := fixture.NewCoordinator(&recordingPipelineBus{}, PipelineCoordinatorOptions{Module: staticSemanticWorkflowModule{source: source}, ProviderCredentials: file, RegisteredAccounts: accounts})
		tool, present := connector.Tools["telegram.send_message"]
		if !present {
			t.Fatal("registered credential control lacks its exact admitted tool")
		}
		var currents []func(context.Context) error
		for _, account := range []string{"420079", "420080", "420081", ""} {
			intent := testNonIdempotentActivityIntent(uuid.NewString(), uuid.NewString(), uuid.NewString())
			intent.Tool, intent.ExecutionFlowID = "telegram.send_message", identity.NormalizeFlowID(".")
			intent.Input = mustActivityInput(map[string]any{"account": account, "chat_id": "17", "text": "customer reply"})
			values, _, current, matched, err := pc.accountActivityCredentials(ctx, intent, tool)
			if account == "420081" || account == "" {
				if err == nil || !matched {
					t.Fatal("missing/unlinked account fell back to another credential")
				}
				continue
			}
			if err != nil || !matched || len(values) != 1 || values[role] != account+":fixture-token" || current == nil || current(ctx) != nil {
				t.Fatal("authored activity did not select only its exact account credential", err)
			}
			prepared, err := (pipelineActivityDispatcher{coordinator: pc}).prepareActivityHTTPTool(ctx, nil, intent, tool)
			if err != nil || !strings.Contains(prepared.url, "bot"+account+":fixture-token/sendMessage") || prepared.validateAdmission == nil || prepared.validateAdmission(ctx) != nil {
				t.Fatal("HTTP preparation bypassed its selected account credential", err)
			}
			currents = append(currents, prepared.validateAdmission)
		}
		if released, err := writer.Release(ctx, operations[0].CredentialAdmissions[0]); err != nil || !released {
			t.Fatal("withdraw exact account credential", released, err)
		}
		if currents[0](ctx) == nil || currents[1](ctx) != nil {
			t.Fatal("withdrawing one account credential did not fence only its own prepared activity")
		}
	})
}

func VerifyS1DirectoryConnectorDispatchUsesExactDeclarationForTest(t *testing.T, open func(*testing.T, semanticview.Source) WorkflowActivityNativeFixtureForTest, compose func(*testing.T, semanticview.Source) semanticview.Source) {
	t.Helper()
	if compose == nil {
		t.Fatal("directory journey requires the real effective-source compiler")
	}
	verifyS1ScopedActivityDispatch(t, open, compose, true)
}

func verifyS1ScopedActivityDispatch(t *testing.T, open func(*testing.T, semanticview.Source) WorkflowActivityNativeFixtureForTest, compose func(*testing.T, semanticview.Source) semanticview.Source, directory bool) {
	t.Helper()
	toolID := "provider_write"
	if directory {
		toolID = "team-bot.send"
	}
	var firstCalls, secondCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if directory && request.Header.Get("Authorization") != "Bearer directory-fixture-secret" {
			t.Errorf("directory connector did not consume its exact qualified credential")
		}
		origin := "research"
		switch request.URL.Path {
		case "/research":
			firstCalls.Add(1)
		case "/research2":
			origin = "research2"
			secondCalls.Add(1)
		default:
			t.Errorf("unexpected scoped endpoint %q", request.URL.Path)
			http.Error(w, "wrong endpoint", http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"origin": origin})
	}))
	defer server.Close()
	files := map[string]string{"manifest.yaml": "version: 1.0.0\nplatform_version: \">=0.7.0 <0.8.0\"\n",
		"schema.yaml": "stages: []\n"}
	for _, flow := range []string{"research", "research2"} {
		files[flow+"/manifest.yaml"] = "version: 1.0.0\nplatform_version: \">=0.7.0 <0.8.0\"\n"
		files[flow+"/schema.yaml"] = "stages: []\n"
		files[flow+"/events.yaml"] = "request:\n"
		files[flow+"/nodes.yaml"] = fmt.Sprintf("scanner:\n  execution_type: system_node\n  subscribes_to: [request]\n  event_handlers:\n    request:\n      activity: {id: send, tool: %s, input: {}}\n", toolID)
		tool := fmt.Sprintf("provider_write:\n  handler_type: http\n  input_schema: {type: object, additionalProperties: false}\n  output_schema:\n    type: object\n    required: [origin]\n    properties: {origin: {type: string, enum: [%s]}}\n  http: {method: POST, url: %q}\n", flow, server.URL+"/"+flow)
		if !directory {
			files[flow+"/tools.yaml"] = tool
		} else {
			files[flow+"/team-bot/manifest.yaml"] = "version: 1.0.0\nplatform_version: '*'\ncapabilities: {can: {call_provider_actions: [send]}, cannot: [bypass journal]}\n"
			files[flow+"/team-bot/connector.yaml"] = fmt.Sprintf("tools:\n  send:\n    category: provider_connector\n    handler_type: http\n    credentials: [token]\n    input_schema: {type: object, additionalProperties: false}\n    output_schema:\n      type: object\n      required: [origin]\n      properties: {origin: {type: string, enum: [%s]}}\n    http:\n      method: POST\n      url: %q\n      headers: {Authorization: 'Bearer {{credentials.token}}'}\n    response_success: {kind: http_status_2xx}\n", flow, server.URL+"/"+flow)
		}
	}
	source := semanticview.Wrap(loadWorkflowTempBundle(t, files))
	source = compose(t, source)
	if _, present := source.ToolEntries()[toolID]; present {
		t.Fatal("fixture did not remove the ambiguous global tool alias")
	}
	fixture := open(t, source)
	ctx := fixture.Context
	runID := uuid.NewString()
	if err := fixture.RequireRun(ctx, runID); err != nil {
		t.Fatal(err)
	}
	bus := &recordingPipelineBus{}
	options := PipelineCoordinatorOptions{Module: staticSemanticWorkflowModule{source: source}}
	if directory {
		options.Credentials = testActivityCredentialStore(t, "team-bot.token", "directory-fixture-secret")
	}
	pc := fixture.NewCoordinator(bus, options)
	var expectedStatuses []string
	for _, flow := range []string{"research", "research2"} {
		intent := testNonIdempotentActivityIntent(runID, uuid.NewString(), uuid.NewString())
		intent.Input = mustActivityInput(map[string]any{})
		intent.ActivityID = "send"
		intent.Tool = toolID
		intent.Owner = runtimeactivityidentity.MustNodeOwner(mustPipelineNode(flow, "scanner"))
		intent.ExecutionFlowID = identity.NormalizeFlowID(flow)
		intent.FlowInstance = flow + "/entity-1"
		intent.HandlerEventKey = "request"
		intent.SuccessEvent = flow + "/send.succeeded"
		intent.FailureEvent = flow + "/send.failed"
		routing, err := events.NewConcreteTemplateInstanceRoutingSource(events.RouteIdentity{
			FlowID: flow, FlowInstance: intent.FlowInstance, EntityID: intent.EntityID.String(),
		})
		if err != nil {
			t.Fatal(err)
		}
		intent.RoutingSource = routing
		request, err := activityRequestEmitIntent(intent)
		if err != nil {
			t.Fatal(err)
		}
		expectedStatuses = append(expectedStatuses, request.Event.ID()+":succeeded")
		for replay := 0; replay < 2; replay++ {
			handled, _, err := pc.handleEventResult(ctx, request.Event)
			if err != nil || !handled {
				t.Fatalf("scoped dispatch/replay %s/%d: handled=%t err=%v", flow, replay, handled, err)
			}
			if len(bus.publishes) == 0 {
				t.Fatal("scoped activity produced no result")
			}
			last := bus.publishes[len(bus.publishes)-1]
			if last.Type() != events.EventType(flow+"/entity-1/send.succeeded") {
				t.Fatalf("scoped call did not produce its success: %s %s", last.Type(), last.Payload())
			}
			var payload struct{ Result struct{ Origin string } }
			if err := json.Unmarshal(last.Payload(), &payload); err != nil || payload.Result.Origin != flow {
				t.Fatalf("scoped call used another endpoint/schema: %+v %v", payload, err)
			}
		}
		if bus.publishes[len(bus.publishes)-1].ID() != bus.publishes[len(bus.publishes)-2].ID() {
			t.Fatal("duplicate activity changed the canonical journaled result identity")
		}
	}
	if firstCalls.Load() != 1 || secondCalls.Load() != 1 || len(bus.publishes) != 4 {
		t.Fatalf("scoped replay redispatched or lost a result: calls=%d/%d results=%d", firstCalls.Load(), secondCalls.Load(), len(bus.publishes))
	}
	journal, err := fixture.ReadJournal(ctx, runID)
	if err != nil || journal.Attempts != 2 {
		t.Fatalf("scoped journal ownership differs: %+v %v", journal, err)
	}
	statuses, err := fixture.ReadAttemptStatuses(ctx)
	slices.Sort(expectedStatuses)
	if err != nil || !slices.Equal(statuses, expectedStatuses) {
		t.Fatalf("scoped journal did not settle both exact effects: %+v %v", statuses, err)
	}
}
