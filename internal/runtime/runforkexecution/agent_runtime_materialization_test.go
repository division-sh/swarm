package runforkexecution

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/mockperformance"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

const selectedContractAgentTestBundleHash = "bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"

func selectedContractTestRootAgentIdentity(t testing.TB, agentID string) agentidentity.Identity {
	return selectedContractTestAgentIdentity(t, agentID, "")
}

func selectedContractTestAgentIdentity(t testing.TB, agentID, flowInstance string) agentidentity.Identity {
	t.Helper()
	name, err := agentidentity.DeclaredName(agentID, "test://selected-contract/agents/"+agentID)
	if err != nil {
		t.Fatalf("construct selected-contract agent name: %v", err)
	}
	route := agentidentity.RootRoute()
	if flowInstance != "" {
		route, err = runtimeflowidentity.StoredRoute("", "", flowInstance).AgentIdentityRoute()
		if err != nil {
			t.Fatalf("construct selected-contract agent route: %v", err)
		}
	}
	identity, err := agentidentity.New(name, route)
	if err != nil {
		t.Fatalf("construct selected-contract agent identity: %v", err)
	}
	return identity
}

func containsSelectedContractAgentID(identities []agentidentity.Identity, agentID string) bool {
	for _, identity := range identities {
		if identity.AgentID() == agentID {
			return true
		}
	}
	return false
}

func selectedContractAgentTestSourceFact(t *testing.T) runtimecorrelation.BundleSourceFact {
	t.Helper()
	fact, err := runtimecorrelation.NewEphemeralBundleSourceFact(selectedContractAgentTestBundleHash)
	if err != nil {
		t.Fatalf("construct selected-contract source fact: %v", err)
	}
	return fact
}

func selectedContractTestDeclarationTopology(t testing.TB) runtimeagenttopology.Admission {
	t.Helper()
	topology, err := runtimeagenttopology.StaticAdmission(
		"selected-contract-test-source-set-v1",
		selectedContractAgentTestBundleHash,
		"ephemeral",
		runtimeagenttopology.LifetimeEphemeral,
	)
	if err != nil {
		t.Fatalf("construct selected-contract declaration topology: %v", err)
	}
	return topology
}

func selectedContractTestProcessCapability(
	t testing.TB,
	ctx context.Context,
	selected runtimestartupownership.Store,
	loaded LoadedSelectedContractSource,
) runtimestartupownership.ProcessCapability {
	t.Helper()
	if selected == nil {
		t.Fatal("selected-contract topology fixture requires a selected store")
	}
	bundleHash, bundleSource := loaded.BundleSourceFact.StorageValues()
	coordinate := runtimeagenttopology.SourceCoordinate{BundleHash: bundleHash, BundleSource: bundleSource}
	records, err := selectedContractStaticAgentRecords(loaded.Source)
	if err != nil {
		t.Fatalf("compile selected-contract declaration records: %v", err)
	}
	desiredByIdentity := make(map[string]runtimeagenttopology.DesiredAgent, len(records))
	for _, record := range records {
		identity, identityErr := record.Config.ConcreteIdentity()
		if identityErr != nil {
			t.Fatalf("selected-contract declaration identity: %v", identityErr)
		}
		revision, revisionErr := canonicaljson.Hash(record.Config)
		if revisionErr != nil {
			t.Fatalf("selected-contract declaration revision: %v", revisionErr)
		}
		candidate := runtimeagenttopology.DesiredAgent{
			Identity: identity, Source: coordinate, ConfigRevision: revision,
		}
		key, keyErr := candidate.Key()
		if keyErr != nil {
			t.Fatalf("selected-contract declaration key: %v", keyErr)
		}
		if previous, exists := desiredByIdentity[key]; exists && previous != candidate {
			t.Fatalf("selected-contract declaration %s compiles inconsistently", identity.Description())
		}
		desiredByIdentity[key] = candidate
	}
	desired := make([]runtimeagenttopology.DesiredAgent, 0, len(desiredByIdentity))
	for _, candidate := range desiredByIdentity {
		desired = append(desired, candidate)
	}
	plan, err := runtimeagenttopology.NewSourceSetPlan([]runtimeagenttopology.SourceCoordinate{coordinate}, desired)
	if err != nil {
		t.Fatalf("construct selected-contract source set: %v", err)
	}
	capability, err := selected.AcquireProcessCapability(ctx, runtimestartupownership.AcquireRequest{
		OwnerID: "selected-contract-test", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString(),
	})
	if err != nil {
		t.Fatalf("acquire selected-contract process capability: %v", err)
	}
	if _, err := capability.InstallCompleteSourceSet(ctx, runtimeagenttopology.SourceSetCommitRequest{
		OperationID: uuid.NewString(), Plan: plan,
	}); err != nil {
		_ = capability.Release(context.Background())
		t.Fatalf("install selected-contract source set: %v", err)
	}
	t.Cleanup(func() {
		if err := capability.Release(context.Background()); err != nil {
			t.Errorf("release selected-contract process capability: %v", err)
		}
	})
	return capability
}

func TestSelectedContractAgentRuntimeWaitsForCurrentRouteSettlementAfterPredecessorRetirement(t *testing.T) {
	processOwner := worklifetime.NewProcess()
	runtimeOwner, err := processOwner.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
		RuntimeInstanceID: "selected-contract-test-runtime",
		BundleHash:        selectedContractAgentTestBundleHash,
	})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	eventBus, err := runtimebus.NewEphemeralEventBusWithOptions(nil, runtimebus.EventBusOptions{
		ExecutionPosture: executionposture.Live,
		BundleSourceFact: selectedContractAgentTestSourceFact(t),
		WorkOwner:        runtimeOwner, ReceiverExecution: eventreceiver.NormalExecution(),
	})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	forkIdentity := selectedContractTestRootAgentIdentity(t, "fork-agent")
	oldToken := runtimeeffects.LifecycleToken{RuntimeEpoch: 7, Identity: forkIdentity, AgentID: "fork-agent", Generation: 1}
	newToken := runtimeeffects.LifecycleToken{RuntimeEpoch: 7, Identity: forkIdentity, AgentID: "fork-agent", Generation: 2}
	oldRoute := eventBus.ReplaceAgentRoute(oldToken, selectedContractAgentRouteAdmission(t, oldToken.AgentID, "item.received"))
	if oldRoute == nil {
		t.Fatal("predecessor route was not installed")
	}
	oldEvent := eventtest.RuntimeControl(eventtest.UUID("old-work"), events.EventType("item.received"), "test", "", []byte(`{}`), 0, eventtest.UUID("run-1"), "", events.EventEnvelope{}, time.Now())
	if err := eventBus.Publish(context.Background(), oldEvent); err != nil {
		t.Fatalf("publish predecessor event: %v", err)
	}
	oldDelivery := <-oldRoute
	replaced := make(chan (<-chan *runtimebus.LocalDelivery), 1)
	go func() {
		replaced <- eventBus.ReplaceAgentRoute(newToken, selectedContractAgentRouteAdmission(t, newToken.AgentID, "item.received"))
	}()
	select {
	case <-replaced:
		t.Fatal("replacement returned before predecessor delivery settled")
	case <-time.After(25 * time.Millisecond):
	}
	if err := oldDelivery.Complete(); err != nil {
		t.Fatalf("complete predecessor delivery: %v", err)
	}
	var newRoute <-chan *runtimebus.LocalDelivery
	select {
	case newRoute = <-replaced:
	case <-time.After(time.Second):
		t.Fatal("replacement did not complete after predecessor settlement")
	}
	newEvent := eventtest.RuntimeControl(eventtest.UUID("new-work"), events.EventType("item.received"), "test", "", []byte(`{}`), 0, eventtest.UUID("run-1"), "", events.EventEnvelope{}, time.Now())
	if err := eventBus.Publish(context.Background(), newEvent); err != nil {
		t.Fatalf("publish successor event: %v", err)
	}
	var newDelivery *runtimebus.LocalDelivery
	select {
	case newDelivery = <-newRoute:
	case <-time.After(time.Second):
		t.Fatal("successor event was not dequeued")
	}

	runtime := &selectedContractAgentRuntime{manager: runtimemanager.NewAgentManagerWithOptions(nil, nil, runtimemanager.AgentManagerOptions{WorkOwner: runtimeOwner, ReceiverExecution: eventreceiver.NormalExecution()})}
	waitCtx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if err := runtime.WaitForQuiescence(waitCtx, eventBus); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitForQuiescence with current route work = %v, want deadline exceeded", err)
	}
	if err := newDelivery.Complete(); err != nil {
		t.Fatalf("complete successor delivery: %v", err)
	}
	waitCtx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := runtime.WaitForQuiescence(waitCtx, eventBus); err != nil {
		t.Fatalf("WaitForQuiescence after current route settlement: %v", err)
	}
	eventBus.RemoveAgentRoute(newToken)
	if _, err := runtimeOwner.RetireAndWait(context.Background()); err != nil {
		t.Fatalf("retire runtime owner: %v", err)
	}
	if _, err := processOwner.Join(context.Background()); err != nil {
		t.Fatalf("join process owner: %v", err)
	}
}

func selectedContractAgentRouteAdmission(t *testing.T, agentID string, subscriptions ...string) semanticview.FlowOwnedAgentSubscriptionAdmission {
	t.Helper()
	admission, err := semanticview.AdmitFlowOwnedAgentSubscriptions(nil, semanticview.FlowOwnedAgentSubscriptionRequest{
		AgentID:       agentID,
		Subscriptions: subscriptions,
	})
	if err != nil {
		t.Fatalf("admit selected-contract agent route: %v", err)
	}
	return admission
}

type selectedContractSelfReleaseScopeProbe struct {
	want runtimeauthoractivity.Scope
	seen chan runtimeauthoractivity.Scope
}

func (p *selectedContractSelfReleaseScopeProbe) CommitAgentLifecycleTransition(ctx context.Context, req runtimemanager.AgentLifecycleTransition) (runtimemanager.AgentLifecycleTransitionResult, error) {
	if req.OperationKind == "self_release" {
		if err := ctx.Err(); err != nil {
			return runtimemanager.AgentLifecycleTransitionResult{}, fmt.Errorf("self-release context is canceled: %w", err)
		}
		scope, ok := runtimeauthoractivity.ScopeFromContext(ctx)
		if !ok {
			return runtimemanager.AgentLifecycleTransitionResult{}, fmt.Errorf("self-release author activity scope is required")
		}
		if scope != p.want {
			return runtimemanager.AgentLifecycleTransitionResult{}, fmt.Errorf("self-release author activity scope = %#v, want %#v", scope, p.want)
		}
		p.seen <- scope
	}
	return runtimemanager.AgentLifecycleTransitionResult{
		OperationID:        req.OperationID,
		TransitionID:       req.OperationID + "-transition",
		AgentID:            req.AgentID,
		PreviousEpoch:      req.ExpectedEpoch,
		RuntimeEpoch:       req.TargetEpoch,
		PreviousGeneration: req.ExpectedGeneration,
		Generation:         req.TargetGeneration,
		PreviousPhase:      req.ExpectedPhase,
		Phase:              req.TargetPhase,
		ConfigRevision:     req.ConfigRevision,
		RunMode:            req.RunMode,
	}, nil
}

type selectedContractSelfReleaseAgent struct {
	id string
}

func (a selectedContractSelfReleaseAgent) ID() string { return a.id }
func (selectedContractSelfReleaseAgent) Type() string { return "worker" }
func (selectedContractSelfReleaseAgent) Subscriptions() []events.EventType {
	return []events.EventType{"item.received"}
}
func (selectedContractSelfReleaseAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	return nil, nil
}

func TestSelectedContractAgentRuntimeBuildsCanonicalMockAdapter(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	selected := storetest.AdmitPostgresRuntimeStore(t, db)
	owner := testGatewayWorkOwner(t)
	mockIdentity := selectedContractTestRootAgentIdentity(t, "mock-agent")
	eventBus, err := runtimebus.NewEphemeralEventBusWithOptions(nil, runtimebus.EventBusOptions{
		ExecutionPosture: executionposture.Live,
		BundleSourceFact: selectedContractAgentTestSourceFact(t),
		WorkOwner:        owner, ReceiverExecution: eventreceiver.NormalExecution(),
	})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	builder, err := buildSelectedContractAgentRuntimeFactory(publishSelectedContractForkEventsRequest{
		Owner:        selectedContractExecutionOwnerForTest(t, selected),
		LoadedSource: LoadedSelectedContractSource{},
		AgentRuntime: selectedContractAgentRuntimePlan{
			Proof: SelectedContractAgentRuntimeMaterialization{AgentRecipients: []agentidentity.Identity{mockIdentity}},
			Options: SelectedContractAgentRuntimeOptions{
				ExecutionPosture: executionposture.Live,
				Config:           &config.Config{LLM: config.LLMConfig{Backend: llmselection.BackendClaudeCLI}},
				LLMRuntime:       runtimellm.NewNoopRuntime(runtimellm.ClaudeCLIProviderContract()),
				AgentManagerOptions: runtimemanager.AgentManagerOptions{
					WorkOwner: owner, ReceiverExecution: eventreceiver.NormalExecution(),
				},
			},
		},
	}, eventBus, &runtimepipeline.PipelineCoordinator{})
	if err != nil {
		t.Fatalf("build selected-contract mock runtime: %v", err)
	}
	if builder.factory == nil {
		t.Fatal("selected-contract mock runtime returned no agent factory")
	}
	actor := runtimeactors.AgentConfig{
		ID: "mock-agent", LLMBackend: llmselection.BackendMock,
		ResolvedLLMProvider: llmselection.ProviderMock, ResolvedLLMTransport: llmselection.TransportMock,
		ExecutionMode: runtimeeffects.ExecutionModeMock,
		Mock: mockperformance.Performance{
			Kind: mockperformance.KindPython, Module: "mocks/mock-agent.py", SourcePath: "mocks/mock-agent.py",
			Source: []byte("def handle(input):\n    return {'text': 'selected mock'}\n"), Digest: "sha256:selected-contract-mock-agent",
		},
	}
	resolved, err := builder.preflight.runtimes.ResolveAgentRuntime(actor)
	if err != nil {
		t.Fatalf("resolve selected-contract exact mock runtime: %v", err)
	}
	if _, ok := resolved.Runtime.(*runtimellm.MockRuntime); !ok {
		t.Fatalf("selected-contract exact mock runtime = %T, want *llm.MockRuntime", resolved.Runtime)
	}
	if builder.cleanup != nil {
		builder.cleanup()
	}
}

func TestSelectedContractAgentRecipientsPreserveConcreteTemplateInstanceIdentity(t *testing.T) {
	first := selectedContractTestAgentIdentity(t, "shared-agent", "review/inst-1")
	second := selectedContractTestAgentIdentity(t, "shared-agent", "review/inst-2")
	planning := runfork.RunForkSelectedContractRecipientPlanning{
		Owner: runfork.RunForkSelectedContractRecipientPlanningOwner,
		RecipientPlanEvents: []runfork.RunForkSelectedContractRecipientPlanEvent{{
			Recipients: []runfork.RunForkContractFrontierRecipient{
				testAgentFrontierRecipient("shared-agent", "review/inst-1", "", first),
				testAgentFrontierRecipient("shared-agent", "review/inst-2", "", second),
			},
		}},
	}
	recipients, err := selectedContractPlannedAgentRecipients(planning)
	if err != nil {
		t.Fatalf("selectedContractPlannedAgentRecipients: %v", err)
	}
	if len(recipients) != 2 || recipients[0] != first || recipients[1] != second {
		t.Fatalf("selected-contract recipients = %#v, want both concrete identities", recipients)
	}
	runtimeProof := SelectedContractAgentRuntimeMaterialization{
		Owner:                     runfork.RunForkSelectedContractForkLocalAgentRuntimeMaterializerExecutorOwner,
		AgentRecipients:           recipients,
		ConfiguredAgentIdentities: []agentidentity.Identity{first, second},
		MaterializationSupported:  true,
	}
	if !selectedContractAgentRuntimeCoversRecipients(runtimeProof, recipients) {
		t.Fatal("exact selected-contract runtime did not cover both concrete identities")
	}
	runtimeProof.ConfiguredAgentIdentities = []agentidentity.Identity{first}
	runtimeProof.AgentRecipients = []agentidentity.Identity{first}
	if selectedContractAgentRuntimeCoversRecipients(runtimeProof, recipients) {
		t.Fatal("slug-only first-instance runtime covered a sibling concrete identity")
	}
}

func TestStartSelectedContractAgentRuntimeDetachesCancellationAndPreservesForkScopeForSelfRelease(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	selected := storetest.AdmitPostgresRuntimeStore(t, db)
	owner := testGatewayWorkOwner(t)
	authority := runtimeeffects.Authority{
		Kind: runtimeeffects.AuthoritySelectedContractFork, ID: "00000000-0000-0000-0000-000000000311",
		SelectedFork: runtimeeffects.SelectedContractForkAuthority{
			ExecutionID: "00000000-0000-0000-0000-000000000311", ForkRunID: "00000000-0000-0000-0000-000000000312", Generation: 1,
			AdmissionFingerprint: "admission", ContainerPlanFingerprint: "container", ActorCensusFingerprint: "actors", EffectiveConfigFingerprint: "config",
		},
		ExecutionOwner: "self-release-scope-test", LeaseExpiresAt: time.Now().UTC().Add(time.Minute), FenceGeneration: 1,
		ExecutionMode: runtimeeffects.ExecutionModeLive,
	}
	wantScope := runtimeauthoractivity.BundleScope(
		"00000000-0000-0000-0000-000000000313",
		"bundle-v1:sha256:3131313131313131313131313131313131313131313131313131313131313131",
	)
	initiatingCtx, cancel := context.WithCancel(context.Background())
	ctx := selectedForkExecutionTestContext(t, initiatingCtx, authority)
	ctx = runtimeauthoractivity.WithScope(ctx, wantScope)
	admission, ok := managedexecution.FromContext(ctx)
	if !ok {
		t.Fatal("selected-contract test admission is missing")
	}
	receiverExecution, err := eventreceiver.SelectedContractForkExecution(
		authority,
		admission,
		liveTestCompletionController(selected, selected, selected, selectedForkDiscardSpendProjection{}),
		runtimecorrelation.RuntimeLineage{},
	)
	if err != nil {
		t.Fatalf("construct selected-contract receiver execution: %v", err)
	}
	eventBus, err := runtimebus.NewEphemeralEventBusWithOptions(nil, runtimebus.EventBusOptions{
		ExecutionPosture:  executionposture.Live,
		BundleSourceFact:  selectedContractAgentTestSourceFact(t),
		WorkOwner:         owner,
		ReceiverExecution: receiverExecution,
	})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	probe := &selectedContractSelfReleaseScopeProbe{want: wantScope, seen: make(chan runtimeauthoractivity.Scope, 1)}

	runtime, _, err := startSelectedContractAgentRuntime(ctx, publishSelectedContractForkEventsRequest{
		Owner: selectedContractExecutionOwnerForTest(t, selected),
		AgentRuntime: selectedContractAgentRuntimePlan{
			Records: []runtimemanager.PersistedAgent{{Config: selectedContractTestAgentConfig(t, runtimeactors.AgentConfig{
				ID: "fork-agent", Identity: selectedContractTestRootAgentIdentity(t, "fork-agent"),
				Role: "worker", ExecutionMode: "live", Subscriptions: []string{"item.received"},
			}), Topology: selectedContractTestDeclarationTopology(t)}},
			Options: SelectedContractAgentRuntimeOptions{
				ExecutionPosture: executionposture.Live,
				AgentFactory: func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
					return selectedContractSelfReleaseAgent{id: cfg.ID}, nil
				},
				AgentManagerOptions: runtimemanager.AgentManagerOptions{
					LifecycleStore: probe, WorkOwner: owner, ReceiverExecution: receiverExecution,
				},
			},
		},
	}, eventBus, &runtimepipeline.PipelineCoordinator{})
	if err != nil {
		t.Fatalf("startSelectedContractAgentRuntime: %v", err)
	}
	cancel()
	if err := runtime.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	select {
	case got := <-probe.seen:
		if got != wantScope {
			t.Fatalf("self-release scope = %#v, want %#v", got, wantScope)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for selected-contract self-release transition")
	}
}

func TestSelectedContractStaticAgentRecordsIncludeInferredFlowRequiredAgents(t *testing.T) {
	flow := runtimecontracts.FlowContractView{
		Path: "analysis",
		Paths: runtimecontracts.FlowContractPaths{
			ID:   "analysis",
			Mode: runtimecontracts.FlowModeStatic,
		},
		Schema: runtimecontracts.FlowSchemaDocument{Mode: runtimecontracts.FlowModeStatic},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"analyzer": {
				Type:           "generic",
				Role:           "analyzer",
				ResolvedIntent: selectedContractTestIntent(t, "analyzer"),
				Subscriptions:  []string{"analysis.requested"},
				EmitEvents:     []string{"analysis.done"},
			},
		},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"analysis": flow.Schema,
		},
		FlowTree: runtimecontracts.FlowTree{
			Root: &runtimecontracts.FlowContractView{
				Children: []runtimecontracts.FlowContractView{flow},
			},
			ByID: map[string]*runtimecontracts.FlowContractView{
				"analysis": &flow,
			},
		},
		URIRegistry: runtimecontracts.ContractURIRegistry{
			Agents: map[string]runtimecontracts.ContractURIRef{
				"analysis/analyzer": {
					Kind: "agent", FlowID: "analysis", LocalID: "analyzer",
					Full: "test://selected-contract/analysis/analyzer",
				},
			},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{Version: "v-test"},
	}

	records, err := selectedContractStaticAgentRecords(semanticview.Wrap(bundle))
	if err != nil {
		t.Fatalf("selectedContractStaticAgentRecords: %v", err)
	}
	count := 0
	for _, record := range records {
		if strings.TrimSpace(record.Config.ID) == "analyzer" {
			count++
		}
	}
	if count < 2 {
		t.Fatalf("records = %#v, want analyzer from static-agent and inferred flow-required-agent materialization paths", records)
	}
}

func selectedContractTestIntent(t testing.TB, agentID string) runtimeagentintent.Resolved {
	t.Helper()
	resolved, err := runtimeagentintent.Resolve(
		runtimeagentintent.SourceInline,
		"inline",
		"agents.yaml#agents."+strings.TrimSpace(agentID)+".intent",
		"Perform the selected-contract test agent's assigned work.",
	)
	if err != nil {
		t.Fatalf("resolve selected-contract test intent: %v", err)
	}
	return resolved
}

func selectedContractTestAgentConfig(t testing.TB, cfg runtimeactors.AgentConfig) runtimeactors.AgentConfig {
	t.Helper()
	if cfg.Intent.Empty() {
		cfg.Intent = selectedContractTestIntent(t, cfg.ID)
	}
	if cfg.Prompt.Empty() {
		prompt, err := runtimeagentintent.IntentOnlyPrompt(cfg.Intent)
		if err != nil {
			t.Fatalf("derive selected-contract test prompt: %v", err)
		}
		cfg.Prompt = prompt
	}
	return cfg
}
