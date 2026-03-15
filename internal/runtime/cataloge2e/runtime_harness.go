package cataloge2e

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"empireai/internal/events"
	runtime "empireai/internal/runtime"
	runtimecontracts "empireai/internal/runtime/contracts"
	runtimepipeline "empireai/internal/runtime/pipeline"
	"empireai/internal/runtime/sessions"
	"empireai/internal/store"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

type catalogTriggerStep struct {
	Event                         string         `yaml:"event"`
	Payload                       map[string]any `yaml:"payload"`
	AssertPersistedBeforeDelivery bool           `yaml:"assert_persisted_before_delivery"`
}

type catalogExpectedDocument struct {
	Trigger struct {
		Boot                          bool                 `yaml:"boot"`
		Event                         string               `yaml:"event"`
		Payload                       map[string]any       `yaml:"payload"`
		Sequence                      []catalogTriggerStep `yaml:"sequence"`
		EntityFieldsBefore            map[string]any       `yaml:"entity_fields_before"`
		AssertPersistedBeforeDelivery bool                 `yaml:"assert_persisted_before_delivery"`
	} `yaml:"trigger"`
	Expected struct {
		BootResult          string         `yaml:"boot_result"`
		HandlerOutcome      string         `yaml:"handler_outcome"`
		EntityState         string         `yaml:"entity_state"`
		EntityFields        map[string]any `yaml:"entity_fields"`
		EmittedEvents       []string       `yaml:"emitted_events"`
		DeadLetter          bool           `yaml:"dead_letter"`
		TemplateInstances   *int           `yaml:"template_instances"`
		FlowInstanceCreated map[string]any `yaml:"flow_instance_created"`
	} `yaml:"expected"`
}

func (d catalogExpectedDocument) triggerSequence() []catalogTriggerStep {
	if len(d.Trigger.Sequence) > 0 {
		return append([]catalogTriggerStep(nil), d.Trigger.Sequence...)
	}
	if strings.TrimSpace(d.Trigger.Event) == "" {
		return nil
	}
	return []catalogTriggerStep{{
		Event:                         strings.TrimSpace(d.Trigger.Event),
		Payload:                       cloneStringAnyMap(d.Trigger.Payload),
		AssertPersistedBeforeDelivery: d.Trigger.AssertPersistedBeforeDelivery,
	}}
}

type runtimeHarness struct {
	t            *testing.T
	ctx          context.Context
	cancel       context.CancelFunc
	db           *sql.DB
	pg           *store.PostgresStore
	rt           *runtime.Runtime
	workflow     *runtimepipeline.WorkflowInstanceStore
	llm          *scriptedLLMRuntime
	bundle       *runtimecontracts.WorkflowContractBundle
	initialState string
	startedAt    time.Time
	publishedIDs map[string]struct{}
}

type agentFixtureDoc struct {
	AgentFixtures map[string][]agentFixtureStep `yaml:"agent_fixtures"`
}

type agentFixtureStep struct {
	On    string             `yaml:"on"`
	Emits []agentFixtureEmit `yaml:"emits"`
}

type agentFixtureEmit struct {
	Event   string         `yaml:"event"`
	Payload map[string]any `yaml:"payload"`
}

func newRuntimeHarness(t *testing.T, fixtureRoot string, start bool) *runtimeHarness {
	t.Helper()
	bundle := loadFixtureBundle(t, fixtureRoot)
	var rootSchema struct {
		InitialState string `yaml:"initial_state"`
	}
	loadYAML(t, filepath.Join(fixtureRoot, "schema.yaml"), &rootSchema)
	module, err := newFixtureWorkflowModule(bundle)
	if err != nil {
		t.Fatalf("newFixtureWorkflowModule: %v", err)
	}

	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	cfg := testRuntimeConfig()
	cfg.LLM.RuntimeMode = "api"
	llmRuntime := newScriptedLLMRuntime()
	loadAgentFixtures(t, fixtureRoot, llmRuntime)
	pg := &store.PostgresStore{DB: db}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	rt, err := runtime.NewRuntime(ctx, cfg, runtime.Stores{
		SQLDB:             db,
		EventStore:        pg,
		SessionRegistry:   sessions.NewPostgresRegistry(db, cfg.LLM.Session.LockTTL),
		ManagerStore:      pg,
		ScheduleStore:     pg,
		MailboxStore:      pg,
		ConversationStore: nil,
		TurnStore:         nil,
	}, runtime.RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     llmRuntime,
	})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if start {
		if err := rt.Start(ctx); err != nil {
			t.Fatalf("runtime.Start: %v", err)
		}
	}
	t.Cleanup(func() { _ = rt.Shutdown() })

	return &runtimeHarness{
		t:            t,
		ctx:          ctx,
		cancel:       cancel,
		db:           db,
		pg:           pg,
		rt:           rt,
		workflow:     runtimepipeline.NewWorkflowInstanceStore(db),
		llm:          llmRuntime,
		bundle:       bundle,
		initialState: strings.TrimSpace(rootSchema.InitialState),
		startedAt:    time.Now().UTC(),
		publishedIDs: map[string]struct{}{},
	}
}

func loadAgentFixtures(t testing.TB, fixtureRoot string, llmRuntime *scriptedLLMRuntime) {
	t.Helper()
	if llmRuntime == nil {
		return
	}
	path := filepath.Join(fixtureRoot, "fixtures.yaml")
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return
		}
		t.Fatalf("stat %s: %v", path, err)
	}
	var doc agentFixtureDoc
	loadYAML(t, path, &doc)
	for agentID, steps := range doc.AgentFixtures {
		agentID = strings.TrimSpace(agentID)
		if agentID == "" {
			continue
		}
		for _, step := range steps {
			llmRuntime.SetAgentFixture(agentID, scriptedAgentFixtureStep{
				On:    strings.TrimSpace(step.On),
				Emits: append([]agentFixtureEmit(nil), step.Emits...),
			})
		}
	}
}

func (h *runtimeHarness) publishAndWait(step catalogTriggerStep, timeout time.Duration) {
	h.t.Helper()
	payload := cloneStringAnyMap(step.Payload)
	if entityID := triggerPayloadEntityID(payload); entityID != "" {
		h.seedInitialState(entityID)
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		h.t.Fatalf("marshal trigger payload: %v", err)
	}
	evt := events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType(strings.TrimSpace(step.Event)),
		SourceAgent: "cataloge2e",
		Payload:     raw,
		CreatedAt:   time.Now().UTC(),
	}
	if entityID := triggerPayloadEntityID(payload); entityID != "" {
		evt = evt.WithEntityID(entityID)
	}
	h.publishedIDs[evt.ID] = struct{}{}

	ctx, cancel := context.WithTimeout(h.ctx, timeout)
	defer cancel()
	if err := h.rt.Bus.Publish(ctx, evt); err != nil {
		h.t.Fatalf("Publish(%s): %v", strings.TrimSpace(step.Event), err)
	}
	if err := h.rt.Bus.WaitForQuiescence(ctx); err != nil {
		h.t.Fatalf("WaitForQuiescence(%s): %v", strings.TrimSpace(step.Event), err)
	}
}

func (h *runtimeHarness) seedInitialState(entityID string) {
	h.t.Helper()
	if h == nil || h.workflow == nil || h.bundle == nil {
		return
	}
	entityID = strings.TrimSpace(entityID)
	if entityID == "" {
		return
	}
	if _, ok, err := h.workflow.Load(h.ctx, entityID); err == nil && ok {
		return
	}
	initialState := strings.TrimSpace(h.initialState)
	if initialState == "" {
		return
	}
	if err := h.workflow.Upsert(h.ctx, runtimepipeline.WorkflowInstance{
		InstanceID:      entityID,
		WorkflowName:    h.bundle.WorkflowName(),
		WorkflowVersion: h.bundle.WorkflowVersion(),
		CurrentState:    initialState,
	}); err != nil {
		h.t.Fatalf("seed initial workflow state for %s: %v", entityID, err)
	}
}

func (h *runtimeHarness) seedEntityFields(expected catalogExpectedDocument) {
	h.t.Helper()
	if h == nil || h.workflow == nil {
		return
	}
	if len(expected.Trigger.EntityFieldsBefore) == 0 {
		return
	}
	entityID := ""
	for _, step := range expected.triggerSequence() {
		if entityID = triggerPayloadEntityID(step.Payload); entityID != "" {
			break
		}
	}
	entityID = strings.TrimSpace(entityID)
	if entityID == "" {
		h.t.Fatal("trigger.entity_fields_before requires a trigger payload entity_id")
	}
	h.seedInitialState(entityID)
	instance, ok, err := h.workflow.Load(h.ctx, entityID)
	if err != nil {
		h.t.Fatalf("load workflow instance for entity field seeding %s: %v", entityID, err)
	}
	if !ok {
		instance = runtimepipeline.WorkflowInstance{
			InstanceID:      entityID,
			WorkflowName:    h.bundle.WorkflowName(),
			WorkflowVersion: h.bundle.WorkflowVersion(),
			CurrentState:    h.initialState,
		}
	}
	if strings.TrimSpace(instance.InstanceID) == "" {
		instance.InstanceID = entityID
	}
	if strings.TrimSpace(instance.WorkflowName) == "" {
		instance.WorkflowName = h.bundle.WorkflowName()
	}
	if strings.TrimSpace(instance.WorkflowVersion) == "" {
		instance.WorkflowVersion = h.bundle.WorkflowVersion()
	}
	if strings.TrimSpace(instance.CurrentState) == "" {
		instance.CurrentState = h.initialState
	}
	if instance.Metadata == nil {
		instance.Metadata = map[string]any{}
	}
	for key, value := range expected.Trigger.EntityFieldsBefore {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		instance.Metadata[key] = value
	}
	if err := h.workflow.Upsert(h.ctx, instance); err != nil {
		h.t.Fatalf("seed entity_fields_before for %s: %v", entityID, err)
	}
}

func triggerPayloadEntityID(payload map[string]any) string {
	if payload == nil {
		return ""
	}
	return strings.TrimSpace(asString(payload["entity_id"]))
}

func cloneStringAnyMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func asString(v any) string {
	switch typed := v.(type) {
	case string:
		return typed
	case []byte:
		return string(typed)
	default:
		return ""
	}
}
