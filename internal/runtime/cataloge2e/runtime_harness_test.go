package cataloge2e

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtime "github.com/division-sh/swarm/internal/runtime"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

const (
	catalogRuntimeRunID          = "88888888-8888-8888-8888-888888888888"
	catalogRuntimePublishTimeout = 5 * time.Second
)

type catalogTriggerStep struct {
	Event                         string         `yaml:"event"`
	Payload                       map[string]any `yaml:"payload"`
	AssertPersistedBeforeDelivery bool           `yaml:"assert_persisted_before_delivery"`
	ErrorContains                 string         `yaml:"error_contains"`
	ReceiptOutcome                string         `yaml:"receipt_outcome"`
	ReceiptFailureClass           string         `yaml:"receipt_failure_class"`
	ReceiptFailureDetail          string         `yaml:"receipt_failure_detail"`
	ReceiptFailureAttributes      map[string]any `yaml:"receipt_failure_attributes"`
}

type catalogExpectedDocument struct {
	Trigger struct {
		Boot                          bool                 `yaml:"boot"`
		Event                         string               `yaml:"event"`
		Concurrent                    []catalogTriggerStep `yaml:"concurrent"`
		Payload                       map[string]any       `yaml:"payload"`
		Sequence                      []catalogTriggerStep `yaml:"sequence"`
		ErrorContains                 string               `yaml:"error_contains"`
		Entity                        map[string]any       `yaml:"entity"`
		EntityStateBefore             string               `yaml:"entity_state_before"`
		EntityFieldsBefore            map[string]any       `yaml:"entity_fields_before"`
		GatesBefore                   map[string]bool      `yaml:"gates_before"`
		AssertPersistedBeforeDelivery bool                 `yaml:"assert_persisted_before_delivery"`
	} `yaml:"trigger"`
	Expected struct {
		BootResult          string                           `yaml:"boot_result"`
		HandlerOutcome      string                           `yaml:"handler_outcome"`
		EntityState         string                           `yaml:"entity_state"`
		ParentState         string                           `yaml:"parent_state"`
		FlowBState          string                           `yaml:"flow_b_state"`
		FlowEntities        map[string]catalogEntityExpected `yaml:"flow_entities"`
		EntityFields        map[string]any                   `yaml:"entity_fields"`
		Gates               map[string]bool                  `yaml:"gates"`
		EmittedEvents       []string                         `yaml:"emitted_events"`
		CausalEvents        []string                         `yaml:"causal_events"`
		AgentReceived       map[string][]string              `yaml:"agent_received"`
		DeadLetter          bool                             `yaml:"dead_letter"`
		ChainDepthExceeded  bool                             `yaml:"chain_depth_exceeded"`
		TemplateInstances   *int                             `yaml:"template_instances"`
		FlowInstanceCreated map[string]any                   `yaml:"flow_instance_created"`
		Entities            map[string]catalogEntityExpected `yaml:"entities"`
	} `yaml:"expected"`
}

type catalogEntityExpected struct {
	HandlerOutcome string          `yaml:"handler_outcome"`
	Exists         *bool           `yaml:"exists"`
	EntityState    string          `yaml:"entity_state"`
	EntityFields   map[string]any  `yaml:"entity_fields"`
	Gates          map[string]bool `yaml:"gates"`
	EmittedEvents  []string        `yaml:"emitted_events"`
	CausalEvents   []string        `yaml:"causal_events"`
	DeadLetter     bool            `yaml:"dead_letter"`
}

func (d catalogExpectedDocument) triggerSequence() []catalogTriggerStep {
	if len(d.Trigger.Concurrent) > 0 {
		return nil
	}
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
		ErrorContains:                 strings.TrimSpace(d.Trigger.ErrorContains),
	}}
}

func (d catalogExpectedDocument) triggerFlowPrefix() string {
	for _, step := range d.triggerSequence() {
		eventName := strings.Trim(strings.TrimSpace(step.Event), "/")
		if eventName == "" {
			continue
		}
		lastSlash := strings.LastIndex(eventName, "/")
		if lastSlash <= 0 {
			continue
		}
		return strings.Trim(eventName[:lastSlash], "/")
	}
	return ""
}

type runtimeHarness struct {
	t              *testing.T
	ctx            context.Context
	cancel         context.CancelFunc
	db             *sql.DB
	pg             *store.PostgresStore
	rt             *runtime.Runtime
	workflow       *runtimepipeline.WorkflowInstanceStore
	llm            *scriptedLLMRuntime
	bundle         *runtimecontracts.WorkflowContractBundle
	initialState   string
	startedAt      time.Time
	publishedIDs   map[string]struct{}
	publishedOrder []string
	eventEntityIDs map[string]string
	previews       map[string]runtimepipeline.HandlerPreview
	mu             sync.Mutex
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
	runtimeCatalogHarnessStartupPolicy().apply(t)
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
	waitForCatalogHarnessDB(t, db)

	cfg := testRuntimeConfig()
	cfg.LLM.Backend = "anthropic"
	llmRuntime := newScriptedLLMRuntime()
	loadAgentFixtures(t, fixtureRoot, llmRuntime)
	pg := &store.PostgresStore{DB: db}

	ctx, cancel := context.WithCancel(runtimecorrelation.WithRunID(context.Background(), catalogRuntimeRunID))
	t.Cleanup(cancel)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status)
		VALUES ($1::uuid, 'running')
		ON CONFLICT (run_id) DO NOTHING
	`, catalogRuntimeRunID); err != nil {
		t.Fatalf("seed catalog runtime run: %v", err)
	}

	rt, err := runtime.NewRuntime(ctx, runtime.RuntimeDeps{Config: cfg, Stores: runtime.Stores{
		SQLDB:               db,
		PipelineStore:       runtimepipeline.NewWorkflowInstanceStore(db),
		EventStore:          pg,
		RuntimeLogStore:     pg,
		SessionRegistry:     sessions.NewPostgresRegistry(db, cfg.LLM.Session.LockTTL),
		ManagerStore:        pg,
		ScheduleStore:       pg,
		StartupOwnership:    pg,
		MailboxStore:        pg,
		ToolEntityStore:     pg,
		HumanTaskStore:      pg,
		RuntimeIngressStore: pg,
		ConversationStore:   nil,
		TurnStore:           nil,
	}, Options: runtime.RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
		LLMRuntime:     llmRuntime,
	}})

	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if start {
		if err := rt.Start(ctx); err != nil {
			t.Fatalf("runtime.Start: %v", err)
		}
	}
	startedAt := catalogHarnessStartBoundary(t, db)
	t.Cleanup(func() { _ = rt.Shutdown() })

	return &runtimeHarness{
		t:              t,
		ctx:            ctx,
		cancel:         cancel,
		db:             db,
		pg:             pg,
		rt:             rt,
		workflow:       runtimepipeline.NewWorkflowInstanceStore(db),
		llm:            llmRuntime,
		bundle:         bundle,
		initialState:   strings.TrimSpace(rootSchema.InitialState),
		startedAt:      startedAt,
		publishedIDs:   map[string]struct{}{},
		publishedOrder: []string{},
		eventEntityIDs: map[string]string{},
		previews:       map[string]runtimepipeline.HandlerPreview{},
	}
}

func catalogHarnessStartBoundary(t testing.TB, db *sql.DB) time.Time {
	t.Helper()
	appTime := time.Now().UTC()
	var out time.Time
	if err := db.QueryRowContext(context.Background(), `SELECT NOW()`).Scan(&out); err != nil {
		t.Fatalf("query catalog harness db time: %v", err)
	}
	dbTime := out.UTC()
	if dbTime.Before(appTime) {
		return dbTime.Add(-1 * time.Second)
	}
	return appTime.Add(-1 * time.Second)
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
	eventType := strings.TrimSpace(step.Event)
	wantErr := strings.TrimSpace(step.ErrorContains)
	if entityID := triggerPayloadEntityID(payload); entityID != "" {
		h.seedInitialState(entityID)
	}
	if wantErr != "" {
		err := h.publishRuntimeEventResult(eventType, "cataloge2e", payload, timeout, true, true)
		if err == nil {
			h.t.Fatalf("Publish(%s) unexpectedly succeeded, want error containing %q", eventType, wantErr)
		}
		if !strings.Contains(err.Error(), wantErr) {
			h.t.Fatalf("Publish(%s) error = %v, want substring %q", eventType, err, wantErr)
		}
		return
	}
	h.publishRuntimeEvent(eventType, "cataloge2e", payload, timeout, true, true)
	h.assertTriggerReceipt(step)
	if eventType == "flow.created" {
		if autoEmit := h.rootAutoEmitOnCreateEvent(); autoEmit != "" {
			h.publishRuntimeEvent(autoEmit, "flow-instance-activator", payload, timeout, true, false)
		}
	}
}

func (h *runtimeHarness) publishConcurrentAndWait(steps []catalogTriggerStep, timeout time.Duration) {
	h.t.Helper()
	if len(steps) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(h.ctx, timeout)
	defer cancel()

	type publishItem struct {
		step catalogTriggerStep
		evt  events.Event
	}
	items := make([]publishItem, 0, len(steps))
	for _, step := range steps {
		payload := cloneStringAnyMap(step.Payload)
		if entityID := triggerPayloadEntityID(payload); entityID != "" {
			h.seedInitialState(entityID)
		}
		raw, err := json.Marshal(payload)
		if err != nil {
			h.t.Fatalf("marshal concurrent trigger payload: %v", err)
		}
		eventEnvelope := events.EventEnvelope{}
		if entityID := triggerPayloadEntityID(payload); entityID != "" {
			eventEnvelope = events.EnvelopeForEntityID(eventEnvelope, entityID)
		} else {
			eventEnvelope = events.EnvelopeForEntityID(eventEnvelope, runtimepipeline.FlowInstanceEntityID(catalogRuntimeRunID))
		}
		evt := eventtest.RootIngress(uuid.NewString(),
			events.EventType(strings.TrimSpace(step.Event)),
			"cataloge2e", "", raw, 0, catalogRuntimeRunID, "", eventEnvelope, time.Now().UTC())
		if preview, ok := h.previewHandlerOutcome(evt); ok {
			h.mu.Lock()
			h.previews[evt.ID()] = preview
			h.mu.Unlock()
		}
		h.mu.Lock()
		h.publishedIDs[evt.ID()] = struct{}{}
		h.publishedOrder = append(h.publishedOrder, evt.ID())
		if entityID := triggerPayloadEntityID(payload); entityID != "" {
			h.eventEntityIDs[evt.ID()] = entityID
		}
		h.mu.Unlock()
		items = append(items, publishItem{step: step, evt: evt})
	}

	errCh := make(chan error, len(items))
	var wg sync.WaitGroup
	for _, item := range items {
		wg.Add(1)
		go func(item publishItem) {
			defer wg.Done()
			if err := h.rt.Bus.Publish(ctx, item.evt); err != nil {
				errCh <- err
			}
		}(item)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			h.t.Fatalf("concurrent publish failed: %v", err)
		}
	}
	if err := h.rt.WaitForQuiescence(ctx); err != nil {
		h.t.Fatalf("WaitForQuiescence(concurrent): %v", err)
	}
	for _, item := range items {
		h.refreshPublishedEventEntityID(item.evt.ID())
	}
}

func (h *runtimeHarness) publishRuntimeEvent(eventType, sourceAgent string, payload map[string]any, timeout time.Duration, recordOutcome bool, excludeFromEmitted bool) {
	if err := h.publishRuntimeEventResult(eventType, sourceAgent, payload, timeout, recordOutcome, excludeFromEmitted); err != nil {
		h.t.Fatalf("Publish(%s): %v", strings.TrimSpace(eventType), err)
	}
}

func (h *runtimeHarness) publishRuntimeEventResult(eventType, sourceAgent string, payload map[string]any, timeout time.Duration, recordOutcome bool, excludeFromEmitted bool) error {
	h.t.Helper()
	payload = cloneStringAnyMap(payload)
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	eventEnvelope := events.EventEnvelope{}
	if entityID := triggerPayloadEntityID(payload); entityID != "" {
		eventEnvelope = events.EnvelopeForEntityID(eventEnvelope, entityID)
	} else {
		eventEnvelope = events.EnvelopeForEntityID(eventEnvelope, runtimepipeline.FlowInstanceEntityID(catalogRuntimeRunID))
	}
	evt := eventtest.RootIngress(uuid.NewString(),
		events.EventType(strings.TrimSpace(eventType)),
		strings.TrimSpace(sourceAgent), "", raw, 0, catalogRuntimeRunID, "", eventEnvelope, time.Now().UTC())
	if recordOutcome {
		if preview, ok := h.previewHandlerOutcome(evt); ok {
			h.mu.Lock()
			h.previews[evt.ID()] = preview
			h.mu.Unlock()
		}
	}
	h.mu.Lock()
	if excludeFromEmitted {
		h.publishedIDs[evt.ID()] = struct{}{}
	}
	if recordOutcome {
		h.publishedOrder = append(h.publishedOrder, evt.ID())
		if entityID := triggerPayloadEntityID(payload); entityID != "" {
			h.eventEntityIDs[evt.ID()] = entityID
		}
	}
	h.mu.Unlock()

	ctx, cancel := context.WithTimeout(h.ctx, timeout)
	defer cancel()
	if err := h.publishBusEvent(ctx, evt); err != nil {
		return err
	}
	if err := h.rt.WaitForQuiescence(ctx); err != nil {
		return err
	}
	h.refreshPublishedEventEntityID(evt.ID())
	return nil
}

func (h *runtimeHarness) refreshPublishedEventEntityID(eventID string) {
	h.t.Helper()
	eventID = strings.TrimSpace(eventID)
	if h == nil || h.db == nil || eventID == "" {
		return
	}
	var entityID string
	err := h.db.QueryRowContext(h.ctx, `
		SELECT COALESCE(entity_id::text, '')
		FROM events
		WHERE event_id = $1::uuid
	`, eventID).Scan(&entityID)
	if err == sql.ErrNoRows {
		return
	}
	if err != nil {
		h.t.Fatalf("query published event entity_id for %s: %v", eventID, err)
	}
	entityID = strings.TrimSpace(entityID)
	if entityID == "" {
		return
	}
	h.mu.Lock()
	h.eventEntityIDs[eventID] = entityID
	h.mu.Unlock()
}

func (h *runtimeHarness) publishBusEvent(ctx context.Context, evt events.Event) error {
	h.t.Helper()
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(150 * time.Millisecond):
			}
		}
		if err := h.rt.Bus.Publish(ctx, evt); err != nil {
			lastErr = err
			if isTransientCatalogPublishError(err) {
				continue
			}
			return err
		}
		return nil
	}
	return lastErr
}

func isTransientCatalogPublishError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, sql.ErrConnDone) {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "bad connection")
}

func waitForCatalogHarnessDB(t testing.TB, db *sql.DB) {
	t.Helper()
	if db == nil {
		t.Fatal("catalog harness db is nil")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var lastErr error
	for attempt := 0; attempt < 25; attempt++ {
		lastErr = db.PingContext(ctx)
		if lastErr == nil {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("catalog harness db ping: %v", lastErr)
		case <-time.After(200 * time.Millisecond):
		}
	}
	t.Fatalf("catalog harness db ping: %v", lastErr)
}

func (h *runtimeHarness) rootAutoEmitOnCreateEvent() string {
	if h == nil || h.bundle == nil || h.bundle.RootSchema == nil {
		return ""
	}
	return strings.TrimSpace(h.bundle.RootSchema.AutoEmitOnCreate.Event)
}

func (h *runtimeHarness) waitForExpectedEmittedEvents(expected catalogExpectedDocument, timeout time.Duration) {
	h.t.Helper()
	entityID := h.expectedTriggerEntityID(expected)
	if entityID == "" || len(expected.Expected.EmittedEvents) == 0 {
		return
	}
	flowPrefix := expected.triggerFlowPrefix()
	source := semanticview.Wrap(h.bundle)
	ctx, cancel := context.WithTimeout(h.ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if h.hasExpectedEmittedEvents(ctx, entityID, expected.Expected.EmittedEvents, flowPrefix, source) {
			if err := h.rt.WaitForQuiescence(ctx); err != nil {
				h.t.Fatalf("WaitForQuiescence(after emitted events): %v", err)
			}
			return
		}
		select {
		case <-ctx.Done():
			h.t.Fatalf("wait for expected emitted events %v for entity %s: %v", expected.Expected.EmittedEvents, entityID, ctx.Err())
		case <-ticker.C:
		}
	}
}

func (h *runtimeHarness) expectedTriggerEntityID(expected catalogExpectedDocument) string {
	if h == nil {
		return ""
	}
	for _, step := range expected.triggerSequence() {
		if entityID := triggerPayloadEntityID(step.Payload); entityID != "" {
			return entityID
		}
	}
	if entityID := triggerPayloadEntityID(expected.Trigger.Payload); entityID != "" {
		return entityID
	}
	return h.firstPublishedEntityID()
}

func (h *runtimeHarness) firstPublishedEntityID() string {
	if h == nil {
		return ""
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, eventID := range h.publishedOrder {
		if entityID := strings.TrimSpace(h.eventEntityIDs[strings.TrimSpace(eventID)]); entityID != "" {
			return entityID
		}
	}
	return ""
}

func (h *runtimeHarness) hasExpectedEmittedEvents(ctx context.Context, entityID string, want []string, flowPrefix string, source semanticview.Source) bool {
	h.t.Helper()
	relevantEventIDs := catalogCausalEventIDs(h.t, h.db, h.startedAt, h.publishedIDs)
	relevantEntityIDs := catalogCausalEntityIDs(h.t, h.db, h.startedAt, h.publishedIDs, entityID)
	rows, err := h.db.QueryContext(ctx, `
		SELECT event_id::text, event_name, COALESCE(NULLIF(payload->>'entity_id', ''), COALESCE(entity_id::text, ''))
		FROM events
		WHERE created_at >= $1
		ORDER BY created_at ASC, event_id ASC
	`, h.startedAt)
	if err != nil {
		h.t.Fatalf("query emitted events for wait: %v", err)
	}
	defer rows.Close()

	counts := make(map[string]int, len(want))
	wantNames := make(map[string]struct{}, len(want))
	for _, name := range want {
		name = strings.TrimSpace(name)
		if name != "" {
			counts[name]++
			wantNames[name] = struct{}{}
		}
	}
	for rows.Next() {
		var eventID, eventName, payloadEntityID string
		if err := rows.Scan(&eventID, &eventName, &payloadEntityID); err != nil {
			h.t.Fatalf("scan emitted events for wait: %v", err)
		}
		if _, skip := h.publishedIDs[strings.TrimSpace(eventID)]; skip {
			continue
		}
		eventID = strings.TrimSpace(eventID)
		payloadEntityID = strings.TrimSpace(payloadEntityID)
		_, causalEvent := relevantEventIDs[eventID]
		_, causalEntity := relevantEntityIDs[payloadEntityID]
		if !causalEvent && !causalEntity {
			continue
		}
		eventName = strings.TrimSpace(eventName)
		if eventName == "" || shouldIgnoreCatalogE2EEvent(eventName) {
			continue
		}
		eventName = normalizeCatalogObservedEventName(eventName, flowPrefix, source, wantNames)
		if flowPrefix == "" && strings.Contains(eventName, "/") {
			continue
		}
		if _, ok := counts[eventName]; !ok {
			continue
		}
		counts[eventName]--
	}
	if err := rows.Err(); err != nil {
		h.t.Fatalf("iterate emitted events for wait: %v", err)
	}
	for name, remaining := range counts {
		if remaining > 0 {
			_ = name
			return false
		}
	}
	return true
}

func (h *runtimeHarness) previewHandlerOutcome(evt events.Event) (runtimepipeline.HandlerPreview, bool) {
	if h == nil || h.bundle == nil {
		return runtimepipeline.HandlerPreview{}, false
	}
	nodeID := firstMatchingNodeHandler(h.bundle, strings.TrimSpace(string(evt.Type())))
	if nodeID == "" {
		return runtimepipeline.HandlerPreview{}, false
	}
	entityID := strings.TrimSpace(evt.EntityID())
	state := runtimepipeline.WorkflowState{
		EntityID: entityID,
	}
	if strings.TrimSpace(entityID) != "" && h.workflow != nil {
		if instance, ok, err := h.workflow.Load(h.ctx, entityID); err == nil && ok {
			state.Stage = runtimepipeline.NormalizeWorkflowStateID(instance.CurrentState)
			state.Metadata = cloneStringAnyMap(instance.Metadata)
		}
	}
	preview, err := runtimepipeline.PreviewContractHandlerExecution(h.ctx, h.bundle, nodeID, evt, state, nil)
	if err != nil {
		return runtimepipeline.HandlerPreview{}, false
	}
	return preview, true
}

func firstMatchingNodeHandler(bundle *runtimecontracts.WorkflowContractBundle, eventType string) string {
	if bundle == nil || strings.TrimSpace(eventType) == "" {
		return ""
	}
	for nodeID := range bundle.Nodes {
		if _, ok := bundle.NodeEventHandler(nodeID, eventType); ok {
			return strings.TrimSpace(nodeID)
		}
	}
	return ""
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
	if len(expected.Trigger.EntityFieldsBefore) == 0 && len(expected.Trigger.Entity) == 0 && len(expected.Trigger.GatesBefore) == 0 && strings.TrimSpace(expected.Trigger.EntityStateBefore) == "" {
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
		entityID = runtimepipeline.FlowInstanceEntityID(catalogRuntimeRunID)
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
	if seededState := strings.TrimSpace(expected.Trigger.EntityStateBefore); seededState != "" {
		instance.CurrentState = seededState
	}
	if instance.Metadata == nil {
		instance.Metadata = map[string]any{}
	}
	for key, value := range expected.Trigger.Entity {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		instance.Metadata[key] = value
	}
	for key, value := range expected.Trigger.EntityFieldsBefore {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		instance.Metadata[key] = value
	}
	if len(expected.Trigger.GatesBefore) > 0 {
		gates, _ := instance.Metadata["gates"].(map[string]any)
		if gates == nil {
			gates = map[string]any{}
		}
		for key, value := range expected.Trigger.GatesBefore {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			gates[key] = value
		}
		instance.Metadata["gates"] = gates
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

func boolFromAny(v any) bool {
	switch typed := v.(type) {
	case bool:
		return typed
	case string:
		switch strings.ToLower(strings.TrimSpace(typed)) {
		case "1", "true", "yes", "on":
			return true
		default:
			return false
		}
	default:
		return false
	}
}
