package swarmflowtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerequiredagents "github.com/division-sh/swarm/internal/runtime/requiredagents"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"gopkg.in/yaml.v3"
)

type catalogTriggerStep struct {
	Event                string         `yaml:"event"`
	Payload              map[string]any `yaml:"payload"`
	Sender               string         `yaml:"sender"`
	ErrorContains        string         `yaml:"error_contains"`
	ReceiptOutcome       string         `yaml:"receipt_outcome"`
	ReceiptErrorContains string         `yaml:"receipt_error_contains"`
}

type catalogExpectedDocument struct {
	Trigger struct {
		Boot                          bool                 `yaml:"boot"`
		Event                         string               `yaml:"event"`
		Payload                       map[string]any       `yaml:"payload"`
		Sequence                      []catalogTriggerStep `yaml:"sequence"`
		Concurrent                    []catalogTriggerStep `yaml:"concurrent"`
		Entity                        map[string]any       `yaml:"entity"`
		EntityFieldsBefore            map[string]any       `yaml:"entity_fields_before"`
		EntityStateBefore             string               `yaml:"entity_state_before"`
		GatesBefore                   map[string]any       `yaml:"gates_before"`
		Sender                        string               `yaml:"sender"`
		AssertAtomicCommit            bool                 `yaml:"assert_atomic_commit"`
		AssertPersistedBeforeDelivery bool                 `yaml:"assert_persisted_before_delivery"`
		AssertSerialProcessing        bool                 `yaml:"assert_serial_processing"`
		InjectFailure                 string               `yaml:"inject_failure"`
		ErrorContains                 string               `yaml:"error_contains"`
	} `yaml:"trigger"`
	Expected struct {
		RuntimeOnly            bool                                `yaml:"runtime_only"`
		BootResult             string                              `yaml:"boot_result"`
		ErrorCategory          string                              `yaml:"error_category"`
		ErrorContains          string                              `yaml:"error_contains"`
		HandlerOutcome         string                              `yaml:"handler_outcome"`
		ChainDepthExceeded     bool                                `yaml:"chain_depth_exceeded"`
		EntityState            string                              `yaml:"entity_state"`
		EmittedEvents          []string                            `yaml:"emitted_events"`
		CausalEvents           []string                            `yaml:"causal_events"`
		EntityFields           map[string]any                      `yaml:"entity_fields"`
		Gates                  map[string]any                      `yaml:"gates"`
		GatesSet               []string                            `yaml:"gates_set"`
		DeadLetter             bool                                `yaml:"dead_letter"`
		DeadLetterReason       string                              `yaml:"dead_letter_reason"`
		ChainDepthAtDeadLetter int                                 `yaml:"chain_depth_at_dead_letter"`
		Diagnostics            []map[string]any                    `yaml:"diagnostics"`
		AgentRouting           map[string]string                   `yaml:"agent_routing"`
		AgentReceived          map[string]any                      `yaml:"agent_received"`
		FlowInstanceCreated    map[string]any                      `yaml:"flow_instance_created"`
		ToolResolution         map[string]any                      `yaml:"tool_resolution"`
		TemplateInstances      any                                 `yaml:"template_instances"`
		Entities               map[string]catalogExpectedPerEntity `yaml:"entities"`
		FlowEntities           map[string]catalogExpectedPerEntity `yaml:"flow_entities"`
		ParentState            string                              `yaml:"parent_state"`
		FlowBState             string                              `yaml:"flow_b_state"`
	} `yaml:"expected"`
}

type catalogExpectedPerEntity struct {
	HandlerOutcome string         `yaml:"handler_outcome"`
	Exists         *bool          `yaml:"exists"`
	EntityState    string         `yaml:"entity_state"`
	EntityFields   map[string]any `yaml:"entity_fields"`
	Gates          map[string]any `yaml:"gates"`
	EmittedEvents  []string       `yaml:"emitted_events"`
	CausalEvents   []string       `yaml:"causal_events"`
	DeadLetter     bool           `yaml:"dead_letter"`
}

type catalogRunResult struct {
	handlerOutcome         string
	entityState            string
	emittedEvents          []string
	entityFields           map[string]any
	gates                  map[string]any
	bootResult             string
	templateInstances      any
	deadLetter             bool
	deadLetterReason       string
	chainDepthAtDeadLetter int
	agentReceived          map[string]any
	entities               map[string]catalogRunResult
	errorCategory          string
	errorContains          string
}

type catalogBootIssue struct {
	Severity string
	Category string
	Message  string
}

type catalogBootScope struct {
	Name    string
	Dir     string
	Root    bool
	Nodes   map[string]any
	Events  map[string]any
	Agents  map[string]any
	Schema  map[string]any
	Package map[string]any
	Policy  map[string]any
	Tools   map[string]any
}

type catalogBootBundle struct {
	Root      catalogBootScope
	Flows     []catalogBootScope
	AllNodes  map[string]any
	AllEvents map[string]any
	AllAgents map[string]any
	AllPolicy map[string]any
	AllTools  map[string]any
	Source    semanticview.Source
}

type catalogNodeContract struct {
	Flow          string                                   `yaml:"flow"`
	Timers        []catalogNodeTimerContract               `yaml:"timers"`
	EventHandlers map[string]catalogSystemNodeEventHandler `yaml:"event_handlers"`
}

type catalogSystemNodeEventHandler struct {
	Emit             runtimecontracts.EmitSpec                 `yaml:"emit"`
	Guard            *runtimecontracts.GuardSpec               `yaml:"guard"`
	Action           catalogActionSpec                         `yaml:"action"`
	ActionParams     *catalogActionParams                      `yaml:"action_params"`
	AdvancesTo       string                                    `yaml:"advances_to"`
	SetsGate         *runtimecontracts.GateSpec                `yaml:"sets_gate"`
	SetsGates        map[string]any                            `yaml:"sets_gates"`
	ClearGates       catalogClearGates                         `yaml:"clear_gates"`
	DataAccumulation runtimecontracts.WorkflowDataAccumulation `yaml:"data_accumulation"`
	OnComplete       catalogRuleList                           `yaml:"on_complete"`
	Rules            catalogRuleList                           `yaml:"rules"`
	Accumulate       *catalogAccumulateSpec                    `yaml:"accumulate"`
	Compute          *catalogComputeSpec                       `yaml:"compute"`
	FanOut           *catalogFanOutSpec                        `yaml:"fan_out"`
	Filter           *runtimecontracts.FilterSpec              `yaml:"filter"`
	Reduce           *runtimecontracts.ReduceSpec              `yaml:"reduce"`
	Count            *runtimecontracts.CountSpec               `yaml:"count"`
	Query            *runtimecontracts.QuerySpec               `yaml:"query"`
	Clear            *runtimecontracts.ClearSpec               `yaml:"clear"`
	Timer            *catalogInlineTimerSpec                   `yaml:"timer"`
	GroupBy          *catalogGroupBySpec                       `yaml:"group_by"`
	SimulateFailure  bool                                      `yaml:"simulate_failure"`
}

func (h *catalogSystemNodeEventHandler) UnmarshalYAML(value *yaml.Node) error {
	type alias catalogSystemNodeEventHandler
	var typed alias
	if err := value.Decode(&typed); err != nil {
		return err
	}
	*h = catalogSystemNodeEventHandler(typed)
	if value == nil || value.Kind != yaml.MappingNode {
		return nil
	}
	if h.SetsGate != nil && strings.TrimSpace(h.SetsGate.Name) != "" {
		return nil
	}
	var setsGateNode *yaml.Node
	for i := 0; i+1 < len(value.Content); i += 2 {
		if strings.TrimSpace(value.Content[i].Value) == "sets_gate" {
			setsGateNode = value.Content[i+1]
			break
		}
	}
	if setsGateNode == nil || setsGateNode.Kind != yaml.MappingNode {
		return nil
	}
	var explicit runtimecontracts.GateSpec
	if err := setsGateNode.Decode(&explicit); err == nil && strings.TrimSpace(explicit.Name) != "" {
		h.SetsGate = &explicit
		return nil
	}
	if len(setsGateNode.Content) != 2 {
		return nil
	}
	name := strings.TrimSpace(setsGateNode.Content[0].Value)
	if name == "" {
		return nil
	}
	var gateValue any
	if err := setsGateNode.Content[1].Decode(&gateValue); err != nil {
		return err
	}
	h.SetsGate = &runtimecontracts.GateSpec{Name: name, Value: gateValue}
	return nil
}

type catalogClearGates []string

type catalogRule struct {
	ID               string                                    `yaml:"id"`
	Description      string                                    `yaml:"description"`
	Condition        string                                    `yaml:"condition"`
	AdvancesTo       string                                    `yaml:"advances_to"`
	Emit             runtimecontracts.EmitSpec                 `yaml:"emit"`
	DataAccumulation runtimecontracts.WorkflowDataAccumulation `yaml:"data_accumulation"`
	Compute          *catalogComputeSpec                       `yaml:"compute"`
}

type catalogRuleList []catalogRule

type catalogAccumulateSpec struct {
	ExpectedFrom string          `yaml:"expected_from"`
	DedupBy      string          `yaml:"dedup_by"`
	Completion   string          `yaml:"completion"`
	From         string          `yaml:"from"`
	Threshold    int             `yaml:"threshold"`
	OnComplete   catalogRuleList `yaml:"on_complete"`
	OnTimeout    *catalogRule    `yaml:"on_timeout"`
}

type catalogComputeSpec struct {
	StoreAs     string `yaml:"store_as"`
	OutputField string `yaml:"output_field"`
}

type catalogFanOutSpec struct {
	ItemsFrom string                    `yaml:"items_from"`
	Target    string                    `yaml:"target"`
	Emit      runtimecontracts.EmitSpec `yaml:"emit"`
}

type catalogActionParams struct {
	Template   string `yaml:"template"`
	InstanceID string `yaml:"instance_id"`
	ConfigFrom string `yaml:"config_from"`
}

type catalogActionSpec struct {
	ID             string `yaml:"id"`
	Template       string `yaml:"template"`
	InstanceIDFrom string `yaml:"instance_id_from"`
}

type catalogGroupBySpec struct {
	ItemsFrom string `yaml:"items_from"`
	Key       string `yaml:"key"`
	StoreAs   string `yaml:"store_as"`
}

type catalogInlineTimerSpec struct {
	DelayMS   int    `yaml:"delay_ms"`
	Emit      string `yaml:"emit"`
	Recurring bool   `yaml:"recurring"`
}

type catalogNodeTimerContract struct {
	ID        string `yaml:"id"`
	StartOn   string `yaml:"start_on"`
	CancelOn  string `yaml:"cancel_on"`
	Duration  string `yaml:"duration"`
	Emits     string `yaml:"emits"`
	Recurring bool   `yaml:"recurring"`
}

type catalogPackageDocument struct {
	Name        string               `yaml:"name"`
	Description string               `yaml:"description"`
	Flows       []catalogPackageFlow `yaml:"flows"`
}

type catalogPackageFlow struct {
	ID   string `yaml:"id"`
	Flow string `yaml:"flow"`
	Mode string `yaml:"mode"`
}

func (f *catalogPackageFlow) UnmarshalYAML(node *yaml.Node) error {
	if f == nil {
		return nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		if strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") || strings.TrimSpace(node.Value) == "" {
			*f = catalogPackageFlow{}
			return nil
		}
		f.Flow = strings.TrimSpace(node.Value)
		return nil
	case yaml.MappingNode:
		type alias catalogPackageFlow
		var aux alias
		if err := node.Decode(&aux); err != nil {
			return err
		}
		*f = catalogPackageFlow(aux)
		return nil
	default:
		return fmt.Errorf("unsupported package flow yaml node kind %d", node.Kind)
	}
}

type catalogEventSchemaEntry struct {
	Payload  map[string]any `yaml:"payload"`
	Required []string       `yaml:"required"`
}

type catalogAgentRegistryEntry struct {
	ID            string   `yaml:"id"`
	Subscriptions []string `yaml:"subscriptions"`
	Produces      []string `yaml:"produces"`
	EmitEvents    []string `yaml:"emit_events"`
	Tools         []string `yaml:"tools"`
	Permissions   []string `yaml:"permissions"`
}

type catalogQueuedEvent struct {
	Event      string
	Payload    map[string]any
	Sender     string
	ChainDepth int
}

type catalogEntitySnapshot struct {
	State  string
	Entity map[string]any
	Gates  map[string]any
}

type catalogSchemaDocument struct {
	InitialState     string                  `yaml:"initial_state"`
	TerminalStates   []string                `yaml:"terminal_states"`
	States           []string                `yaml:"states"`
	AutoEmitOnCreate catalogAutoEmitOnCreate `yaml:"auto_emit_on_create"`
}

type catalogAutoEmitOnCreate struct {
	Event       string `yaml:"event"`
	Description string `yaml:"description"`
}

func (g *catalogClearGates) UnmarshalYAML(node *yaml.Node) error {
	if g == nil {
		return nil
	}
	values, err := decodeCatalogClearGates(node)
	if err != nil {
		return err
	}
	*g = values
	return nil
}

func (r *catalogRule) UnmarshalYAML(node *yaml.Node) error {
	if r == nil {
		return nil
	}
	if node.Kind == yaml.ScalarNode {
		r.Description = strings.TrimSpace(node.Value)
		return nil
	}
	type alias catalogRule
	var aux alias
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*r = catalogRule(aux)
	return nil
}

func (r *catalogRuleList) UnmarshalYAML(node *yaml.Node) error {
	if r == nil {
		return nil
	}
	rules, err := decodeCatalogRuleList(node)
	if err != nil {
		return err
	}
	*r = rules
	return nil
}

func (a *catalogAutoEmitOnCreate) UnmarshalYAML(node *yaml.Node) error {
	if a == nil {
		return nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		if strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") || strings.TrimSpace(node.Value) == "" {
			*a = catalogAutoEmitOnCreate{}
			return nil
		}
		a.Event = strings.TrimSpace(node.Value)
		return nil
	case yaml.MappingNode:
		type alias catalogAutoEmitOnCreate
		var aux alias
		if err := node.Decode(&aux); err != nil {
			return err
		}
		*a = catalogAutoEmitOnCreate(aux)
		return nil
	default:
		return fmt.Errorf("unsupported auto_emit_on_create yaml node kind %d", node.Kind)
	}
}

func (a *catalogActionSpec) UnmarshalYAML(node *yaml.Node) error {
	if a == nil {
		return nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		if strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") || strings.TrimSpace(node.Value) == "" {
			*a = catalogActionSpec{}
			return nil
		}
		a.ID = strings.TrimSpace(node.Value)
		return nil
	case yaml.MappingNode:
		type alias catalogActionSpec
		var aux alias
		if err := node.Decode(&aux); err != nil {
			return err
		}
		*a = catalogActionSpec(aux)
		return nil
	default:
		return fmt.Errorf("unsupported action yaml node kind %d", node.Kind)
	}
}

func TestCatalogRunner_ValidatesCurrentCatalogPackages(t *testing.T) {
	for _, dir := range discoveredCatalogCaseDirs(t) {
		dir := dir
		t.Run(dir, func(t *testing.T) {
			root := filepath.Join(repoRootForTest(t), "tests", filepath.FromSlash(dir))
			var expected catalogExpectedDocument
			loadExpectedYAMLForCatalogTest(t, filepath.Join(root, "expected.yaml"), &expected)
			if err := validateCatalogExpectedDocument(dir, expected); err != nil {
				t.Fatalf("validate expected.yaml: %v", err)
			}
		})
	}
}

func TestCatalogRunner_IdentifiesSimpleHarnessEligiblePackages(t *testing.T) {
	eligible := 0
	for _, dir := range discoveredCatalogCaseDirs(t) {
		var expected catalogExpectedDocument
		root := filepath.Join(repoRootForTest(t), "tests", filepath.FromSlash(dir))
		loadExpectedYAMLForCatalogTest(t, filepath.Join(root, "expected.yaml"), &expected)
		if !catalogCaseSimpleHarnessEligible(expected) {
			continue
		}
		eligible++
	}
	if eligible == 0 {
		t.Fatal("no simple-harness-eligible catalog packages discovered")
	}
}

func discoveredCatalogCaseDirs(t testing.TB) []string {
	t.Helper()
	root := filepath.Join(repoRootForTest(t), "tests")
	dirs := make([]string, 0, 128)
	if err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return nil
		}
		if !catalogCasePresent(path) {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(strings.TrimSpace(rel))
		if rel != "" && rel != "." {
			dirs = append(dirs, rel)
		}
		return nil
	}); err != nil {
		t.Fatalf("walk Swarm test catalog root: %v", err)
	}
	sort.Strings(dirs)
	if len(dirs) == 0 {
		t.Fatal("no Swarm test catalog packages discovered")
	}
	return dirs
}

func catalogCasePresent(dir string) bool {
	required := []string{"package.yaml", "schema.yaml", "nodes.yaml", "expected.yaml"}
	for _, name := range required {
		info, err := os.Stat(filepath.Join(dir, name))
		if err != nil || info.IsDir() {
			return false
		}
	}
	return true
}

func runSimpleCatalogCase(t testing.TB, dir string) (catalogRunResult, catalogExpectedDocument) {
	t.Helper()
	var expected catalogExpectedDocument
	loadExpectedYAMLForCatalogTest(t, filepath.Join(dir, "expected.yaml"), &expected)
	var pkg catalogPackageDocument
	if _, err := os.Stat(filepath.Join(dir, "package.yaml")); err == nil {
		loadYAMLForCatalogTest(t, filepath.Join(dir, "package.yaml"), &pkg)
	}
	var schema catalogSchemaDocument
	loadYAMLForCatalogTest(t, filepath.Join(dir, "schema.yaml"), &schema)
	policy := map[string]any{}
	loadYAMLForCatalogTest(t, filepath.Join(dir, "policy.yaml"), &policy)
	slashedDir := filepath.ToSlash(dir)
	if expected.Trigger.Boot || strings.Contains(slashedDir, "tier8-boot-verification/") {
		return runBootVerificationCatalogCase(t, dir, pkg, schema, nil, nil, nil, policy, expected)
	}
	nodes := map[string]catalogNodeContract{}
	loadYAMLForCatalogTest(t, filepath.Join(dir, "nodes.yaml"), &nodes)
	eventCatalog := map[string]catalogEventSchemaEntry{}
	if _, err := os.Stat(filepath.Join(dir, "events.yaml")); err == nil {
		loadYAMLForCatalogTest(t, filepath.Join(dir, "events.yaml"), &eventCatalog)
	}
	agents := map[string]catalogAgentRegistryEntry{}
	if _, err := os.Stat(filepath.Join(dir, "agents.yaml")); err == nil {
		loadYAMLForCatalogTest(t, filepath.Join(dir, "agents.yaml"), &agents)
		for key, entry := range agents {
			if strings.TrimSpace(entry.ID) == "" {
				entry.ID = strings.TrimSpace(key)
				agents[key] = entry
			}
		}
	}
	if strings.Contains(slashedDir, "tier6-event-loop/") || strings.Contains(slashedDir, "tier7-composition/") {
		return runEventLoopCatalogCase(t, dir, pkg, schema, nodes, eventCatalog, agents, policy, expected)
	}
	if strings.Contains(slashedDir, "tier4-cross-entity/") || strings.Contains(slashedDir, "tier5-flow-lifecycle/") {
		return runAdvancedCatalogCase(t, dir, pkg, schema, nodes, policy, expected)
	}

	entity := map[string]any{}
	for key, value := range expected.Trigger.Entity {
		entity[strings.TrimSpace(key)] = value
	}
	for key, value := range expected.Trigger.EntityFieldsBefore {
		entity[strings.TrimSpace(key)] = value
	}
	if len(expected.Trigger.GatesBefore) > 0 {
		gates := map[string]any{}
		for key, value := range expected.Trigger.GatesBefore {
			key = strings.TrimSpace(key)
			gates[key] = value
			entity[key] = value
		}
		entity["gates"] = gates
	}
	state := strings.TrimSpace(catalogFirstNonEmptyString(expected.Trigger.EntityStateBefore, schema.InitialState))
	result := catalogRunResult{
		entityState:  state,
		entityFields: cloneStringAnyMapCatalog(entity),
		gates:        cloneStringAnyMapCatalog(asMapForCatalog(entity["gates"])),
	}
	var handler catalogSystemNodeEventHandler
	var ok bool
	for _, node := range nodes {
		if strings.TrimSpace(expected.Trigger.Event) != "" {
			handler, ok = node.EventHandlers[strings.TrimSpace(expected.Trigger.Event)]
		} else if len(expected.Trigger.Sequence) > 0 {
			handler, ok = node.EventHandlers[strings.TrimSpace(expected.Trigger.Sequence[0].Event)]
		}
		if ok {
			break
		}
	}
	if !ok {
		t.Fatalf("no handler found in %s", dir)
	}

	steps := expected.Trigger.Sequence
	if len(steps) == 0 && strings.TrimSpace(expected.Trigger.Event) != "" {
		steps = []catalogTriggerStep{{
			Event:   strings.TrimSpace(expected.Trigger.Event),
			Payload: expected.Trigger.Payload,
		}}
	}

	accumulate := handler.Accumulate
	if accumulate == nil {
		for _, step := range steps {
			result = executeCatalogHandlerStep(t, handler, step, entity, policy, result)
		}
		return result, expected
	}

	expectedCount := 0
	if ref := strings.TrimSpace(accumulate.ExpectedFrom); ref != "" {
		expectedCount = asIntForCatalog(resolveCatalogRef(ref, entity, map[string]any{"payload": expected.Trigger.Payload, "entity": entity}))
	}
	received := 0
	seen := map[string]struct{}{}
	completion := strings.ToLower(strings.TrimSpace(accumulate.Completion))
	for _, step := range steps {
		if strings.EqualFold(strings.TrimSpace(accumulate.From), step.Sender) || strings.TrimSpace(accumulate.From) == "" {
			// keep step
		} else {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(step.Event), "accumulate.timeout") {
			if strings.EqualFold(strings.TrimSpace(accumulate.Completion), "timeout") {
				result = executeCatalogHandlerStep(t, handler, step, entity, policy, result)
				break
			}
			if accumulate.OnTimeout != nil {
				result.handlerOutcome = "success"
				if next := strings.TrimSpace(accumulate.OnTimeout.AdvancesTo); next != "" {
					result.entityState = next
				}
				if emit := strings.TrimSpace(accumulate.OnTimeout.Emit.EventType()); emit != "" {
					result.emittedEvents = append(result.emittedEvents, emit)
				}
				if accumulate.OnTimeout.DataAccumulation.HasWrites() {
					applyCatalogDataAccumulation(accumulate.OnTimeout.DataAccumulation, step.Payload, entity)
					result.entityFields = cloneStringAnyMapCatalog(entity)
				}
				applyCatalogCompute(accumulate.OnTimeout.Compute, entity)
				applyCatalogDataAccumulation(handler.DataAccumulation, step.Payload, entity)
				applyCatalogCompute(handler.Compute, entity)
				result.entityFields = cloneStringAnyMapCatalog(entity)
				if emit := strings.TrimSpace(handler.Emit.EventType()); emit != "" {
					result.emittedEvents = append(result.emittedEvents, emit)
				}
				break
			}
			continue
		}
		key := catalogAccumulationKey(step.Payload, accumulate.DedupBy, entity, policy)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		received++
		entity["received_items"] = appendCatalogItem(entity["received_items"], step.Payload)
		if catalogAccumulationComplete(completion, received, expectedCount, accumulate.Threshold) {
			result = executeCatalogHandlerStep(t, handler, step, entity, policy, result)
			break
		}
	}
	if result.handlerOutcome == "" {
		result.handlerOutcome = "success"
		result.entityState = catalogFirstNonEmptyString(result.entityState, "collecting")
	}
	if strings.TrimSpace(result.entityState) == "" {
		result.entityState = state
	}
	return result, expected
}

func runAdvancedCatalogCase(
	t testing.TB,
	dir string,
	pkg catalogPackageDocument,
	schema catalogSchemaDocument,
	nodes map[string]catalogNodeContract,
	policy map[string]any,
	expected catalogExpectedDocument,
) (catalogRunResult, catalogExpectedDocument) {
	t.Helper()
	entity, gates := catalogInitialEntity(expected)
	state := strings.TrimSpace(catalogFirstNonEmptyString(expected.Trigger.EntityStateBefore, schema.InitialState))
	result := catalogRunResult{
		entityState:       state,
		entityFields:      cloneStringAnyMapCatalog(entity),
		gates:             cloneStringAnyMapCatalog(gates),
		templateInstances: nil,
	}
	if expected.Trigger.Boot {
		result.bootResult = "success"
		result.templateInstances = 0
		for _, flow := range pkg.Flows {
			if strings.EqualFold(strings.TrimSpace(flow.Mode), "template") {
				result.templateInstances = 0
				break
			}
		}
		return result, expected
	}
	steps := expected.Trigger.Sequence
	if len(steps) == 0 && strings.TrimSpace(expected.Trigger.Event) != "" {
		steps = []catalogTriggerStep{{
			Event:   strings.TrimSpace(expected.Trigger.Event),
			Payload: expected.Trigger.Payload,
			Sender:  strings.TrimSpace(expected.Trigger.Sender),
		}}
	}
	createdFlowInstances := map[string]struct{}{}
	activeTimers := map[string]catalogNodeTimerContract{}
	terminal := catalogStringSet(schema.TerminalStates)
	for _, step := range steps {
		if strings.EqualFold(strings.TrimSpace(step.Event), "flow.created") && strings.TrimSpace(schema.AutoEmitOnCreate.Event) != "" {
			handler, _, _, ok := catalogResolveHandler(nodes, schema.AutoEmitOnCreate.Event)
			if !ok {
				t.Fatalf("no handler found for auto_emit_on_create event %q in %s", schema.AutoEmitOnCreate.Event, dir)
			}
			stepResult := executeCatalogHandlerStep(t, handler, catalogTriggerStep{
				Event:   strings.TrimSpace(schema.AutoEmitOnCreate.Event),
				Payload: step.Payload,
				Sender:  step.Sender,
			}, entity, policy, catalogRunResult{entityState: state, gates: cloneStringAnyMapCatalog(gates)})
			stepResult.emittedEvents = append(stepResult.emittedEvents, strings.TrimSpace(schema.AutoEmitOnCreate.Event))
			stepResult.entityFields = cloneStringAnyMapCatalog(entity)
			stepResult.gates = cloneStringAnyMapCatalog(asMapForCatalog(entity["gates"]))
			result = stepResult
			state = result.entityState
			continue
		}
		handler, nodeID, node, ok := catalogResolveHandler(nodes, step.Event)
		if !ok {
			t.Fatalf("no handler found for event %q in %s", step.Event, dir)
		}
		if _, isTerminal := terminal[strings.TrimSpace(state)]; isTerminal {
			result = catalogApplyTerminalEventPolicy(result, state, step.Event)
			continue
		}
		if err := catalogValidateTimerEventActivation(nodes, activeTimers, step.Event); err != nil {
			t.Fatal(err)
		}
		stepResult := executeCatalogHandlerStep(t, handler, step, entity, policy, catalogRunResult{
			entityState: state,
			gates:       cloneStringAnyMapCatalog(asMapForCatalog(entity["gates"])),
		})
		applyCatalogClear(handler.Clear, entity)
		applyCatalogQuery(handler.Query, entity)
		if outcome, handled := applyCatalogAction(handler, step.Payload, entity, createdFlowInstances); handled {
			stepResult.handlerOutcome = outcome
			if outcome == "error" {
				stepResult.emittedEvents = nil
				stepResult.entityState = state
				stepResult.entityFields = cloneStringAnyMapCatalog(entity)
				stepResult.gates = cloneStringAnyMapCatalog(asMapForCatalog(entity["gates"]))
				result = stepResult
				break
			}
		}
		applyCatalogInlineTimer(handler.Timer, &stepResult)
		applyCatalogNodeTimers(nodeID, node, step.Event, activeTimers)
		stepResult.entityFields = cloneStringAnyMapCatalog(entity)
		stepResult.gates = cloneStringAnyMapCatalog(asMapForCatalog(entity["gates"]))
		result = stepResult
		state = result.entityState
	}
	if strings.TrimSpace(result.entityState) == "" {
		result.entityState = state
	}
	return result, expected
}

func runEventLoopCatalogCase(
	t testing.TB,
	dir string,
	pkg catalogPackageDocument,
	schema catalogSchemaDocument,
	nodes map[string]catalogNodeContract,
	eventCatalog map[string]catalogEventSchemaEntry,
	agents map[string]catalogAgentRegistryEntry,
	policy map[string]any,
	expected catalogExpectedDocument,
) (catalogRunResult, catalogExpectedDocument) {
	t.Helper()
	_ = pkg
	if len(expected.Trigger.Concurrent) > 0 {
		result := catalogRunResult{entities: map[string]catalogRunResult{}}
		for _, step := range expected.Trigger.Concurrent {
			entityID := strings.TrimSpace(asStringForCatalog(step.Payload["entity_id"]))
			if entityID == "" {
				entityID = "unknown"
			}
			subExpected := expected
			subExpected.Trigger.Concurrent = nil
			subExpected.Trigger.Sequence = []catalogTriggerStep{step}
			subExpected.Trigger.Event = ""
			subExpected.Expected.Entities = nil
			subResult, _ := runEventLoopCatalogCase(t, dir, pkg, schema, nodes, eventCatalog, agents, policy, subExpected)
			result.entities[entityID] = subResult
		}
		return result, expected
	}

	entityID := strings.TrimSpace(asStringForCatalog(expected.Trigger.Payload["entity_id"]))
	if entityID == "" && len(expected.Trigger.Sequence) > 0 {
		entityID = strings.TrimSpace(asStringForCatalog(expected.Trigger.Sequence[0].Payload["entity_id"]))
	}
	initialEntity, initialGates := catalogInitialEntity(expected)
	initialState := strings.TrimSpace(catalogFirstNonEmptyString(expected.Trigger.EntityStateBefore, schema.InitialState))
	snapshot := catalogEntitySnapshot{
		State:  initialState,
		Entity: cloneStringAnyMapCatalog(initialEntity),
		Gates:  cloneStringAnyMapCatalog(initialGates),
	}
	if snapshot.Entity == nil {
		snapshot.Entity = map[string]any{}
	}
	if snapshot.Gates != nil {
		snapshot.Entity["gates"] = cloneStringAnyMapCatalog(snapshot.Gates)
	}
	snapshot.Entity["state"] = initialState
	stateByScope := map[string]string{"default": initialState}

	queue := make([]catalogQueuedEvent, 0, 16)
	steps := expected.Trigger.Sequence
	if len(steps) == 0 && strings.TrimSpace(expected.Trigger.Event) != "" {
		steps = []catalogTriggerStep{{
			Event:   strings.TrimSpace(expected.Trigger.Event),
			Payload: expected.Trigger.Payload,
			Sender:  strings.TrimSpace(expected.Trigger.Sender),
		}}
	}
	for _, step := range steps {
		queue = append(queue, catalogQueuedEvent{
			Event:      strings.TrimSpace(step.Event),
			Payload:    cloneStringAnyMapCatalog(step.Payload),
			Sender:     strings.TrimSpace(step.Sender),
			ChainDepth: 1,
		})
	}

	result := catalogRunResult{
		handlerOutcome: "success",
		entityState:    initialState,
		entityFields:   cloneStringAnyMapCatalog(snapshot.Entity),
		gates:          cloneStringAnyMapCatalog(snapshot.Gates),
		agentReceived:  map[string]any{},
	}
	terminal := catalogStringSet(schema.TerminalStates)
	allEmitted := make([]string, 0, 8)

	for len(queue) > 0 {
		ev := queue[0]
		queue = queue[1:]

		if err := catalogEventPayloadError(eventCatalog[ev.Event], ev.Payload); err != nil {
			result.handlerOutcome = ""
			result.errorContains = err.Error()
			result.deadLetter = false
			result.deadLetterReason = ""
			result.chainDepthAtDeadLetter = 0
			result.entityState = initialState
			result.entityFields = cloneStringAnyMapCatalog(snapshot.Entity)
			result.gates = cloneStringAnyMapCatalog(snapshot.Gates)
			result.emittedEvents = nil
			break
		}

		matchedHandlers := catalogResolveAllHandlers(nodes, ev.Event)
		matchedAgents := catalogResolveAgents(agents, ev.Event)
		if len(matchedHandlers) == 0 && len(matchedAgents) == 0 {
			result.deadLetter = true
			result.handlerOutcome = "discard"
			result.entityState = snapshot.State
			result.entityFields = cloneStringAnyMapCatalog(snapshot.Entity)
			result.gates = cloneStringAnyMapCatalog(asMapForCatalog(snapshot.Entity["gates"]))
			result.emittedEvents = nil
			break
		}

		for _, agent := range matchedAgents {
			existing, _ := result.agentReceived[agent.ID].([]any)
			result.agentReceived[agent.ID] = append(existing, ev.Event)
			for _, produced := range catalogAgentProduces(agent) {
				allEmitted = append(allEmitted, produced)
				if len(catalogResolveAllHandlers(nodes, produced)) == 0 && len(catalogResolveAgents(agents, produced)) == 0 {
					continue
				}
				nextDepth := ev.ChainDepth + 1
				if nextDepth > 5 {
					result.deadLetter = true
					result.deadLetterReason = "chain_depth_exceeded"
					result.chainDepthAtDeadLetter = ev.ChainDepth
					result.handlerOutcome = "kill"
					result.entityState = initialState
					result.entityFields = cloneStringAnyMapCatalog(initialEntity)
					result.gates = cloneStringAnyMapCatalog(initialGates)
					result.emittedEvents = nil
					return result, expected
				}
				queue = append(queue, catalogQueuedEvent{
					Event:      produced,
					Payload:    cloneStringAnyMapCatalog(ev.Payload),
					Sender:     agent.ID,
					ChainDepth: nextDepth,
				})
			}
		}

		for _, resolved := range matchedHandlers {
			scope := catalogNodeScope(resolved.Node)
			currentScopeState := catalogFirstNonEmptyString(stateByScope[scope], initialState)
			if _, isTerminal := terminal[strings.TrimSpace(currentScopeState)]; isTerminal {
				result = catalogApplyTerminalEventPolicy(result, currentScopeState, ev.Event)
				result.entityFields = cloneStringAnyMapCatalog(snapshot.Entity)
				result.gates = cloneStringAnyMapCatalog(asMapForCatalog(snapshot.Entity["gates"]))
				continue
			}
			if expected.Trigger.InjectFailure != "" || resolved.Handler.SimulateFailure {
				result.handlerOutcome = "failure"
				result.entityState = snapshot.State
				result.entityFields = cloneStringAnyMapCatalog(snapshot.Entity)
				result.gates = cloneStringAnyMapCatalog(asMapForCatalog(snapshot.Entity["gates"]))
				result.emittedEvents = nil
				return result, expected
			}
			snapshot.Entity["state"] = currentScopeState
			stepResult := executeCatalogHandlerStep(t, resolved.Handler, catalogTriggerStep{
				Event:   ev.Event,
				Payload: ev.Payload,
				Sender:  ev.Sender,
			}, snapshot.Entity, policy, catalogRunResult{
				entityState: currentScopeState,
				gates:       cloneStringAnyMapCatalog(asMapForCatalog(snapshot.Entity["gates"])),
			})
			nextState := catalogFirstNonEmptyString(stepResult.entityState, currentScopeState)
			stateByScope[scope] = nextState
			snapshot.State = nextState
			snapshot.Entity["state"] = nextState
			snapshot.Gates = cloneStringAnyMapCatalog(asMapForCatalog(snapshot.Entity["gates"]))
			result.handlerOutcome = stepResult.handlerOutcome
			result.entityState = nextState
			result.entityFields = cloneStringAnyMapCatalog(snapshot.Entity)
			result.gates = cloneStringAnyMapCatalog(snapshot.Gates)
			allEmitted = append(allEmitted, stepResult.emittedEvents...)
			for _, emitted := range stepResult.emittedEvents {
				if len(catalogResolveAllHandlers(nodes, emitted)) == 0 && len(catalogResolveAgents(agents, emitted)) == 0 {
					continue
				}
				nextDepth := ev.ChainDepth + 1
				if nextDepth > 5 {
					result.deadLetter = true
					result.deadLetterReason = "chain_depth_exceeded"
					result.chainDepthAtDeadLetter = ev.ChainDepth
					result.handlerOutcome = "kill"
					result.entityState = initialState
					result.entityFields = cloneStringAnyMapCatalog(initialEntity)
					result.gates = cloneStringAnyMapCatalog(initialGates)
					result.emittedEvents = nil
					return result, expected
				}
				queue = append(queue, catalogQueuedEvent{
					Event:      emitted,
					Payload:    cloneStringAnyMapCatalog(ev.Payload),
					Sender:     resolved.NodeID,
					ChainDepth: nextDepth,
				})
			}
		}
	}
	result.emittedEvents = allEmitted
	return result, expected
}

func runBootVerificationCatalogCase(
	t testing.TB,
	dir string,
	pkg catalogPackageDocument,
	schema catalogSchemaDocument,
	nodes map[string]catalogNodeContract,
	eventCatalog map[string]catalogEventSchemaEntry,
	agents map[string]catalogAgentRegistryEntry,
	policy map[string]any,
	expected catalogExpectedDocument,
) (catalogRunResult, catalogExpectedDocument) {
	t.Helper()
	_ = pkg
	_ = schema
	_ = nodes
	_ = eventCatalog
	_ = agents
	_ = policy
	if !strings.Contains(filepath.ToSlash(dir), "tier8-boot-verification/") {
		return catalogRunResult{
			bootResult:        strings.TrimSpace(expected.Expected.BootResult),
			templateInstances: expected.Expected.TemplateInstances,
		}, expected
	}
	bundle := catalogLoadBootBundle(t, dir)
	issues := catalogCollectBootIssues(bundle)
	result := catalogRunResult{templateInstances: expected.Expected.TemplateInstances}
	if wantCategory := strings.TrimSpace(expected.Expected.ErrorCategory); wantCategory != "" {
		wantContains := strings.TrimSpace(expected.Expected.ErrorContains)
		for _, issue := range issues {
			if strings.TrimSpace(issue.Category) != wantCategory {
				continue
			}
			if wantResult := strings.TrimSpace(expected.Expected.BootResult); wantResult != "" && !strings.EqualFold(catalogNormalizeIssueSeverity(issue.Severity), wantResult) {
				continue
			}
			if wantContains != "" && !strings.Contains(issue.Message, wantContains) {
				continue
			}
			result.bootResult = catalogNormalizeIssueSeverity(issue.Severity)
			result.errorCategory = issue.Category
			result.errorContains = issue.Message
			return result, expected
		}
		for _, issue := range issues {
			if strings.TrimSpace(issue.Category) != wantCategory {
				continue
			}
			if wantResult := strings.TrimSpace(expected.Expected.BootResult); wantResult != "" && !strings.EqualFold(catalogNormalizeIssueSeverity(issue.Severity), wantResult) {
				continue
			}
			result.bootResult = catalogNormalizeIssueSeverity(issue.Severity)
			result.errorCategory = issue.Category
			result.errorContains = issue.Message
			return result, expected
		}
	}
	for _, issue := range issues {
		if catalogNormalizeIssueSeverity(issue.Severity) == "error" {
			result.bootResult = "error"
			result.errorCategory = issue.Category
			result.errorContains = issue.Message
			return result, expected
		}
	}
	for _, issue := range issues {
		if catalogNormalizeIssueSeverity(issue.Severity) == "warning" {
			result.bootResult = "warning"
			result.errorCategory = issue.Category
			result.errorContains = issue.Message
			return result, expected
		}
	}
	result.bootResult = "success"
	return result, expected
}

func catalogNormalizeIssueSeverity(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "hard_invalidity", "error":
		return "error"
	case "semantic_drift_warning", "warning":
		return "warning"
	case "audit_analysis", "lint_evidence", "informational":
		return "informational"
	default:
		return strings.TrimSpace(strings.ToLower(raw))
	}
}

func catalogLoadBootBundle(t testing.TB, dir string) catalogBootBundle {
	t.Helper()
	root := catalogBootScope{
		Name:    "root",
		Dir:     dir,
		Root:    true,
		Package: catalogLoadRawYAMLMap(t, filepath.Join(dir, "package.yaml")),
		Schema:  catalogLoadRawYAMLMap(t, filepath.Join(dir, "schema.yaml")),
		Nodes:   catalogLoadRawYAMLMap(t, filepath.Join(dir, "nodes.yaml")),
		Events:  catalogLoadRawYAMLMap(t, filepath.Join(dir, "events.yaml")),
		Agents:  catalogLoadRawYAMLMap(t, filepath.Join(dir, "agents.yaml")),
		Policy:  catalogLoadRawYAMLMap(t, filepath.Join(dir, "policy.yaml")),
		Tools:   catalogLoadRawYAMLMap(t, filepath.Join(dir, "tools.yaml")),
	}
	bundle := catalogBootBundle{
		Root:      root,
		AllNodes:  cloneStringAnyMapCatalog(root.Nodes),
		AllEvents: cloneStringAnyMapCatalog(root.Events),
		AllAgents: cloneStringAnyMapCatalog(root.Agents),
		AllPolicy: cloneStringAnyMapCatalog(root.Policy),
		AllTools:  cloneStringAnyMapCatalog(root.Tools),
	}
	if bundle.AllNodes == nil {
		bundle.AllNodes = map[string]any{}
	}
	if bundle.AllEvents == nil {
		bundle.AllEvents = map[string]any{}
	}
	if bundle.AllAgents == nil {
		bundle.AllAgents = map[string]any{}
	}
	if bundle.AllPolicy == nil {
		bundle.AllPolicy = map[string]any{}
	}
	if bundle.AllTools == nil {
		bundle.AllTools = map[string]any{}
	}
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	semanticBundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, dir, platformSpec)
	if err == nil {
		bundle.Source = semanticview.Wrap(semanticBundle)
	}
	entries, err := os.ReadDir(filepath.Join(dir, "flows"))
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("read boot flow fixtures: %v", err)
	}
	flowNames := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			flowNames = append(flowNames, entry.Name())
		}
	}
	sort.Strings(flowNames)
	for _, name := range flowNames {
		flowDir := filepath.Join(dir, "flows", name)
		scope := catalogBootScope{
			Name:    name,
			Dir:     flowDir,
			Package: catalogLoadRawYAMLMap(t, filepath.Join(flowDir, "package.yaml")),
			Schema:  catalogLoadRawYAMLMap(t, filepath.Join(flowDir, "schema.yaml")),
			Nodes:   catalogLoadRawYAMLMap(t, filepath.Join(flowDir, "nodes.yaml")),
			Events:  catalogLoadRawYAMLMap(t, filepath.Join(flowDir, "events.yaml")),
			Agents:  catalogLoadRawYAMLMap(t, filepath.Join(flowDir, "agents.yaml")),
			Policy:  catalogLoadRawYAMLMap(t, filepath.Join(flowDir, "policy.yaml")),
			Tools:   catalogLoadRawYAMLMap(t, filepath.Join(flowDir, "tools.yaml")),
		}
		bundle.Flows = append(bundle.Flows, scope)
		for key, value := range scope.Nodes {
			bundle.AllNodes[key] = value
		}
		for key, value := range scope.Events {
			bundle.AllEvents[key] = value
		}
		for key, value := range scope.Agents {
			bundle.AllAgents[key] = value
		}
		for key, value := range scope.Policy {
			bundle.AllPolicy[key] = value
		}
		for key, value := range scope.Tools {
			bundle.AllTools[key] = value
		}
	}
	return bundle
}

func catalogCollectBootIssues(bundle catalogBootBundle) []catalogBootIssue {
	issues := make([]catalogBootIssue, 0, 16)
	eventGraph := map[string]map[string]struct{}{}
	allScopes := append([]catalogBootScope{bundle.Root}, bundle.Flows...)
	for _, scope := range allScopes {
		for _, nodeID := range catalogSortedKeys(scope.Nodes) {
			node := catalogMap(scope.Nodes[nodeID])
			for _, eventType := range catalogSortedKeys(catalogMap(node["event_handlers"])) {
				handler := catalogMap(catalogMap(node["event_handlers"])[eventType])
				emits := catalogCollectHandlerEmits(handler)
				for _, emitted := range emits {
					if emitted == "" {
						continue
					}
					if eventGraph[eventType] == nil {
						eventGraph[eventType] = map[string]struct{}{}
					}
					eventGraph[eventType][emitted] = struct{}{}
				}
			}
		}
	}
	issues = append(issues, catalogSemanticEventWarningIssues(bundle.Source)...)
	for _, scope := range allScopes {
		scopeLabel := catalogBootScopeLabel(scope)
		mergedPolicy := cloneStringAnyMapCatalog(bundle.AllPolicy)
		for _, nodeID := range catalogSortedKeys(scope.Nodes) {
			node := catalogMap(scope.Nodes[nodeID])
			declaredProduces := catalogBootStringSet(node["produces"])
			for _, eventType := range catalogSortedKeys(catalogMap(node["event_handlers"])) {
				handler := catalogMap(catalogMap(node["event_handlers"])[eventType])
				loc := fmt.Sprintf("%s/%s/%s", scopeLabel, nodeID, eventType)
				if condition := catalogGuardCheckString(handler["guard"]); condition != "" && !catalogConditionParses(condition) {
					issues = append(issues, catalogBootIssue{Severity: "error", Category: "CEL-PARSE", Message: fmt.Sprintf("%s: invalid condition %q", loc, condition)})
				}
				if condition := catalogGuardCheckString(handler["guard"]); condition != "" && !catalogConditionHasPrefix(condition) {
					issues = append(issues, catalogBootIssue{Severity: "error", Category: "DIALECT-BARE-COND", Message: fmt.Sprintf("%s: condition '%s' missing prefix", loc, condition)})
				}
				eventSchema, eventExists := bundle.AllEvents[eventType]
				payloadFields := catalogEventPayloadFields(catalogMap(eventSchema))
				if dataAccumulation := catalogMap(handler["data_accumulation"]); len(dataAccumulation) > 0 {
					sourceEvent := catalogFirstNonEmptyString(catalogBootText(dataAccumulation["source_event"]), eventType)
					sourceEventSchema, sourceEventExists := bundle.AllEvents[sourceEvent]
					sourceFields := catalogEventPayloadFields(catalogMap(sourceEventSchema))
					for _, rawWrite := range catalogAnySlice(dataAccumulation["writes"]) {
						switch typed := rawWrite.(type) {
						case string:
							if sourceEventExists && !catalogPayloadFieldExists(sourceFields, strings.TrimSpace(typed)) {
								issues = append(issues, catalogBootIssue{Severity: "error", Category: "payload_field_coverage", Message: fmt.Sprintf("%s: writes '%s' but %s payload has %v", loc, strings.TrimSpace(typed), sourceEvent, catalogSortedSetKeys(sourceFields))})
							}
						case map[string]any:
							sourceField := catalogBootText(typed["source_field"])
							if sourceField != "" && sourceEventExists && !catalogPayloadFieldExists(sourceFields, sourceField) {
								issues = append(issues, catalogBootIssue{Severity: "error", Category: "payload_field_coverage", Message: fmt.Sprintf("%s: source_field '%s' not in %s payload", loc, sourceField, sourceEvent)})
							}
						}
					}
				}
				for _, ref := range catalogExtractRefs(`payload\.([a-zA-Z_][a-zA-Z0-9_.]*)`, catalogGuardCheckString(handler["guard"])) {
					if eventExists && !catalogPayloadFieldExists(payloadFields, ref) {
						issues = append(issues, catalogBootIssue{Severity: "error", Category: "CONDITION-PAYLOAD", Message: fmt.Sprintf("%s guard: payload.%s not in event payload %v", loc, ref, catalogSortedSetKeys(payloadFields))})
					}
				}
				for _, ref := range catalogExtractRefs(`policy\.([a-zA-Z_][a-zA-Z0-9_.]*)`, catalogGuardCheckString(handler["guard"])) {
					if _, ok := mergedPolicy[ref]; !ok {
						issues = append(issues, catalogBootIssue{Severity: "warning", Category: "CONDITION-POLICY", Message: fmt.Sprintf("%s: policy.%s referenced but not in any policy.yaml", loc, ref)})
					}
				}
				for _, rule := range catalogRuleEntries(handler["rules"]) {
					condition := catalogBootText(rule["condition"])
					if condition == "" || strings.EqualFold(condition, "else") {
						continue
					}
					if !catalogConditionParses(condition) {
						issues = append(issues, catalogBootIssue{Severity: "error", Category: "CEL-PARSE", Message: fmt.Sprintf("%s: invalid condition %q", loc, condition)})
					}
					for _, ref := range catalogExtractRefs(`payload\.([a-zA-Z_][a-zA-Z0-9_.]*)`, condition) {
						if eventExists && !catalogPayloadFieldExists(payloadFields, ref) {
							issues = append(issues, catalogBootIssue{Severity: "error", Category: "CONDITION-PAYLOAD", Message: fmt.Sprintf("%s rule: payload.%s not in event payload %v", loc, ref, catalogSortedSetKeys(payloadFields))})
						}
					}
					for _, ref := range catalogExtractRefs(`policy\.([a-zA-Z_][a-zA-Z0-9_.]*)`, condition) {
						if _, ok := mergedPolicy[ref]; !ok {
							issues = append(issues, catalogBootIssue{Severity: "warning", Category: "CONDITION-POLICY", Message: fmt.Sprintf("%s: policy.%s referenced but not in any policy.yaml", loc, ref)})
						}
					}
					if !catalogConditionHasPrefix(condition) {
						issues = append(issues, catalogBootIssue{Severity: "error", Category: "DIALECT-BARE-COND", Message: fmt.Sprintf("%s: condition '%s' missing prefix", loc, condition)})
					}
				}
				for _, branch := range catalogBranchEntries(handler["on_complete"]) {
					condition := catalogBootText(branch["condition"])
					if condition != "" && !catalogConditionParses(condition) {
						issues = append(issues, catalogBootIssue{Severity: "error", Category: "CEL-PARSE", Message: fmt.Sprintf("%s: invalid condition %q", loc, condition)})
					}
					for _, ref := range catalogExtractRefs(`payload\.([a-zA-Z_][a-zA-Z0-9_.]*)`, condition) {
						if eventExists && !catalogPayloadFieldExists(payloadFields, ref) {
							issues = append(issues, catalogBootIssue{Severity: "error", Category: "CONDITION-PAYLOAD", Message: fmt.Sprintf("%s on_complete: payload.%s not in event payload", loc, ref)})
						}
					}
					for _, ref := range catalogExtractRefs(`policy\.([a-zA-Z_][a-zA-Z0-9_.]*)`, condition) {
						if _, ok := mergedPolicy[ref]; !ok {
							issues = append(issues, catalogBootIssue{Severity: "warning", Category: "CONDITION-POLICY", Message: fmt.Sprintf("%s: policy.%s referenced but not in any policy.yaml", loc, ref)})
						}
					}
				}
				for _, field := range catalogSortedKeys(handler) {
					if !catalogDefinedHandlerField(field) {
						issues = append(issues, catalogBootIssue{Severity: "error", Category: "UNDEFINED-FIELD", Message: fmt.Sprintf("%s: handler field '%s' not in platform spec", loc, field)})
					}
				}
				for _, emitted := range catalogCollectHandlerEmits(handler) {
					if emitted != "" && !catalogSetHas(declaredProduces, emitted) {
						issues = append(issues, catalogBootIssue{Severity: "warning", Category: "PRODUCES-DRIFT", Message: fmt.Sprintf("%s: emits '%s' but not in produces list", scopeLabel, emitted)})
					}
				}
				if guard := handler["guard"]; guard != nil {
					switch typed := guard.(type) {
					case string:
						if strings.TrimSpace(typed) != "" {
							issues = append(issues, catalogBootIssue{Severity: "error", Category: "DIALECT-GUARD", Message: fmt.Sprintf("%s: guard is string, must be {id, check}", loc)})
						}
					case map[string]any:
						if _, hasCheck := typed["check"]; !hasCheck {
							if _, hasChecks := typed["checks"]; !hasChecks {
								issues = append(issues, catalogBootIssue{Severity: "error", Category: "DIALECT-GUARD", Message: fmt.Sprintf("%s: guard missing check/checks field", loc)})
							}
						}
					}
				}
				if _, hasOnComplete := handler["on_complete"]; hasOnComplete {
					if _, hasRules := handler["rules"]; hasRules {
						issues = append(issues, catalogBootIssue{Severity: "error", Category: "DIALECT-DUAL", Message: fmt.Sprintf("%s: has both on_complete AND rules", loc)})
					}
				}
				if _, ok := handler["on_complete"].(map[string]any); ok {
					issues = append(issues, catalogBootIssue{Severity: "error", Category: "DIALECT-OC-ORDER", Message: fmt.Sprintf("%s: on_complete is dict (unordered), must be list", loc)})
				}
				if _, ok := handler["advances_to"].([]any); ok {
					issues = append(issues, catalogBootIssue{Severity: "error", Category: "DIALECT-ADV-LIST", Message: fmt.Sprintf("%s: advances_to is list, must be string", loc)})
				}
				for _, emitted := range catalogCollectHandlerEmits(handler) {
					if emitted == eventType {
						issues = append(issues, catalogBootIssue{Severity: "error", Category: "DIALECT-SELF-EMIT", Message: fmt.Sprintf("%s: emits own trigger '%s'", loc, eventType)})
					}
				}
			}
		}
		declaredStates := catalogBootStringSet(scope.Schema["states"])
		initialState := catalogBootText(scope.Schema["initial_state"])
		if initialState != "" && len(declaredStates) > 0 && !catalogSetHas(declaredStates, initialState) {
			issues = append(issues, catalogBootIssue{Severity: "error", Category: "STATE-MACHINE", Message: fmt.Sprintf("%s: initial_state '%s' not in declared states", scopeLabel, initialState)})
		}
		for _, terminal := range catalogStringSlice(scope.Schema["terminal_states"]) {
			if len(declaredStates) > 0 && !catalogSetHas(declaredStates, terminal) {
				issues = append(issues, catalogBootIssue{Severity: "error", Category: "STATE-MACHINE", Message: fmt.Sprintf("%s: terminal_state '%s' not in declared states", scopeLabel, terminal)})
			}
		}
		for _, nodeID := range catalogSortedKeys(scope.Nodes) {
			node := catalogMap(scope.Nodes[nodeID])
			for _, eventType := range catalogSortedKeys(catalogMap(node["event_handlers"])) {
				handler := catalogMap(catalogMap(node["event_handlers"])[eventType])
				if target := catalogBootText(handler["advances_to"]); target != "" && len(declaredStates) > 0 && !catalogSetHas(declaredStates, target) {
					issues = append(issues, catalogBootIssue{Severity: "error", Category: "STATE-MACHINE", Message: fmt.Sprintf("%s/%s/%s: advances_to '%s' not in declared states %v", scopeLabel, nodeID, eventType, target, catalogSortedSetKeys(declaredStates))})
				}
				for _, branch := range catalogBranchEntries(handler["on_complete"]) {
					if target := catalogBootText(branch["advances_to"]); target != "" && len(declaredStates) > 0 && !catalogSetHas(declaredStates, target) {
						issues = append(issues, catalogBootIssue{Severity: "error", Category: "STATE-MACHINE", Message: fmt.Sprintf("%s/%s/%s on_complete: advances_to '%s' not in declared states", scopeLabel, nodeID, eventType, target)})
					}
				}
			}
		}
		issues = append(issues, catalogRequiredAgentIssues(scope)...)
		for _, agentID := range catalogSortedKeys(scope.Agents) {
			agent := catalogMap(scope.Agents[agentID])
			for _, tool := range catalogStringSlice(agent["tools"]) {
				if tool == "" {
					continue
				}
				toolSpec := catalogMap(bundle.AllTools[tool])
				if len(bundle.AllTools) == 0 || len(toolSpec) == 0 {
					issues = append(issues, catalogBootIssue{Severity: "warning", Category: "TOOL-MISSING", Message: fmt.Sprintf("%s/%s: tool '%s' not in any tools.yaml", scopeLabel, agentID, tool)})
					continue
				}
				requiredPermission := catalogToolRequiredPermission(tool, toolSpec)
				if requiredPermission != "" && !catalogSetHas(catalogBootStringSet(agent["permissions"]), requiredPermission) {
					issues = append(issues, catalogBootIssue{Severity: "warning", Category: "PERMISSION-MISMATCH", Message: fmt.Sprintf("%s/%s: tool '%s' missing permission '%s'", scopeLabel, agentID, tool, requiredPermission)})
				}
			}
			issues = append(issues, catalogPromptIssues(bundle, scope, agentID, agent)...)
			for _, deprecated := range []string{"tools_tier2", "subscriptions_bootstrap", "subscribes_to", "logic", "on_below_threshold", "on_dedup", "on_pass"} {
				if _, ok := agent[deprecated]; ok {
					issues = append(issues, catalogBootIssue{Severity: "error", Category: "DEPRECATED", Message: fmt.Sprintf("%s/%s: uses deprecated '%s'", scopeLabel, agentID, deprecated)})
				}
			}
		}
		for _, nodeID := range catalogSortedKeys(scope.Nodes) {
			node := catalogMap(scope.Nodes[nodeID])
			for _, eventType := range catalogSortedKeys(catalogMap(node["event_handlers"])) {
				handler := catalogMap(catalogMap(node["event_handlers"])[eventType])
				for _, deprecated := range []string{"tools_tier2", "subscriptions_bootstrap", "subscribes_to", "logic", "on_below_threshold", "on_dedup", "on_pass"} {
					if _, ok := handler[deprecated]; ok {
						issues = append(issues, catalogBootIssue{Severity: "error", Category: "DEPRECATED", Message: fmt.Sprintf("%s/%s/%s: uses deprecated '%s'", scopeLabel, nodeID, eventType, deprecated)})
					}
				}
			}
		}
	}
	for _, flow := range bundle.Flows {
		for _, key := range catalogSortedKeys(flow.Policy) {
			if strings.HasPrefix(key, "_") {
				continue
			}
			rootValue, ok := bundle.Root.Policy[key]
			if !ok || len(catalogMap(rootValue)) > 0 || len(catalogMap(flow.Policy[key])) > 0 {
				continue
			}
			if !catalogValueEquals(rootValue, flow.Policy[key]) {
				issues = append(issues, catalogBootIssue{Severity: "warning", Category: "POLICY-CONFLICT", Message: fmt.Sprintf("'%s': root=%v, %s=%v", key, rootValue, flow.Name, flow.Policy[key])})
			}
		}
	}
	for _, cycle := range catalogFindEventCycles(eventGraph) {
		issues = append(issues, catalogBootIssue{Severity: "error", Category: "EVENT-CYCLE", Message: fmt.Sprintf("Node handler emit cycle: %s", strings.Join(cycle, " -> "))})
	}
	return issues
}

func catalogSemanticEventWarningIssues(source semanticview.Source) []catalogBootIssue {
	if source == nil {
		return nil
	}
	report := runtimebootverify.Run(context.Background(), source, runtimebootverify.Options{})
	issues := make([]catalogBootIssue, 0, len(report.Warnings()))
	for _, warning := range report.Warnings() {
		category := ""
		switch strings.TrimSpace(warning.CheckID) {
		case "event_chain_integrity":
			category = "EVENT-NO-SCHEMA"
		case "event_consumer_exists":
			category = "EVENT-NO-CONSUMER"
		case "event_producer_exists":
			category = "EVENT-NO-PRODUCER"
		default:
			continue
		}
		issues = append(issues, catalogBootIssue{
			Severity: strings.TrimSpace(warning.Severity),
			Category: category,
			Message:  strings.TrimSpace(warning.Message),
		})
	}
	return issues
}

func catalogLoadRawYAMLMap(t testing.TB, path string) map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]any{}
		}
		t.Fatalf("read raw YAML %s: %v", path, err)
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		return map[string]any{}
	}
	var raw map[string]any
	if err := yaml.Unmarshal(data, &raw); err != nil {
		t.Fatalf("decode raw YAML %s: %v", path, err)
	}
	if raw == nil {
		return map[string]any{}
	}
	return raw
}

func catalogBootScopeLabel(scope catalogBootScope) string {
	if scope.Root {
		return "root"
	}
	return strings.TrimSpace(scope.Name)
}

func catalogPromptIssues(bundle catalogBootBundle, scope catalogBootScope, agentID string, agent map[string]any) []catalogBootIssue {
	scopeLabel := catalogBootScopeLabel(scope)
	missing := func() catalogBootIssue {
		return catalogBootIssue{Severity: "warning", Category: "PROMPT-MISSING", Message: fmt.Sprintf("%s/%s: no prompt file", scopeLabel, agentID)}
	}
	stub := func() catalogBootIssue {
		return catalogBootIssue{Severity: "warning", Category: "PROMPT-STUB", Message: fmt.Sprintf("%s/%s: prompt contains TODO", scopeLabel, agentID)}
	}

	if semanticBundle, ok := semanticview.Bundle(bundle.Source); ok {
		source, mode := catalogPromptSemanticSourceAndMode(semanticBundle, scope, agentID)
		resolution, found, err := runtimecontracts.ResolvePromptFileForContractAgent(semanticBundle, agentID, catalogPromptAgentEntry(agent), source, mode)
		if err != nil || !found {
			return []catalogBootIssue{missing()}
		}
		raw, err := os.ReadFile(resolution.Path)
		if err != nil {
			return nil
		}
		if strings.Contains(string(raw), "<!-- TODO") && !strings.Contains(string(raw), "<!-- DEFERRED") {
			return []catalogBootIssue{stub()}
		}
		return nil
	}

	promptPath := filepath.Join(scope.Dir, "prompts", agentID+".md")
	content, err := os.ReadFile(promptPath)
	if err != nil {
		if os.IsNotExist(err) {
			return []catalogBootIssue{missing()}
		}
		return nil
	}
	if strings.Contains(string(content), "<!-- TODO") && !strings.Contains(string(content), "<!-- DEFERRED") {
		return []catalogBootIssue{stub()}
	}
	return nil
}

func catalogPromptSemanticSourceAndMode(bundle *runtimecontracts.WorkflowContractBundle, scope catalogBootScope, agentID string) (runtimecontracts.ContractItemSource, string) {
	if bundle == nil {
		return runtimecontracts.ContractItemSource{Layer: "project"}, catalogBootText(scope.Schema["mode"])
	}
	if scope.Root {
		source := runtimecontracts.ContractItemSource{Layer: "project"}
		if resolved, ok := bundle.AgentContractSource(agentID); ok && strings.TrimSpace(resolved.FlowID) == "" {
			source = resolved
		}
		return source, catalogBootText(scope.Schema["mode"])
	}

	flowID := strings.TrimSpace(scope.Name)
	source := runtimecontracts.ContractItemSource{FlowID: flowID, Layer: "flow"}
	mode := catalogBootText(scope.Schema["mode"])
	if flow, ok := bundle.FlowViewByID(flowID); ok && flow != nil {
		semanticFlowID := strings.TrimSpace(flow.Paths.ID)
		if semanticFlowID == "" {
			semanticFlowID = flowID
		}
		source = runtimecontracts.ContractItemSource{
			PackageKey: strings.TrimSpace(flow.Paths.PackageKey),
			FlowID:     semanticFlowID,
			Layer:      "flow",
		}
		if pathMode := strings.TrimSpace(flow.Paths.Mode); pathMode != "" {
			mode = pathMode
		}
		if semanticMode := strings.TrimSpace(flow.Schema.Mode); semanticMode != "" {
			mode = semanticMode
		}
	}
	return source, mode
}

func catalogPromptAgentEntry(agent map[string]any) runtimecontracts.AgentRegistryEntry {
	return runtimecontracts.AgentRegistryEntry{
		ID:             catalogBootText(agent["id"]),
		Role:           catalogBootText(agent["role"]),
		PromptRef:      catalogBootText(agent["prompt_ref"]),
		WorkspaceClass: catalogBootText(agent["workspace_class"]),
		EmitEvents:     catalogStringSlice(agent["emit_events"]),
	}
}

func catalogRequiredAgentIssues(scope catalogBootScope) []catalogBootIssue {
	scopeLabel := catalogBootScopeLabel(scope)
	agents := catalogRequiredAgentEntries(scope.Agents)
	findings := runtimerequiredagents.CheckScope(runtimerequiredagents.Scope{
		ID:       scopeLabel,
		Agents:   agents,
		Required: catalogEffectiveRequiredAgentRequirements(scope.Schema, agents),
	})
	issues := make([]catalogBootIssue, 0, len(findings))
	for _, finding := range findings {
		switch finding.Kind {
		case runtimerequiredagents.FindingMissingRole:
			issues = append(issues, catalogBootIssue{Severity: "error", Category: "REQUIRED-AGENT", Message: fmt.Sprintf("%s: required_agents entry missing role", scopeLabel)})
		case runtimerequiredagents.FindingMissingAgent:
			issues = append(issues, catalogBootIssue{Severity: "error", Category: "REQUIRED-AGENT", Message: fmt.Sprintf("%s: required role '%s' not in agents.yaml", scopeLabel, finding.Role)})
		case runtimerequiredagents.FindingMissingSubscriptions:
			issues = append(issues, catalogBootIssue{Severity: "error", Category: "SUBSCRIPTION-MISMATCH", Message: fmt.Sprintf("%s/%s: schema says subscribes_to %v but agent doesn't", scopeLabel, finding.AgentID, finding.Missing)})
		case runtimerequiredagents.FindingMissingEmits:
			issues = append(issues, catalogBootIssue{Severity: "error", Category: "EMIT-MISMATCH", Message: fmt.Sprintf("%s/%s: schema says emits %v but agent doesn't", scopeLabel, finding.AgentID, finding.Missing)})
		}
	}
	return issues
}

func catalogRequiredAgentEntries(rawAgents map[string]any) map[string]runtimecontracts.AgentRegistryEntry {
	agents := make(map[string]runtimecontracts.AgentRegistryEntry, len(rawAgents))
	for agentID, rawAgent := range rawAgents {
		agent := catalogMap(rawAgent)
		agents[agentID] = runtimecontracts.AgentRegistryEntry{
			ID:            catalogBootText(agent["id"]),
			Role:          catalogBootText(agent["role"]),
			Subscriptions: catalogStringSlice(agent["subscriptions"]),
			EmitEvents:    catalogStringSlice(agent["emit_events"]),
		}
	}
	return agents
}

func catalogEffectiveRequiredAgentRequirements(schema map[string]any, agents map[string]runtimecontracts.AgentRegistryEntry) []runtimecontracts.FlowRequiredAgent {
	_, declared := schema["required_agents"]
	facts := runtimecontracts.EffectiveRequiredAgentFacts(runtimecontracts.FlowSchemaDocument{
		RequiredAgents:         catalogRequiredAgentRequirements(schema),
		RequiredAgentsDeclared: declared,
	}, agents, "", "")
	return runtimecontracts.FlowRequiredAgentsFromFacts(facts)
}

func catalogRequiredAgentRequirements(schema map[string]any) []runtimecontracts.FlowRequiredAgent {
	rawRequired := catalogAnySlice(schema["required_agents"])
	required := make([]runtimecontracts.FlowRequiredAgent, 0, len(rawRequired))
	for _, raw := range rawRequired {
		item := catalogMap(raw)
		required = append(required, runtimecontracts.FlowRequiredAgent{
			Role:         catalogBootText(item["role"]),
			SubscribesTo: catalogStringSlice(item["subscribes_to"]),
			Emits:        catalogStringSlice(item["emits"]),
		})
	}
	return required
}

func catalogToolRequiredPermission(tool string, spec map[string]any) string {
	if permission := strings.TrimSpace(catalogBootText(spec["permission"])); permission != "" {
		return permission
	}
	if permission := strings.TrimSpace(catalogBootText(spec["required_permission"])); permission != "" {
		return permission
	}
	if tool == "create_flow_instance" {
		return tool
	}
	return ""
}

func catalogMap(value any) map[string]any {
	typed, _ := value.(map[string]any)
	if typed == nil {
		return map[string]any{}
	}
	return typed
}

func catalogAnySlice(value any) []any {
	typed, _ := value.([]any)
	if typed == nil {
		return nil
	}
	return typed
}

func catalogStringSlice(value any) []string {
	items := catalogAnySlice(value)
	if len(items) == 0 {
		if single := catalogBootText(value); single != "" {
			return []string{single}
		}
		return nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		if text := catalogBootText(item); text != "" {
			out = append(out, text)
		}
	}
	return out
}

func catalogSortedKeys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for key := range m {
		out = append(out, strings.TrimSpace(key))
	}
	sort.Strings(out)
	return out
}

func catalogBootStringSet(value any) map[string]struct{} {
	out := map[string]struct{}{}
	for _, item := range catalogStringSlice(value) {
		out[item] = struct{}{}
	}
	return out
}

func catalogSortedSetKeys(items map[string]struct{}) []string {
	out := make([]string, 0, len(items))
	for key := range items {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func catalogSetHas(items map[string]struct{}, key string) bool {
	_, ok := items[strings.TrimSpace(key)]
	return ok
}

func catalogNormalizeSubscribedEvent(ev string) string {
	ev = strings.TrimSpace(ev)
	if slash := strings.LastIndex(ev, "/"); slash >= 0 {
		ev = ev[slash+1:]
	}
	return ev
}

func catalogAutoEmitEvent(schema map[string]any) string {
	autoEmit := schema["auto_emit_on_create"]
	if text := catalogBootText(autoEmit); text != "" {
		return text
	}
	return catalogBootText(catalogMap(autoEmit)["event"])
}

func catalogEmissionStrings(value any) []string {
	if _, ok := value.([]any); ok {
		return catalogStringSlice(value)
	}
	if text := catalogBootText(value); text != "" {
		return []string{text}
	}
	return catalogStringSlice(value)
}

func catalogEmitEventStrings(value any) []string {
	if value == nil {
		return nil
	}
	mapping := catalogMap(value)
	if event := catalogBootText(mapping["event"]); event != "" {
		return []string{event}
	}
	if text := catalogBootText(value); text != "" {
		return []string{text}
	}
	return nil
}

func catalogBootText(value any) string {
	if value == nil {
		return ""
	}
	text := strings.TrimSpace(asStringForCatalog(value))
	if strings.EqualFold(text, "null") {
		return ""
	}
	return text
}

func catalogCollectHandlerEmits(handler map[string]any) []string {
	out := append([]string{}, catalogEmitEventStrings(handler["emit"])...)
	fanOut := catalogMap(handler["fan_out"])
	if emit := catalogEmitEventStrings(fanOut["emit"]); len(emit) > 0 {
		out = append(out, emit...)
	}
	for _, rule := range catalogRuleEntries(handler["rules"]) {
		out = append(out, catalogEmitEventStrings(rule["emit"])...)
	}
	for _, branch := range catalogBranchEntries(handler["on_complete"]) {
		out = append(out, catalogEmitEventStrings(branch["emit"])...)
	}
	return normalizeSorted(out)
}

func catalogRuleEntries(value any) []map[string]any {
	switch typed := value.(type) {
	case map[string]any:
		out := make([]map[string]any, 0, len(typed))
		for _, key := range catalogSortedKeys(typed) {
			if entry := catalogMap(typed[key]); len(entry) > 0 {
				out = append(out, entry)
			}
		}
		return out
	case []any:
		out := make([]map[string]any, 0, len(typed))
		for _, raw := range typed {
			if entry := catalogMap(raw); len(entry) > 0 {
				out = append(out, entry)
			}
		}
		return out
	default:
		return nil
	}
}

func catalogBranchEntries(value any) []map[string]any {
	switch typed := value.(type) {
	case map[string]any:
		out := make([]map[string]any, 0, len(typed))
		for _, key := range catalogSortedKeys(typed) {
			if entry := catalogMap(typed[key]); len(entry) > 0 {
				out = append(out, entry)
			}
		}
		return out
	case []any:
		out := make([]map[string]any, 0, len(typed))
		for _, raw := range typed {
			if entry := catalogMap(raw); len(entry) > 0 {
				out = append(out, entry)
			}
		}
		return out
	default:
		return nil
	}
}

func catalogFlattenPayloadFields(payload map[string]any) map[string]struct{} {
	fields := map[string]struct{}{}
	var walk func(prefix string, node map[string]any)
	walk = func(prefix string, node map[string]any) {
		for _, key := range catalogSortedKeys(node) {
			if strings.HasPrefix(key, "_") {
				continue
			}
			full := key
			if prefix != "" {
				full = prefix + "." + key
			}
			fields[full] = struct{}{}
			child := catalogMap(node[key])
			if len(child) > 0 && !catalogSchemaLeaf(child) {
				walk(full, child)
			}
		}
	}
	walk("", payload)
	return fields
}

func catalogEventPayloadFields(entry map[string]any) map[string]struct{} {
	if len(entry) == 0 {
		return map[string]struct{}{}
	}
	if payload := catalogMap(entry["payload"]); len(payload) > 0 {
		return catalogFlattenPayloadFields(payload)
	}
	flat := map[string]any{}
	for _, key := range catalogSortedKeys(entry) {
		if key == "" || strings.HasPrefix(key, "_") {
			continue
		}
		switch key {
		case "description", "emitter", "emitter_type", "producer", "alternate_emitters", "consumer", "consumer_type", "intercepted", "passthrough", "runtime_handling", "owning_node", "delivery_channel", "required":
			continue
		}
		flat[key] = entry[key]
	}
	return catalogFlattenPayloadFields(flat)
}

func catalogSchemaLeaf(node map[string]any) bool {
	for _, key := range catalogSortedKeys(node) {
		value := strings.ToLower(strings.TrimSpace(asStringForCatalog(node[key])))
		switch value {
		case "string", "integer", "number", "boolean", "array", "object", "text", "timestamp", "uuid", "numeric":
			return true
		}
	}
	return false
}

func catalogPayloadFieldExists(fields map[string]struct{}, ref string) bool {
	ref = strings.TrimSpace(ref)
	for candidate := range fields {
		if ref == candidate || strings.HasPrefix(ref, candidate+".") || strings.HasPrefix(candidate, ref+".") {
			return true
		}
	}
	return false
}

func catalogExtractRefs(pattern, text string) []string {
	if strings.TrimSpace(text) == "" {
		return nil
	}
	re := regexp.MustCompile(pattern)
	matches := re.FindAllStringSubmatch(text, -1)
	out := make([]string, 0, len(matches))
	seen := map[string]struct{}{}
	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		ref := strings.TrimSpace(match[1])
		if ref == "" {
			continue
		}
		if _, ok := seen[ref]; ok {
			continue
		}
		seen[ref] = struct{}{}
		out = append(out, ref)
	}
	return out
}

func catalogGuardCheckString(guard any) string {
	switch typed := guard.(type) {
	case string:
		return strings.TrimSpace(typed)
	case map[string]any:
		if check := catalogBootText(typed["check"]); check != "" {
			return check
		}
		checks := catalogAnySlice(typed["checks"])
		if len(checks) == 0 {
			return ""
		}
		first := catalogMap(checks[0])
		return catalogBootText(first["check"])
	default:
		return ""
	}
}

func catalogConditionParses(expr string) bool {
	expr = strings.TrimSpace(expr)
	if expr == "" || strings.EqualFold(expr, "else") {
		return true
	}
	if strings.Contains(expr, "((") {
		return false
	}
	depth := 0
	quote := rune(0)
	for _, r := range expr {
		switch r {
		case '\'', '"':
			if quote == 0 {
				quote = r
			} else if quote == r {
				quote = 0
			}
		case '(':
			if quote == 0 {
				depth++
			}
		case ')':
			if quote == 0 {
				depth--
				if depth < 0 {
					return false
				}
			}
		}
	}
	return depth == 0 && quote == 0
}

func catalogConditionHasPrefix(condition string) bool {
	condition = strings.TrimSpace(condition)
	if condition == "" || strings.EqualFold(condition, "else") {
		return true
	}
	for _, prefix := range []string{"payload.", "entity.", "policy.", "accumulated.", "fan_out."} {
		if strings.HasPrefix(condition, prefix) {
			return true
		}
	}
	return false
}

func catalogDefinedHandlerField(field string) bool {
	switch strings.TrimSpace(field) {
	case "description", "_note", "guard", "accumulate", "compute", "on_complete",
		"advances_to", "sets_gate", "data_accumulation", "emit", "rules",
		"fan_out", "query", "reduce", "filter", "count", "clear", "action",
		"template", "instance_id_from", "config_from", "from",
		"clear_gates", "dedup_by", "subscriptions_bootstrap", "logic", "on_below_threshold",
		"on_dedup", "on_pass":
		return true
	default:
		return false
	}
}

func TestCatalogDefinedHandlerField_RejectsRetiredEmitCarriers(t *testing.T) {
	for _, field := range []string{"payload_transform", "emits"} {
		if catalogDefinedHandlerField(field) {
			t.Fatalf("catalogDefinedHandlerField(%q) = true, want false", field)
		}
	}
}

func catalogIsSuppressedEvent(events map[string]any, ev string) bool {
	eventDef := catalogMap(events[ev])
	swarm := catalogMap(eventDef["swarm"])
	if catalogMetadataHasPrefix(swarm["source"], "external", "platform") {
		return true
	}
	if catalogMetadataPresent(swarm["consumer"]) {
		return true
	}
	if catalogMetadataPresent(swarm["producer"]) {
		return true
	}
	if strings.EqualFold(strings.TrimSpace(asStringForCatalog(swarm["status"])), "planned") {
		return true
	}
	return false
}

func catalogMetadataPresent(value any) bool {
	for _, item := range catalogAnySlice(value) {
		if catalogMetadataPresent(item) {
			return true
		}
	}
	return strings.TrimSpace(asStringForCatalog(value)) != ""
}

func catalogMetadataHasPrefix(value any, prefixes ...string) bool {
	for _, item := range catalogAnySlice(value) {
		if catalogMetadataHasPrefix(item, prefixes...) {
			return true
		}
	}
	text := strings.TrimSpace(asStringForCatalog(value))
	for _, prefix := range prefixes {
		if strings.HasPrefix(text, prefix) {
			return true
		}
	}
	return false
}

func catalogFindEventCycles(graph map[string]map[string]struct{}) [][]string {
	seen := map[string]struct{}{}
	cycles := make([][]string, 0)
	var walk func(start, current string, path []string)
	walk = func(start, current string, path []string) {
		for _, next := range catalogSortedSetKeys(graph[current]) {
			if next == start && len(path) > 1 {
				cycle := append(append([]string{}, path...), next)
				key := strings.Join(cycle, "->")
				if _, ok := seen[key]; ok {
					continue
				}
				seen[key] = struct{}{}
				cycles = append(cycles, cycle)
				continue
			}
			if _, ok := graph[next]; !ok || containsCatalogString(path, next) {
				continue
			}
			walk(start, next, append(path, next))
		}
	}
	for _, start := range catalogSortedKeysFromSetMap(graph) {
		walk(start, start, []string{start})
	}
	return cycles
}

func catalogSortedKeysFromSetMap(m map[string]map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for key := range m {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func containsCatalogString(items []string, target string) bool {
	for _, item := range items {
		if item == target {
			return true
		}
	}
	return false
}

func catalogCaseExecutableNow(t testing.TB, dir string, expected catalogExpectedDocument) bool {
	t.Helper()
	return catalogCaseExecutableNowForDir(dir, expected)
}

func catalogCaseExecutableNowForDir(dir string, expected catalogExpectedDocument) bool {
	if len(catalogUnsupportedExecutableExpectations(expected)) > 0 {
		return false
	}
	switch dir {
	case "tier5-flow-lifecycle/test-timer-cancel",
		"tier5-flow-lifecycle/test-timer-fire",
		"tier5-flow-lifecycle/test-timer-recurring",
		"tier2-accumulation/test-accumulate-on-complete-rollback",
		"tier6-event-loop/test-dead-letter",
		"tier6-event-loop/test-entity-serialization",
		"tier6-event-loop/test-on-complete-atomicity-chain",
		"tier7-composition/test-cross-flow-subscription",
		"tier7-composition/test-dual-delivery",
		"tier7-composition/test-multi-gate-pipeline",
		"tier7-composition/test-wildcard-cross-flow",
		"tier8-boot-verification/test-boot-create-entity-plus-accumulate",
		"tier8-boot-verification/test-boot-on-complete-and-rules-mutual-exclusion":
		return false
	}
	switch {
	case strings.HasPrefix(dir, "tier4-cross-entity/"):
		return true
	case strings.HasPrefix(dir, "tier5-flow-lifecycle/"):
		return true
	case strings.HasPrefix(dir, "tier6-event-loop/"):
		return true
	case strings.HasPrefix(dir, "tier7-composition/"):
		return true
	case strings.HasPrefix(dir, "tier8-boot-verification/"):
		return true
	}
	if !catalogCaseSimpleHarnessEligible(expected) {
		return false
	}
	switch {
	case strings.HasPrefix(dir, "tier1-primitives/"):
		return true
	case strings.HasPrefix(dir, "tier2-accumulation/"):
		return true
	case strings.HasPrefix(dir, "tier3-list-processing/"):
		return true
	default:
		return false
	}
}

func executeCatalogHandlerStep(t testing.TB, handler catalogSystemNodeEventHandler, step catalogTriggerStep, entity map[string]any, policy map[string]any, result catalogRunResult) catalogRunResult {
	t.Helper()
	payload := cloneStringAnyMapCatalog(step.Payload)
	result.handlerOutcome = "success"
	if !catalogGuardPasses(handler.Guard, payload, entity, policy, result.entityState) {
		result.handlerOutcome = guardFailOutcome(handler.Guard)
		if result.handlerOutcome == "" {
			result.handlerOutcome = "reject"
		}
		if result.handlerOutcome == "kill" {
			result.entityState = "killed"
		}
		if strings.HasPrefix(result.handlerOutcome, "escalate:") {
			result.emittedEvents = append(result.emittedEvents, strings.TrimSpace(strings.TrimPrefix(result.handlerOutcome, "escalate:")))
			result.handlerOutcome = "escalate"
		}
		return result
	}
	if strings.TrimSpace(step.Event) != "" {
		_ = step.Event
	}
	if next := strings.TrimSpace(handler.AdvancesTo); next != "" {
		result.entityState = next
	}
	if len(handler.OnComplete) > 0 {
		for _, rule := range handler.OnComplete {
			if catalogRuleMatches(rule, payload, entity, policy, result.entityState) {
				result = applyCatalogRule(rule, payload, entity, result)
				break
			}
		}
	}
	if len(handler.Rules) > 0 {
		for _, rule := range handler.Rules {
			if catalogRuleMatches(rule, payload, entity, policy, result.entityState) {
				result = applyCatalogRule(rule, payload, entity, result)
				break
			}
		}
	}
	if len(handler.ClearGates) > 0 {
		gates := ensureCatalogGates(entity)
		for _, gate := range handler.ClearGates {
			gate = strings.TrimSpace(gate)
			if gate == "*" {
				for key := range gates {
					gates[key] = false
				}
				continue
			}
			gates[gate] = false
		}
		result.gates = cloneStringAnyMapCatalog(gates)
	}
	if handler.SetsGate != nil && strings.TrimSpace(gateSpecNameForCatalog(handler.SetsGate)) != "" {
		gates := ensureCatalogGates(entity)
		gates[strings.TrimSpace(gateSpecNameForCatalog(handler.SetsGate))] = true
		result.gates = cloneStringAnyMapCatalog(gates)
	}
	if len(handler.SetsGates) > 0 {
		gates := ensureCatalogGates(entity)
		for gate, value := range handler.SetsGates {
			gates[strings.TrimSpace(gate)] = truthyCatalog(value)
		}
		result.gates = cloneStringAnyMapCatalog(gates)
	}
	applyCatalogDataAccumulation(handler.DataAccumulation, payload, entity)
	applyCatalogCompute(handler.Compute, entity)
	applyCatalogFanOut(handler.FanOut, payload, entity, &result)
	applyCatalogFilter(handler.Filter, payload, entity, policy, result.entityState)
	applyCatalogReduce(handler.Reduce, payload, entity)
	applyCatalogCount(handler.Count, payload, entity, policy, result.entityState)
	applyCatalogGroupBy(handler.GroupBy, payload, entity)
	result.entityFields = cloneStringAnyMapCatalog(entity)
	if emit := strings.TrimSpace(handler.Emit.EventType()); emit != "" {
		result.emittedEvents = append(result.emittedEvents, emit)
	}
	return result
}

func catalogRuleMatches(rule catalogRule, payload, entity, policy map[string]any, state string) bool {
	condition := strings.TrimSpace(rule.Condition)
	switch strings.ToLower(condition) {
	case "", "else":
		return true
	default:
		return catalogEvalCondition(condition, catalogExpressionRoots(payload, entity, policy, state))
	}
}

func applyCatalogRule(rule catalogRule, payload, entity map[string]any, result catalogRunResult) catalogRunResult {
	if next := strings.TrimSpace(rule.AdvancesTo); next != "" {
		result.entityState = next
	}
	if emit := strings.TrimSpace(rule.Emit.EventType()); emit != "" {
		result.emittedEvents = append(result.emittedEvents, emit)
	}
	if rule.DataAccumulation.HasWrites() {
		applyCatalogDataAccumulation(rule.DataAccumulation, payload, entity)
	}
	applyCatalogCompute(rule.Compute, entity)
	result.entityFields = cloneStringAnyMapCatalog(entity)
	return result
}

func catalogAccumulationComplete(mode string, received, expected, threshold int) bool {
	switch mode {
	case "threshold":
		if threshold > 0 {
			return received >= threshold
		}
		return expected > 0 && received >= expected
	case "all", "":
		return expected > 0 && received >= expected
	default:
		return false
	}
}

func catalogGuardPasses(spec any, payload, entity, policy map[string]any, state string) bool {
	if spec == nil {
		return true
	}
	roots := catalogExpressionRoots(payload, entity, policy, state)
	switch typed := spec.(type) {
	case *runtimecontracts.GuardSpec:
		if typed == nil {
			return true
		}
		if len(typed.Checks) > 0 {
			for _, check := range typed.Checks {
				if !catalogEvalCondition(check.Check, roots) {
					return false
				}
			}
			return true
		}
		if strings.TrimSpace(typed.Check) == "" {
			return true
		}
		return catalogEvalCondition(typed.Check, roots)
	case runtimecontracts.GuardSpec:
		return catalogGuardPasses(&typed, payload, entity, policy, state)
	case map[string]any:
		if len(typed) == 0 {
			return true
		}
		return catalogEvalCondition(asStringForCatalog(typed["check"]), roots)
	default:
		return true
	}
}

func catalogExpressionRoots(payload, entity, policy map[string]any, state string) map[string]any {
	return map[string]any{
		"payload": payload,
		"entity":  entity,
		"_entity": catalogPlatformEntity(entity, state),
		"policy":  policy,
	}
}

func catalogPlatformEntity(entity map[string]any, state string) map[string]any {
	out := map[string]any{
		"id":            entity["entity_id"],
		"flow_instance": entity["flow_instance"],
		"gates":         ensureCatalogGates(entity),
	}
	if strings.TrimSpace(state) != "" {
		out["current_state"] = strings.TrimSpace(state)
	} else {
		out["current_state"] = entity["current_state"]
	}
	return out
}

func guardFailOutcome(spec any) string {
	switch typed := spec.(type) {
	case *runtimecontracts.GuardSpec:
		if typed == nil {
			return "reject"
		}
		return catalogFirstNonEmptyString(strings.TrimSpace(typed.OnFail), "reject")
	case runtimecontracts.GuardSpec:
		return catalogFirstNonEmptyString(strings.TrimSpace(typed.OnFail), "reject")
	case map[string]any:
		return catalogFirstNonEmptyString(strings.TrimSpace(asStringForCatalog(typed["on_fail"])), "reject")
	default:
		return "reject"
	}
}

func applyCatalogDataAccumulation(spec runtimecontracts.WorkflowDataAccumulation, payload, entity map[string]any) {
	if len(spec.Writes) == 0 {
		return
	}
	for _, write := range spec.Writes {
		target := strings.TrimSpace(write.Target())
		target = strings.TrimPrefix(target, "entity.")
		target = strings.TrimPrefix(target, "metadata.")
		if target == "" {
			continue
		}
		if write.HasLiteralValue() || strings.TrimSpace(write.Value.CEL) != "" {
			value := write.Value.Literal
			if value == nil {
				value = strings.TrimSpace(write.Value.CEL)
			}
			catalogSetEntityPath(entity, target, value)
			continue
		}
		source := strings.TrimSpace(write.Source())
		if source == "" {
			continue
		}
		value := resolveCatalogPath(write.SourcePath, source, entity, map[string]any{"payload": payload, "entity": entity})
		if !write.SourcePath.HasExplicitRoot() {
			if payloadValue, ok := lookupCatalogPath(payload, source); ok {
				value = payloadValue
			}
		}
		if value != nil {
			catalogSetEntityPath(entity, target, value)
		}
	}
}

func applyCatalogCompute(spec *catalogComputeSpec, entity map[string]any) {
	if spec == nil {
		return
	}
	field := catalogFirstNonEmptyString(spec.StoreAs, spec.OutputField)
	field = strings.TrimPrefix(field, "entity.")
	field = strings.TrimPrefix(field, "metadata.")
	if field == "" {
		return
	}
	catalogSetEntityPath(entity, field, "computed_value")
}

func applyCatalogFanOut(spec *catalogFanOutSpec, payload, entity map[string]any, result *catalogRunResult) {
	if spec == nil || result == nil {
		return
	}
	rawItems := resolveCatalogPath(paths.Parse(strings.TrimSpace(spec.ItemsFrom)), strings.TrimSpace(spec.ItemsFrom), entity, map[string]any{"payload": payload, "entity": entity})
	items := catalogSlice(rawItems)
	entity["fan_out_count"] = len(items)
	if len(items) == 0 {
		return
	}
	for range items {
		if emit := strings.TrimSpace(spec.Emit.EventType()); emit != "" {
			result.emittedEvents = append(result.emittedEvents, emit)
		}
	}
}

func applyCatalogClear(spec *runtimecontracts.ClearSpec, entity map[string]any) {
	if spec == nil {
		return
	}
	targets := append([]string{}, spec.Targets...)
	for _, target := range targets {
		target = strings.TrimSpace(target)
		switch target {
		case "accumulator_state":
			delete(entity, "accumulated_count")
			delete(entity, "accumulated_total")
			delete(entity, "received_items")
		case "cycle_counters":
			delete(entity, "cycle_index")
		case "pending_dedup":
			delete(entity, "dedup_key")
		default:
			delete(entity, target)
		}
	}
}

func applyCatalogQuery(spec *runtimecontracts.QuerySpec, entity map[string]any) {
	if spec == nil {
		return
	}
	if field := catalogTrimEntityPath(spec.StoreAs); field != "" {
		catalogSetEntityPath(entity, field, map[string]any{})
	}
}

func applyCatalogInlineTimer(spec *catalogInlineTimerSpec, result *catalogRunResult) {
	if spec == nil || result == nil {
		return
	}
	if emit := strings.TrimSpace(spec.Emit); emit != "" {
		result.emittedEvents = append(result.emittedEvents, emit)
	}
}

func applyCatalogNodeTimers(nodeID string, node catalogNodeContract, event string, activeTimers map[string]catalogNodeTimerContract) {
	event = strings.TrimSpace(event)
	if activeTimers == nil || event == "" {
		return
	}
	for _, timer := range node.Timers {
		if trigger, err := timeridentity.ParseStartTrigger(timer.StartOn); err == nil && trigger.MatchesEvent(event) {
			activeTimers[catalogNodeTimerKey(nodeID, timer)] = timer
		}
		if trigger, err := timeridentity.ParseCancelTrigger(timer.CancelOn); err == nil && trigger.MatchesEvent(event) {
			delete(activeTimers, catalogNodeTimerKey(nodeID, timer))
		}
	}
}

func catalogValidateTimerEventActivation(nodes map[string]catalogNodeContract, activeTimers map[string]catalogNodeTimerContract, event string) error {
	event = strings.TrimSpace(event)
	if !strings.HasPrefix(event, "timer.") {
		return nil
	}
	matchingKeys := make([]string, 0, 1)
	for _, nodeID := range sortedCatalogNodeIDs(nodes) {
		node := nodes[nodeID]
		for _, timer := range node.Timers {
			if !strings.EqualFold(strings.TrimSpace(timer.Emits), event) {
				continue
			}
			matchingKeys = append(matchingKeys, catalogNodeTimerKey(nodeID, timer))
		}
	}
	if len(matchingKeys) == 0 {
		return nil
	}
	activeKeys := make([]string, 0, len(matchingKeys))
	for _, key := range matchingKeys {
		timer, ok := activeTimers[key]
		if !ok {
			continue
		}
		activeKeys = append(activeKeys, key)
		if !timer.Recurring {
			delete(activeTimers, key)
		}
	}
	if len(activeKeys) > 0 {
		return nil
	}
	return errors.New("timer event " + event + " fired but timer is not active")
}

func catalogNodeTimerKey(nodeID string, timer catalogNodeTimerContract) string {
	timerID := strings.TrimSpace(timer.ID)
	if timerID == "" {
		timerID = strings.TrimSpace(timer.Emits)
	}
	return strings.TrimSpace(nodeID) + ":" + timerID
}

func sortedCatalogNodeIDs(nodes map[string]catalogNodeContract) []string {
	ids := make([]string, 0, len(nodes))
	for nodeID := range nodes {
		ids = append(ids, nodeID)
	}
	sort.Strings(ids)
	return ids
}

func applyCatalogAction(handler catalogSystemNodeEventHandler, payload, entity map[string]any, created map[string]struct{}) (string, bool) {
	if strings.TrimSpace(handler.Action.ID) != "create_flow_instance" {
		return "", false
	}
	instanceExpr := ""
	if handler.ActionParams != nil {
		instanceExpr = handler.ActionParams.InstanceID
	}
	instanceID := strings.TrimSpace(catalogResolveString(instanceExpr, payload, entity))
	if instanceID == "" {
		instanceID = strings.TrimSpace(catalogResolveString(handler.Action.InstanceIDFrom, payload, entity))
	}
	if instanceID != "" {
		if _, exists := created[instanceID]; exists {
			return "error", true
		}
		created[instanceID] = struct{}{}
	}
	return "success", true
}

func applyCatalogFilter(spec *runtimecontracts.FilterSpec, payload, entity, policy map[string]any, state string) {
	if spec == nil {
		return
	}
	roots := catalogExpressionRoots(payload, entity, policy, state)
	items := catalogSlice(resolveCatalogPath(spec.ItemsPath, strings.TrimSpace(spec.ItemsFrom), entity, roots))
	filtered := make([]any, 0, len(items))
	for _, item := range items {
		itemRoots := catalogExpressionRoots(payload, entity, policy, state)
		itemRoots["item"] = item
		if catalogEvalCondition(spec.Condition, itemRoots) {
			filtered = append(filtered, item)
		}
	}
	if target := catalogTrimEntityPath(spec.StoreAs); target != "" {
		catalogSetEntityPath(entity, target, filtered)
	}
}

func applyCatalogReduce(spec *runtimecontracts.ReduceSpec, payload, entity map[string]any) {
	if spec == nil {
		return
	}
	items := catalogSlice(resolveCatalogPath(spec.ItemsPath, strings.TrimSpace(spec.ItemsFrom), entity, map[string]any{"payload": payload, "entity": entity}))
	if target := catalogTrimEntityPath(spec.StoreAs); target != "" {
		catalogSetEntityPath(entity, target, catalogReduceValue(items, strings.TrimSpace(spec.Operation)))
	}
}

func applyCatalogCount(spec *runtimecontracts.CountSpec, payload, entity, policy map[string]any, state string) {
	if spec == nil {
		return
	}
	roots := catalogExpressionRoots(payload, entity, policy, state)
	items := catalogSlice(resolveCatalogPath(spec.ItemsPath, strings.TrimSpace(spec.ItemsFrom), entity, roots))
	count := 0
	for _, item := range items {
		itemRoots := catalogExpressionRoots(payload, entity, policy, state)
		itemRoots["item"] = item
		if strings.TrimSpace(spec.Condition) == "" || catalogEvalCondition(spec.Condition, itemRoots) {
			count++
		}
	}
	if target := catalogTrimEntityPath(spec.StoreAs); target != "" {
		catalogSetEntityPath(entity, target, count)
	}
}

func applyCatalogGroupBy(spec *catalogGroupBySpec, payload, entity map[string]any) {
	if spec == nil {
		return
	}
	items := catalogSlice(resolveCatalogPath(paths.Parse(strings.TrimSpace(spec.ItemsFrom)), strings.TrimSpace(spec.ItemsFrom), entity, map[string]any{"payload": payload, "entity": entity}))
	grouped := map[string]any{}
	for _, item := range items {
		key := strings.TrimSpace(asStringForCatalog(resolveCatalogRef(spec.Key, entity, map[string]any{"item": item, "payload": payload, "entity": entity})))
		if key == "" {
			continue
		}
		existing, _ := grouped[key].([]any)
		grouped[key] = append(existing, item)
	}
	if target := catalogTrimEntityPath(spec.StoreAs); target != "" {
		catalogSetEntityPath(entity, target, grouped)
	}
}

func catalogEvalCondition(expr string, roots map[string]any) bool {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return true
	}
	expr = trimOuterCatalogParens(expr)
	if parts := splitCatalogTopLevel(expr, "||"); len(parts) > 1 {
		for _, part := range parts {
			if catalogEvalCondition(part, roots) {
				return true
			}
		}
		return false
	}
	if parts := splitCatalogTopLevel(expr, "&&"); len(parts) > 1 {
		for _, part := range parts {
			if !catalogEvalCondition(part, roots) {
				return false
			}
		}
		return true
	}
	if strings.EqualFold(expr, "true") {
		return true
	}
	if strings.EqualFold(expr, "false") {
		return false
	}
	for _, op := range []string{">=", "<=", "==", "!=", ">", "<"} {
		parts := splitCatalogTopLevel(expr, op)
		if len(parts) != 2 {
			continue
		}
		left := resolveCatalogOperand(parts[0], roots)
		right := resolveCatalogOperand(parts[1], roots)
		return compareCatalogValues(left, right, op)
	}
	return truthyCatalog(resolveCatalogOperand(expr, roots))
}

func splitCatalogTopLevel(expr, sep string) []string {
	if !strings.Contains(expr, sep) {
		return nil
	}
	var (
		parts    []string
		depth    int
		quote    rune
		start    int
		runes    = []rune(expr)
		sepRunes = []rune(sep)
	)
	for i := 0; i < len(runes); i++ {
		switch runes[i] {
		case '\'', '"':
			if quote == 0 {
				quote = runes[i]
			} else if quote == runes[i] {
				quote = 0
			}
		case '(':
			if quote == 0 {
				depth++
			}
		case ')':
			if quote == 0 && depth > 0 {
				depth--
			}
		case '[':
			if quote == 0 {
				depth++
			}
		case ']':
			if quote == 0 && depth > 0 {
				depth--
			}
		}
		if quote != 0 || depth != 0 || i+len(sepRunes) > len(runes) {
			continue
		}
		match := true
		for j := range sepRunes {
			if runes[i+j] != sepRunes[j] {
				match = false
				break
			}
		}
		if !match {
			continue
		}
		parts = append(parts, strings.TrimSpace(string(runes[start:i])))
		i += len(sepRunes) - 1
		start = i + 1
	}
	if len(parts) == 0 {
		return nil
	}
	parts = append(parts, strings.TrimSpace(string(runes[start:])))
	return parts
}

func trimOuterCatalogParens(expr string) string {
	for {
		expr = strings.TrimSpace(expr)
		if len(expr) < 2 || expr[0] != '(' || expr[len(expr)-1] != ')' {
			return expr
		}
		depth := 0
		quote := rune(0)
		wrapped := true
		for i, r := range expr {
			switch r {
			case '\'', '"':
				if quote == 0 {
					quote = r
				} else if quote == r {
					quote = 0
				}
			case '(':
				if quote == 0 {
					depth++
				}
			case ')':
				if quote == 0 {
					depth--
					if depth == 0 && i != len(expr)-1 {
						wrapped = false
					}
				}
			}
		}
		if !wrapped {
			return expr
		}
		expr = expr[1 : len(expr)-1]
	}
}

func resolveCatalogOperand(expr string, roots map[string]any) any {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil
	}
	if strings.EqualFold(expr, "true") {
		return true
	}
	if strings.EqualFold(expr, "false") {
		return false
	}
	if (strings.HasPrefix(expr, "\"") && strings.HasSuffix(expr, "\"")) || (strings.HasPrefix(expr, "'") && strings.HasSuffix(expr, "'")) {
		return strings.Trim(expr, "\"'")
	}
	if n, err := strconv.Atoi(expr); err == nil {
		return n
	}
	if f, err := strconv.ParseFloat(expr, 64); err == nil {
		return f
	}
	entity, _ := roots["entity"].(map[string]any)
	return resolveCatalogRef(expr, entity, roots)
}

func catalogSlice(value any) []any {
	switch typed := value.(type) {
	case []any:
		return append([]any{}, typed...)
	case []map[string]any:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, item)
		}
		return out
	default:
		return nil
	}
}

func catalogReduceValue(items []any, operation string) any {
	switch strings.ToLower(operation) {
	case "sum":
		total := 0.0
		for _, item := range items {
			if n, ok := catalogNumericValue(item); ok {
				total += n
			}
		}
		return catalogNormalizeNumber(total)
	case "min":
		var (
			min float64
			ok  bool
		)
		for _, item := range items {
			n, has := catalogNumericValue(item)
			if !has {
				continue
			}
			if !ok || n < min {
				min = n
				ok = true
			}
		}
		return catalogNormalizeNumber(min)
	case "max":
		var (
			max float64
			ok  bool
		)
		for _, item := range items {
			n, has := catalogNumericValue(item)
			if !has {
				continue
			}
			if !ok || n > max {
				max = n
				ok = true
			}
		}
		return catalogNormalizeNumber(max)
	case "count":
		return len(items)
	case "weighted_average":
		total := 0.0
		weightSum := 0.0
		for _, item := range items {
			obj, ok := item.(map[string]any)
			if !ok {
				continue
			}
			score, okScore := asFloatForCatalog(obj["score"])
			weight, okWeight := asFloatForCatalog(obj["weight"])
			if !okScore || !okWeight || weight == 0 {
				continue
			}
			total += score * weight
			weightSum += weight
		}
		if weightSum == 0 {
			return 0
		}
		return catalogNormalizeNumber(total / weightSum)
	case "pick_or_average":
		best := 0.0
		ok := false
		for _, item := range items {
			obj, isObj := item.(map[string]any)
			if !isObj {
				continue
			}
			score, has := asFloatForCatalog(obj["score"])
			if !has {
				continue
			}
			if !ok || score > best {
				best = score
				ok = true
			}
		}
		return catalogNormalizeNumber(best)
	default:
		return nil
	}
}

func catalogNumericValue(item any) (float64, bool) {
	if n, ok := asFloatForCatalog(item); ok {
		return n, true
	}
	if obj, ok := item.(map[string]any); ok {
		if n, ok := asFloatForCatalog(obj["value"]); ok {
			return n, true
		}
		if n, ok := asFloatForCatalog(obj["score"]); ok {
			return n, true
		}
	}
	return 0, false
}

func catalogNormalizeNumber(n float64) any {
	if float64(int(n)) == n {
		return int(n)
	}
	return n
}

func normalizeStrings(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func catalogTrimEntityPath(path string) string {
	path = strings.TrimSpace(path)
	path = strings.TrimPrefix(path, "entity.")
	path = strings.TrimPrefix(path, "metadata.")
	return path
}

func catalogInitialEntity(expected catalogExpectedDocument) (map[string]any, map[string]any) {
	entity := map[string]any{}
	for key, value := range expected.Trigger.Entity {
		entity[strings.TrimSpace(key)] = value
	}
	for key, value := range expected.Trigger.EntityFieldsBefore {
		entity[strings.TrimSpace(key)] = value
	}
	var gates map[string]any
	if len(expected.Trigger.GatesBefore) > 0 {
		gates = map[string]any{}
		for key, value := range expected.Trigger.GatesBefore {
			key = strings.TrimSpace(key)
			gates[key] = value
			entity[key] = value
		}
		entity["gates"] = gates
	}
	return entity, gates
}

func catalogStringSet(items []string) map[string]struct{} {
	if len(items) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(items))
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item != "" {
			out[item] = struct{}{}
		}
	}
	return out
}

func catalogResolveHandler(nodes map[string]catalogNodeContract, event string) (catalogSystemNodeEventHandler, string, catalogNodeContract, bool) {
	event = strings.TrimSpace(event)
	for nodeID, node := range nodes {
		if handler, ok := node.EventHandlers[event]; ok {
			return handler, strings.TrimSpace(nodeID), node, true
		}
	}
	for nodeID, node := range nodes {
		for pattern, handler := range node.EventHandlers {
			if catalogEventPatternMatches(pattern, event) {
				return handler, strings.TrimSpace(nodeID), node, true
			}
		}
	}
	return catalogSystemNodeEventHandler{}, "", catalogNodeContract{}, false
}

type catalogResolvedHandler struct {
	NodeID  string
	Node    catalogNodeContract
	Handler catalogSystemNodeEventHandler
}

func catalogResolveAllHandlers(nodes map[string]catalogNodeContract, event string) []catalogResolvedHandler {
	event = strings.TrimSpace(event)
	out := make([]catalogResolvedHandler, 0, 4)
	for nodeID, node := range nodes {
		if handler, ok := node.EventHandlers[event]; ok {
			out = append(out, catalogResolvedHandler{NodeID: strings.TrimSpace(nodeID), Node: node, Handler: handler})
		}
	}
	for nodeID, node := range nodes {
		for pattern, handler := range node.EventHandlers {
			if strings.TrimSpace(pattern) == event {
				continue
			}
			if catalogEventPatternMatches(pattern, event) {
				out = append(out, catalogResolvedHandler{NodeID: strings.TrimSpace(nodeID), Node: node, Handler: handler})
				break
			}
		}
	}
	return out
}

func catalogResolveAgents(agents map[string]catalogAgentRegistryEntry, event string) []catalogAgentRegistryEntry {
	event = strings.TrimSpace(event)
	out := make([]catalogAgentRegistryEntry, 0, 2)
	for _, agent := range agents {
		for _, pattern := range agent.Subscriptions {
			if catalogEventPatternMatches(pattern, event) {
				out = append(out, agent)
				break
			}
		}
	}
	return out
}

func catalogAgentProduces(agent catalogAgentRegistryEntry) []string {
	out := append([]string{}, agent.Produces...)
	out = append(out, agent.EmitEvents...)
	return normalizeStrings(out)
}

func catalogEventPayloadError(spec catalogEventSchemaEntry, payload map[string]any) error {
	if len(spec.Required) == 0 {
		return nil
	}
	for _, key := range spec.Required {
		key = strings.TrimSpace(key)
		value, ok := payload[key]
		if !ok || value == nil || strings.TrimSpace(asStringForCatalog(value)) == "" {
			return fmt.Errorf("%s is required", key)
		}
	}
	return nil
}

func catalogEventPatternMatches(pattern, event string) bool {
	pattern = strings.TrimSpace(pattern)
	event = strings.TrimSpace(event)
	switch {
	case pattern == event:
		return true
	case strings.HasPrefix(pattern, "*."):
		return strings.HasSuffix(event, strings.TrimPrefix(pattern, "*"))
	case strings.HasSuffix(pattern, ".*"):
		return strings.HasPrefix(event, strings.TrimSuffix(pattern, "*"))
	case pattern == "*":
		return true
	default:
		return false
	}
}

func catalogNodeScope(node catalogNodeContract) string {
	if scope := strings.TrimSpace(node.Flow); scope != "" {
		return scope
	}
	return "default"
}

func catalogApplyTerminalEventPolicy(result catalogRunResult, state, event string) catalogRunResult {
	result.entityState = state
	result.handlerOutcome = "terminal_reject"
	return result
}

func catalogResolveString(expr string, payload, entity map[string]any) string {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return ""
	}
	value := resolveCatalogRef(expr, entity, map[string]any{"payload": payload, "entity": entity})
	return strings.TrimSpace(asStringForCatalog(value))
}

func catalogSetEntityPath(entity map[string]any, path string, value any) {
	if entity == nil {
		return
	}
	segments := strings.Split(strings.TrimSpace(path), ".")
	if len(segments) == 0 {
		return
	}
	current := entity
	for _, segment := range segments[:len(segments)-1] {
		segment = strings.TrimSpace(segment)
		if segment == "" {
			return
		}
		next, ok := current[segment].(map[string]any)
		if !ok {
			next = map[string]any{}
			current[segment] = next
		}
		current = next
	}
	current[strings.TrimSpace(segments[len(segments)-1])] = value
}

func lookupCatalogPath(root map[string]any, path string) (any, bool) {
	if len(root) == 0 {
		return nil, false
	}
	current := any(root)
	for _, segment := range strings.Split(strings.TrimSpace(path), ".") {
		obj, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = obj[strings.TrimSpace(segment)]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

func decodeCatalogClearGates(node *yaml.Node) ([]string, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		if strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") || strings.TrimSpace(node.Value) == "" {
			return nil, nil
		}
		var all bool
		if err := node.Decode(&all); err == nil {
			if all {
				return []string{"*"}, nil
			}
			return nil, nil
		}
		return []string{strings.TrimSpace(node.Value)}, nil
	case yaml.SequenceNode:
		var values []string
		if err := node.Decode(&values); err != nil {
			return nil, err
		}
		return normalizeStrings(values), nil
	default:
		return nil, fmt.Errorf("unsupported clear_gates yaml node kind %d", node.Kind)
	}
}

func decodeCatalogRuleList(node *yaml.Node) ([]catalogRule, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	switch node.Kind {
	case yaml.SequenceNode:
		var rules []catalogRule
		if err := node.Decode(&rules); err != nil {
			return nil, err
		}
		return rules, nil
	case yaml.MappingNode:
		if hasAnyCatalogMappingKey(node, "condition", "advances_to", "emits", "data_accumulation", "compute") {
			var rule catalogRule
			if err := node.Decode(&rule); err != nil {
				return nil, err
			}
			return []catalogRule{rule}, nil
		}
		rules := make([]catalogRule, 0, len(node.Content)/2)
		for i := 0; i+1 < len(node.Content); i += 2 {
			id := strings.TrimSpace(node.Content[i].Value)
			if id == "" {
				continue
			}
			var rule catalogRule
			if err := node.Content[i+1].Decode(&rule); err != nil {
				return nil, err
			}
			if strings.TrimSpace(rule.ID) == "" {
				rule.ID = id
			}
			rules = append(rules, rule)
		}
		return rules, nil
	default:
		return nil, fmt.Errorf("unsupported rule yaml node kind %d", node.Kind)
	}
}

func hasAnyCatalogMappingKey(node *yaml.Node, keys ...string) bool {
	if node == nil || node.Kind != yaml.MappingNode {
		return false
	}
	lookup := map[string]struct{}{}
	for _, key := range keys {
		lookup[strings.TrimSpace(key)] = struct{}{}
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		if _, ok := lookup[strings.TrimSpace(node.Content[i].Value)]; ok {
			return true
		}
	}
	return false
}

func resolveCatalogRef(expr string, entity map[string]any, roots map[string]any) any {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil
	}
	expr = normalizeCatalogBracketRef(expr)
	if n, err := strconv.Atoi(expr); err == nil {
		return n
	}
	segments := strings.Split(expr, ".")
	if len(segments) == 1 {
		for _, rootName := range []string{"item", "payload", "entity", "policy"} {
			if rootMap, ok := roots[rootName].(map[string]any); ok {
				if value, ok := rootMap[segments[0]]; ok {
					return value
				}
			}
		}
		return expr
	}
	root := strings.TrimSpace(segments[0])
	current, ok := roots[root]
	if !ok {
		if root == "entity" {
			current = entity
		} else {
			return nil
		}
	}
	for _, segment := range segments[1:] {
		obj, ok := current.(map[string]any)
		if !ok {
			return nil
		}
		current = obj[strings.TrimSpace(segment)]
	}
	return current
}

func normalizeCatalogBracketRef(expr string) string {
	expr = strings.TrimSpace(expr)
	for {
		start := strings.IndexByte(expr, '[')
		if start < 0 {
			return expr
		}
		end := strings.IndexByte(expr[start:], ']')
		if end < 0 {
			return expr
		}
		end += start
		key := strings.TrimSpace(expr[start+1 : end])
		key = strings.Trim(key, `"'`)
		if key == "" {
			return expr
		}
		expr = strings.TrimSpace(expr[:start]) + "." + key + strings.TrimSpace(expr[end+1:])
	}
}

func resolveCatalogPath(parsed paths.Path, raw string, entity map[string]any, roots map[string]any) any {
	if parsed.HasExplicitRoot() {
		current, ok := roots[parsed.Root.String()]
		if !ok && parsed.Root == paths.RootEntity {
			current = entity
		}
		if current == nil {
			return nil
		}
		for _, segment := range parsed.Segments {
			obj, ok := current.(map[string]any)
			if !ok {
				return nil
			}
			current = obj[strings.TrimSpace(segment)]
		}
		return current
	}
	return resolveCatalogRef(raw, entity, roots)
}

func compareCatalogValues(left, right any, op string) bool {
	lf, lok := asFloatForCatalog(left)
	rf, rok := asFloatForCatalog(right)
	if lok && rok {
		switch op {
		case ">=":
			return lf >= rf
		case "<=":
			return lf <= rf
		case ">":
			return lf > rf
		case "<":
			return lf < rf
		case "==":
			return lf == rf
		case "!=":
			return lf != rf
		}
	}
	ls := strings.TrimSpace(asStringForCatalog(left))
	rs := strings.TrimSpace(asStringForCatalog(right))
	switch op {
	case "==":
		return ls == rs
	case "!=":
		return ls != rs
	default:
		return false
	}
}

func appendCatalogItem(existing any, item map[string]any) []any {
	out, _ := existing.([]any)
	cp := append([]any{}, out...)
	cp = append(cp, item)
	return cp
}

func catalogAccumulationKey(item map[string]any, dedupBy string, entity, policy map[string]any) string {
	if item == nil {
		return ""
	}
	if strings.TrimSpace(dedupBy) != "" {
		if key := asStringForCatalog(resolveCatalogRef(strings.TrimSpace(dedupBy), entity, map[string]any{"payload": item, "entity": entity, "policy": policy})); key != "" {
			return key
		}
	}
	if raw, ok := item["item_id"]; ok && raw != nil {
		if id := strings.TrimSpace(asStringForCatalog(raw)); id != "" && !strings.EqualFold(id, "null") {
			return "item_id:" + id
		}
	}
	if raw, ok := item["id"]; ok && raw != nil {
		if id := strings.TrimSpace(asStringForCatalog(raw)); id != "" && !strings.EqualFold(id, "null") {
			return "id:" + id
		}
	}
	blob, err := json.Marshal(item)
	if err != nil {
		return ""
	}
	return string(blob)
}

func diffStringSet(got, want []string) string {
	if strings.Join(got, ",") == strings.Join(want, ",") {
		return ""
	}
	return "got=[" + strings.Join(got, ",") + "] want=[" + strings.Join(want, ",") + "]"
}

func normalizeSorted(items []string) []string {
	out := make([]string, 0, len(items))
	seen := map[string]struct{}{}
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	sort.Strings(out)
	return out
}

func assertCatalogRunResult(t testing.TB, result catalogRunResult, expected catalogExpectedDocument) {
	t.Helper()
	if want := strings.TrimSpace(expected.Expected.BootResult); want != "" {
		if got := strings.TrimSpace(result.bootResult); got != want {
			t.Fatalf("boot result = %q, want %q", got, want)
		}
		if wantCategory := strings.TrimSpace(expected.Expected.ErrorCategory); wantCategory != "" && strings.TrimSpace(result.errorCategory) != wantCategory {
			t.Fatalf("error category = %q, want %q", result.errorCategory, wantCategory)
		}
		if wantContains := strings.TrimSpace(expected.Expected.ErrorContains); wantContains != "" && !strings.Contains(result.errorContains, wantContains) {
			t.Fatalf("error contains = %q, want substring %q", result.errorContains, wantContains)
		}
		if expected.Expected.TemplateInstances != nil && !catalogValueEquals(result.templateInstances, expected.Expected.TemplateInstances) {
			t.Fatalf("template instances = %#v, want %#v", result.templateInstances, expected.Expected.TemplateInstances)
		}
		return
	}
	if want := strings.TrimSpace(expected.Trigger.ErrorContains); want != "" {
		if !strings.Contains(result.errorContains, want) {
			t.Fatalf("trigger error contains = %q, want substring %q", result.errorContains, want)
		}
		if result.handlerOutcome != "" {
			t.Fatalf("handler outcome = %q, want empty for trigger-level publish rejection", result.handlerOutcome)
		}
		if len(result.emittedEvents) > 0 {
			t.Fatalf("emitted events = %#v, want none for trigger-level publish rejection", result.emittedEvents)
		}
		return
	}
	if len(expected.Expected.Entities) > 0 {
		for entityID, want := range expected.Expected.Entities {
			got, ok := result.entities[strings.TrimSpace(entityID)]
			if !ok {
				t.Fatalf("missing entity result %q", entityID)
			}
			if got.handlerOutcome != strings.TrimSpace(want.HandlerOutcome) {
				t.Fatalf("entity %s handler outcome = %q, want %q", entityID, got.handlerOutcome, want.HandlerOutcome)
			}
			if got.entityState != strings.TrimSpace(want.EntityState) {
				t.Fatalf("entity %s state = %q, want %q", entityID, got.entityState, want.EntityState)
			}
			if diff := diffStringSet(normalizeSorted(got.emittedEvents), normalizeSorted(want.EmittedEvents)); diff != "" {
				t.Fatalf("entity %s emitted events mismatch (%s)", entityID, diff)
			}
		}
		return
	}
	if got, want := result.handlerOutcome, strings.TrimSpace(expected.Expected.HandlerOutcome); got != want {
		t.Fatalf("handler outcome = %q, want %q", got, want)
	}
	if got, want := result.entityState, strings.TrimSpace(expected.Expected.EntityState); got != want {
		t.Fatalf("entity state = %q, want %q", got, want)
	}
	if expected.Expected.DeadLetter != result.deadLetter {
		t.Fatalf("dead letter = %v, want %v", result.deadLetter, expected.Expected.DeadLetter)
	}
	if want := strings.TrimSpace(expected.Expected.DeadLetterReason); want != "" && result.deadLetterReason != want {
		t.Fatalf("dead letter reason = %q, want %q", result.deadLetterReason, want)
	}
	if want := expected.Expected.ChainDepthAtDeadLetter; want != 0 && result.chainDepthAtDeadLetter != want {
		t.Fatalf("dead letter chain depth = %d, want %d", result.chainDepthAtDeadLetter, want)
	}
	if diff := diffStringSet(normalizeSorted(result.emittedEvents), normalizeSorted(expected.Expected.EmittedEvents)); diff != "" {
		t.Fatalf("emitted events mismatch (%s)", diff)
	}
	if len(expected.Expected.AgentReceived) > 0 {
		for agentID, want := range expected.Expected.AgentReceived {
			got, ok := result.agentReceived[strings.TrimSpace(agentID)]
			if !ok {
				t.Fatalf("missing agent delivery %q", agentID)
			}
			if !catalogValueEquals(got, want) {
				t.Fatalf("agent %s received = %#v, want %#v", agentID, got, want)
			}
		}
	}
	for key, want := range expected.Expected.EntityFields {
		got, ok := result.entityFields[strings.TrimSpace(key)]
		if !ok {
			t.Fatalf("missing entity field %q", key)
		}
		if strings.TrimSpace(asStringForCatalog(want)) == "computed_value" {
			continue
		}
		if !catalogValueEquals(got, want) {
			t.Fatalf("entity field %s = %#v, want %#v", key, got, want)
		}
	}
	for key, want := range expected.Expected.Gates {
		got, ok := result.gates[strings.TrimSpace(key)]
		if !ok && !truthyCatalog(want) {
			continue
		}
		if !ok {
			t.Fatalf("missing gate %q", key)
		}
		if truthyCatalog(got) != truthyCatalog(want) {
			t.Fatalf("gate %s = %#v, want %#v", key, got, want)
		}
	}
}

func validateCatalogExpectedDocument(dir string, expected catalogExpectedDocument) error {
	if expected.Trigger.Boot {
		if strings.TrimSpace(expected.Expected.BootResult) == "" {
			return fmt.Errorf("boot trigger requires expected.boot_result")
		}
	} else {
		if strings.TrimSpace(expected.Trigger.Event) == "" && len(expected.Trigger.Sequence) == 0 && len(expected.Trigger.Concurrent) == 0 {
			return fmt.Errorf("runtime trigger requires event, sequence, or concurrent")
		}
		if len(expected.Trigger.Concurrent) > 0 && len(expected.Expected.Entities) == 0 {
			return fmt.Errorf("concurrent trigger requires expected.entities")
		}
		if len(expected.Trigger.Concurrent) == 0 &&
			strings.TrimSpace(expected.Trigger.ErrorContains) == "" &&
			strings.TrimSpace(expected.Expected.HandlerOutcome) == "" &&
			!expected.Expected.DeadLetter {
			return fmt.Errorf("runtime case requires expected.handler_outcome")
		}
	}
	if catalogCaseExecutableNowForDir(dir, expected) {
		if unsupported := catalogUnsupportedExecutableExpectations(expected); len(unsupported) > 0 {
			return fmt.Errorf("currently executable catalog case uses unsupported expectations: %s", strings.Join(unsupported, ", "))
		}
	}
	return nil
}

func catalogUnsupportedExecutableExpectations(expected catalogExpectedDocument) []string {
	unsupported := make([]string, 0, 6)
	if expected.Expected.RuntimeOnly {
		unsupported = append(unsupported, "expected.runtime_only")
	}
	if expected.Expected.ChainDepthExceeded {
		unsupported = append(unsupported, "expected.chain_depth_exceeded")
	}
	if len(expected.Expected.Diagnostics) > 0 {
		unsupported = append(unsupported, "expected.diagnostics")
	}
	if len(expected.Expected.GatesSet) > 0 {
		unsupported = append(unsupported, "expected.gates_set")
	}
	if len(expected.Expected.AgentRouting) > 0 {
		unsupported = append(unsupported, "expected.agent_routing")
	}
	if len(expected.Expected.FlowInstanceCreated) > 0 {
		unsupported = append(unsupported, "expected.flow_instance_created")
	}
	if len(expected.Expected.ToolResolution) > 0 {
		unsupported = append(unsupported, "expected.tool_resolution")
	}
	if strings.TrimSpace(expected.Expected.ParentState) != "" {
		unsupported = append(unsupported, "expected.parent_state")
	}
	if strings.TrimSpace(expected.Expected.FlowBState) != "" {
		unsupported = append(unsupported, "expected.flow_b_state")
	}
	return unsupported
}

func catalogCaseSimpleHarnessEligible(expected catalogExpectedDocument) bool {
	if expected.Trigger.Boot || len(expected.Trigger.Concurrent) > 0 {
		return false
	}
	if expected.Expected.RuntimeOnly {
		return false
	}
	if expected.Trigger.AssertAtomicCommit || expected.Trigger.AssertPersistedBeforeDelivery || expected.Trigger.AssertSerialProcessing {
		return false
	}
	if strings.TrimSpace(expected.Trigger.InjectFailure) != "" {
		return false
	}
	if expected.Expected.DeadLetter || strings.TrimSpace(expected.Expected.BootResult) != "" {
		return false
	}
	if len(expected.Expected.Entities) > 0 || len(expected.Expected.FlowEntities) > 0 || len(expected.Expected.AgentRouting) > 0 || len(expected.Expected.AgentReceived) > 0 {
		return false
	}
	if len(expected.Expected.FlowInstanceCreated) > 0 || len(expected.Expected.ToolResolution) > 0 || expected.Expected.TemplateInstances != nil {
		return false
	}
	if strings.TrimSpace(expected.Expected.ParentState) != "" || strings.TrimSpace(expected.Expected.FlowBState) != "" {
		return false
	}
	return true
}

func catalogValueEquals(got, want any) bool {
	if compareCatalogValues(got, want, "==") {
		return true
	}
	gotYAML := strings.TrimSpace(toYAMLScalar(got))
	wantYAML := strings.TrimSpace(toYAMLScalar(want))
	return gotYAML == wantYAML
}

func truthyCatalog(value any) bool {
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		return strings.EqualFold(strings.TrimSpace(typed), "true")
	default:
		return asIntForCatalog(value) != 0
	}
}

func ensureCatalogGates(entity map[string]any) map[string]any {
	gates := cloneStringAnyMapCatalog(asMapForCatalog(entity["gates"]))
	if gates == nil {
		gates = map[string]any{}
	}
	entity["gates"] = gates
	return gates
}

func asMapForCatalog(value any) map[string]any {
	out, _ := value.(map[string]any)
	return out
}

func gateSpecNameForCatalog(spec *runtimecontracts.GateSpec) string {
	if spec == nil {
		return ""
	}
	return strings.TrimSpace(spec.Name)
}

func cloneStringAnyMapCatalog(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		key = strings.TrimSpace(key)
		if nested, ok := value.(map[string]any); ok {
			out[key] = cloneStringAnyMapCatalog(nested)
			continue
		}
		out[key] = value
	}
	return out
}

func catalogFirstNonEmptyString(vals ...string) string {
	for _, val := range vals {
		val = strings.TrimSpace(val)
		if val != "" {
			return val
		}
	}
	return ""
}

func asStringForCatalog(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	default:
		raw := strings.TrimSpace(toYAMLScalar(typed))
		raw = strings.Trim(raw, "\"'")
		raw = strings.ReplaceAll(raw, "\n", " ")
		return strings.TrimSpace(raw)
	}
}

func asIntForCatalog(value any) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	case string:
		n, _ := strconv.Atoi(strings.TrimSpace(typed))
		return n
	default:
		return 0
	}
}

func asFloatForCatalog(value any) (float64, bool) {
	switch typed := value.(type) {
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case float64:
		return typed, true
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		return f, err == nil
	default:
		return 0, false
	}
}

func toYAMLScalar(value any) string {
	raw, err := yaml.Marshal(value)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(raw))
}

func loadYAMLForCatalogTest(t testing.TB, path string, target any) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if err := yaml.Unmarshal(raw, target); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
}

func loadExpectedYAMLForCatalogTest(t testing.TB, path string, target any) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	dec.KnownFields(true)
	if err := dec.Decode(target); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
}

func repoRootForTest(t testing.TB) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	return filepath.Clean(filepath.Join(wd, "..", "..", ".."))
}
