package masflowtest

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	runtimecontracts "empireai/internal/runtime/contracts"
	"empireai/internal/runtime/core/paths"
	"gopkg.in/yaml.v3"
)

type catalogTriggerStep struct {
	Event   string         `yaml:"event"`
	Payload map[string]any `yaml:"payload"`
	Sender  string         `yaml:"sender"`
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
	} `yaml:"trigger"`
	Expected struct {
		BootResult             string                              `yaml:"boot_result"`
		ErrorCategory          string                              `yaml:"error_category"`
		ErrorContains          string                              `yaml:"error_contains"`
		HandlerOutcome         string                              `yaml:"handler_outcome"`
		EntityState            string                              `yaml:"entity_state"`
		EmittedEvents          []string                            `yaml:"emitted_events"`
		EntityFields           map[string]any                      `yaml:"entity_fields"`
		Gates                  map[string]any                      `yaml:"gates"`
		GatesSet               []string                            `yaml:"gates_set"`
		DeadLetter             bool                                `yaml:"dead_letter"`
		DeadLetterReason       string                              `yaml:"dead_letter_reason"`
		ChainDepthAtDeadLetter int                                 `yaml:"chain_depth_at_dead_letter"`
		AgentRouting           map[string]string                   `yaml:"agent_routing"`
		AgentReceived          map[string]any                      `yaml:"agent_received"`
		FlowInstanceCreated    map[string]any                      `yaml:"flow_instance_created"`
		ToolResolution         map[string]any                      `yaml:"tool_resolution"`
		TemplateInstances      any                                 `yaml:"template_instances"`
		Entities               map[string]catalogExpectedPerEntity `yaml:"entities"`
		ParentState            string                              `yaml:"parent_state"`
		FlowBState             string                              `yaml:"flow_b_state"`
	} `yaml:"expected"`
}

type catalogExpectedPerEntity struct {
	HandlerOutcome string   `yaml:"handler_outcome"`
	EntityState    string   `yaml:"entity_state"`
	EmittedEvents  []string `yaml:"emitted_events"`
}

type catalogRunResult struct {
	handlerOutcome string
	entityState    string
	emittedEvents  []string
	entityFields   map[string]any
	gates          map[string]any
}

type catalogNodeContract struct {
	EventHandlers map[string]catalogSystemNodeEventHandler `yaml:"event_handlers"`
}

type catalogSystemNodeEventHandler struct {
	Emits            runtimecontracts.EventEmission            `yaml:"emits"`
	Guard            *runtimecontracts.GuardSpec               `yaml:"guard"`
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
	GroupBy          *catalogGroupBySpec                       `yaml:"group_by"`
}

type catalogClearGates []string

type catalogRule struct {
	ID               string                                    `yaml:"id"`
	Description      string                                    `yaml:"description"`
	Condition        string                                    `yaml:"condition"`
	AdvancesTo       string                                    `yaml:"advances_to"`
	Emits            runtimecontracts.EventEmission            `yaml:"emits"`
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
	ItemsFrom   string                    `yaml:"items_from"`
	Target      string                    `yaml:"target"`
	EmitPerItem string                    `yaml:"emit_per_item"`
	EmitMapping *catalogFanOutEmitMapping `yaml:"emit_mapping"`
}

type catalogFanOutEmitMapping struct {
	KeyField string            `yaml:"key_field"`
	Mapping  map[string]string `yaml:"mapping"`
}

type catalogGroupBySpec struct {
	ItemsFrom string `yaml:"items_from"`
	Key       string `yaml:"key"`
	StoreAs   string `yaml:"store_as"`
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

func TestCatalogRunner_ValidatesCurrentCatalogPackages(t *testing.T) {
	for _, dir := range discoveredCatalogCaseDirs(t) {
		dir := dir
		t.Run(dir, func(t *testing.T) {
			root := filepath.Join(repoRootFromMASTest(t), "tests", filepath.FromSlash(dir))
			var expected catalogExpectedDocument
			loadExpectedYAMLForCatalogTest(t, filepath.Join(root, "expected.yaml"), &expected)
			if err := validateCatalogExpectedDocument(expected); err != nil {
				t.Fatalf("validate expected.yaml: %v", err)
			}
		})
	}
}

func TestCatalogRunner_IdentifiesSimpleHarnessEligiblePackages(t *testing.T) {
	eligible := 0
	for _, dir := range discoveredCatalogCaseDirs(t) {
		var expected catalogExpectedDocument
		root := filepath.Join(repoRootFromMASTest(t), "tests", filepath.FromSlash(dir))
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
	root := filepath.Join(repoRootFromMASTest(t), "tests")
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
		t.Fatalf("walk MAS test catalog root: %v", err)
	}
	sort.Strings(dirs)
	if len(dirs) == 0 {
		t.Fatal("no MAS test catalog packages discovered")
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
	var schema runtimecontracts.FlowSchemaDocument
	loadYAMLForCatalogTest(t, filepath.Join(dir, "schema.yaml"), &schema)
	nodes := map[string]catalogNodeContract{}
	loadYAMLForCatalogTest(t, filepath.Join(dir, "nodes.yaml"), &nodes)
	policy := map[string]any{}
	loadYAMLForCatalogTest(t, filepath.Join(dir, "policy.yaml"), &policy)
	var expected catalogExpectedDocument
	loadExpectedYAMLForCatalogTest(t, filepath.Join(dir, "expected.yaml"), &expected)

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
				if !accumulate.OnTimeout.Emits.Empty() {
					result.emittedEvents = append(result.emittedEvents, accumulate.OnTimeout.Emits.Values()...)
				}
				if accumulate.OnTimeout.DataAccumulation.HasWrites() {
					applyCatalogDataAccumulation(accumulate.OnTimeout.DataAccumulation, step.Payload, entity)
					result.entityFields = cloneStringAnyMapCatalog(entity)
				}
				applyCatalogCompute(accumulate.OnTimeout.Compute, entity)
				result.entityFields = cloneStringAnyMapCatalog(entity)
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

func catalogCaseExecutableNow(t testing.TB, dir string, expected catalogExpectedDocument) bool {
	t.Helper()
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
	case strings.HasPrefix(dir, "tier6-event-loop/"):
		return false
	default:
		return false
	}
}

func executeCatalogHandlerStep(t testing.TB, handler catalogSystemNodeEventHandler, step catalogTriggerStep, entity map[string]any, policy map[string]any, result catalogRunResult) catalogRunResult {
	t.Helper()
	payload := cloneStringAnyMapCatalog(step.Payload)
	result.handlerOutcome = "success"
	if !catalogGuardPasses(handler.Guard, payload, entity, policy) {
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
			if catalogRuleMatches(rule, payload, entity, policy) {
				result = applyCatalogRule(rule, payload, entity, result)
				break
			}
		}
	}
	if len(handler.Rules) > 0 {
		for _, rule := range handler.Rules {
			if catalogRuleMatches(rule, payload, entity, policy) {
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
	applyCatalogFilter(handler.Filter, payload, entity, policy)
	applyCatalogReduce(handler.Reduce, payload, entity)
	applyCatalogCount(handler.Count, payload, entity, policy)
	applyCatalogGroupBy(handler.GroupBy, payload, entity)
	result.entityFields = cloneStringAnyMapCatalog(entity)
	result.emittedEvents = append(result.emittedEvents, handler.Emits.Values()...)
	return result
}

func catalogRuleMatches(rule catalogRule, payload, entity, policy map[string]any) bool {
	condition := strings.TrimSpace(rule.Condition)
	switch strings.ToLower(condition) {
	case "", "else":
		return true
	default:
		return catalogEvalCondition(condition, map[string]any{"payload": payload, "entity": entity, "policy": policy})
	}
}

func applyCatalogRule(rule catalogRule, payload, entity map[string]any, result catalogRunResult) catalogRunResult {
	if next := strings.TrimSpace(rule.AdvancesTo); next != "" {
		result.entityState = next
	}
	if !rule.Emits.Empty() {
		result.emittedEvents = append(result.emittedEvents, rule.Emits.Values()...)
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

func catalogGuardPasses(spec any, payload, entity, policy map[string]any) bool {
	if spec == nil {
		return true
	}
	switch typed := spec.(type) {
	case *runtimecontracts.GuardSpec:
		if typed == nil {
			return true
		}
		if len(typed.Checks) > 0 {
			for _, check := range typed.Checks {
				if !catalogEvalCondition(check.Check, map[string]any{"payload": payload, "entity": entity, "policy": policy}) {
					return false
				}
			}
			return true
		}
		if strings.TrimSpace(typed.Check) == "" {
			return true
		}
		return catalogEvalCondition(typed.Check, map[string]any{"payload": payload, "entity": entity, "policy": policy})
	case runtimecontracts.GuardSpec:
		return catalogGuardPasses(&typed, payload, entity, policy)
	case map[string]any:
		if len(typed) == 0 {
			return true
		}
		return catalogEvalCondition(asStringForCatalog(typed["check"]), map[string]any{"payload": payload, "entity": entity, "policy": policy})
	default:
		return true
	}
	return true
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
	for _, item := range items {
		if spec.EmitMapping != nil && len(spec.EmitMapping.Mapping) > 0 {
			mappingKey := resolveCatalogRef(strings.TrimSpace(spec.EmitMapping.KeyField), entity, map[string]any{"item": item, "entity": entity, "payload": payload})
			if eventType, ok := spec.EmitMapping.Mapping[strings.TrimSpace(asStringForCatalog(mappingKey))]; ok {
				result.emittedEvents = append(result.emittedEvents, strings.TrimSpace(eventType))
			}
			continue
		}
		if emit := strings.TrimSpace(spec.EmitPerItem); emit != "" {
			result.emittedEvents = append(result.emittedEvents, emit)
		}
	}
}

func applyCatalogFilter(spec *runtimecontracts.FilterSpec, payload, entity, policy map[string]any) {
	if spec == nil {
		return
	}
	items := catalogSlice(resolveCatalogPath(spec.ItemsPath, strings.TrimSpace(spec.ItemsFrom), entity, map[string]any{"payload": payload, "entity": entity}))
	filtered := make([]any, 0, len(items))
	for _, item := range items {
		if catalogEvalCondition(spec.Condition, map[string]any{"item": item, "payload": payload, "entity": entity, "policy": policy}) {
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

func applyCatalogCount(spec *runtimecontracts.CountSpec, payload, entity, policy map[string]any) {
	if spec == nil {
		return
	}
	items := catalogSlice(resolveCatalogPath(spec.ItemsPath, strings.TrimSpace(spec.ItemsFrom), entity, map[string]any{"payload": payload, "entity": entity}))
	count := 0
	for _, item := range items {
		if strings.TrimSpace(spec.Condition) == "" || catalogEvalCondition(spec.Condition, map[string]any{"item": item, "payload": payload, "entity": entity, "policy": policy}) {
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
	if got, want := result.handlerOutcome, strings.TrimSpace(expected.Expected.HandlerOutcome); got != want {
		t.Fatalf("handler outcome = %q, want %q", got, want)
	}
	if got, want := result.entityState, strings.TrimSpace(expected.Expected.EntityState); got != want {
		t.Fatalf("entity state = %q, want %q", got, want)
	}
	if diff := diffStringSet(normalizeSorted(result.emittedEvents), normalizeSorted(expected.Expected.EmittedEvents)); diff != "" {
		t.Fatalf("emitted events mismatch (%s)", diff)
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
		if !ok {
			t.Fatalf("missing gate %q", key)
		}
		if truthyCatalog(got) != truthyCatalog(want) {
			t.Fatalf("gate %s = %#v, want %#v", key, got, want)
		}
	}
}

func validateCatalogExpectedDocument(expected catalogExpectedDocument) error {
	if expected.Trigger.Boot {
		if strings.TrimSpace(expected.Expected.BootResult) == "" {
			return fmt.Errorf("boot trigger requires expected.boot_result")
		}
		return nil
	}
	if strings.TrimSpace(expected.Trigger.Event) == "" && len(expected.Trigger.Sequence) == 0 && len(expected.Trigger.Concurrent) == 0 {
		return fmt.Errorf("runtime trigger requires event, sequence, or concurrent")
	}
	if len(expected.Trigger.Concurrent) > 0 && len(expected.Expected.Entities) == 0 {
		return fmt.Errorf("concurrent trigger requires expected.entities")
	}
	if len(expected.Trigger.Concurrent) == 0 && strings.TrimSpace(expected.Expected.HandlerOutcome) == "" && !expected.Expected.DeadLetter {
		return fmt.Errorf("runtime case requires expected.handler_outcome")
	}
	return nil
}

func catalogCaseSimpleHarnessEligible(expected catalogExpectedDocument) bool {
	if expected.Trigger.Boot || len(expected.Trigger.Concurrent) > 0 {
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
	if len(expected.Expected.Entities) > 0 || len(expected.Expected.AgentRouting) > 0 || len(expected.Expected.AgentReceived) > 0 {
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

func repoRootFromMASTest(t testing.TB) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	return filepath.Clean(filepath.Join(wd, "..", "..", ".."))
}
