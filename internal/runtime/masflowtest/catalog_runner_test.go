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
	nodes := map[string]runtimecontracts.SystemNodeContract{}
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
	var handler runtimecontracts.SystemNodeEventHandler
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
	completion := strings.ToLower(strings.TrimSpace(accumulate.Completion.String()))
	for _, step := range steps {
		key := catalogAccumulationKey(step.Payload)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		received++
		entity["received_items"] = appendCatalogItem(entity["received_items"], step.Payload)
		if catalogAccumulationComplete(completion, received, expectedCount) {
			result = executeCatalogHandlerStep(t, handler, step, entity, policy, result)
			break
		}
	}
	if result.handlerOutcome == "" {
		result.handlerOutcome = "pending"
	}
	if strings.TrimSpace(result.entityState) == "" {
		result.entityState = state
	}
	return result, expected
}

func executeCatalogHandlerStep(t testing.TB, handler runtimecontracts.SystemNodeEventHandler, step catalogTriggerStep, entity map[string]any, policy map[string]any, result catalogRunResult) catalogRunResult {
	t.Helper()
	payload := cloneStringAnyMapCatalog(step.Payload)
	result.handlerOutcome = "success"
	if !catalogGuardPasses(handler.Guard, payload, entity, policy) {
		result.handlerOutcome = guardFailOutcome(handler.Guard)
		if result.handlerOutcome == "" {
			result.handlerOutcome = "blocked"
		}
		if result.handlerOutcome == "kill" {
			result.entityState = "killed"
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
			gates[strings.TrimSpace(gate)] = false
		}
		result.gates = cloneStringAnyMapCatalog(gates)
	}
	if handler.SetsGate != nil && strings.TrimSpace(gateSpecNameForCatalog(handler.SetsGate)) != "" {
		gates := ensureCatalogGates(entity)
		gates[strings.TrimSpace(gateSpecNameForCatalog(handler.SetsGate))] = true
		result.gates = cloneStringAnyMapCatalog(gates)
	}
	applyCatalogDataAccumulation(handler.DataAccumulation, payload, entity)
	applyCatalogCompute(handler.Compute, entity)
	applyCatalogFanOut(handler.FanOut, payload, entity, &result)
	result.entityFields = cloneStringAnyMapCatalog(entity)
	result.emittedEvents = append(result.emittedEvents, handler.Emits.Values()...)
	return result
}

func catalogRuleMatches(rule runtimecontracts.HandlerRuleEntry, payload, entity, policy map[string]any) bool {
	condition := strings.TrimSpace(rule.Condition)
	switch strings.ToLower(condition) {
	case "", "else":
		return true
	default:
		return catalogGuardPasses(map[string]any{"check": condition}, payload, entity, policy)
	}
}

func applyCatalogRule(rule runtimecontracts.HandlerRuleEntry, payload, entity map[string]any, result catalogRunResult) catalogRunResult {
	if next := strings.TrimSpace(rule.AdvancesTo); next != "" {
		result.entityState = next
	}
	if !rule.Emits.Empty() {
		result.emittedEvents = append(result.emittedEvents, rule.Emits.Values()...)
	}
	if rule.DataAccumulation.HasWrites() {
		applyCatalogDataAccumulation(rule.DataAccumulation, payload, entity)
		result.entityFields = cloneStringAnyMapCatalog(entity)
	}
	return result
}

func catalogAccumulationComplete(mode string, received, expected int) bool {
	switch mode {
	case "threshold", "all":
		return expected > 0 && received >= expected
	default:
		return false
	}
}

func catalogGuardPasses(spec any, payload, entity, policy map[string]any) bool {
	if spec == nil {
		return true
	}
	var (
		check string
		ok    bool
	)
	switch typed := spec.(type) {
	case *runtimecontracts.GuardSpec:
		if typed == nil {
			return true
		}
		check = strings.TrimSpace(typed.Check)
		ok = typed.Check != "" || typed.ID != "" || typed.OnFail != "" || len(typed.Checks) > 0
	case runtimecontracts.GuardSpec:
		check = strings.TrimSpace(typed.Check)
		ok = typed.Check != "" || typed.ID != "" || typed.OnFail != "" || len(typed.Checks) > 0
	case map[string]any:
		if len(typed) == 0 {
			return true
		}
		check = strings.TrimSpace(asStringForCatalog(typed["check"]))
		ok = true
	default:
		return true
	}
	if !ok {
		return true
	}
	if check == "" {
		return true
	}
	for _, op := range []string{">=", "<=", "==", "!=", ">", "<"} {
		if !strings.Contains(check, op) {
			continue
		}
		parts := strings.SplitN(check, op, 2)
		if len(parts) != 2 {
			return false
		}
		left := resolveCatalogRef(strings.TrimSpace(parts[0]), entity, map[string]any{"payload": payload, "policy": policy})
		right := resolveCatalogRef(strings.TrimSpace(parts[1]), entity, map[string]any{"payload": payload, "policy": policy})
		return compareCatalogValues(left, right, op)
	}
	return false
}

func guardFailOutcome(spec any) string {
	switch typed := spec.(type) {
	case *runtimecontracts.GuardSpec:
		if typed == nil {
			return ""
		}
		return strings.TrimSpace(typed.OnFail)
	case runtimecontracts.GuardSpec:
		return strings.TrimSpace(typed.OnFail)
	case map[string]any:
		return strings.TrimSpace(asStringForCatalog(typed["on_fail"]))
	default:
		return ""
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
		if write.HasLiteralValue() {
			entity[target] = write.Value.Literal
			continue
		}
		source := strings.TrimSpace(write.Source())
		if source == "" {
			continue
		}
		if value := resolveCatalogPath(write.SourcePath, source, entity, map[string]any{"payload": payload, "entity": entity}); value != nil {
			entity[target] = value
		}
	}
}

func applyCatalogCompute(spec *runtimecontracts.ComputeSpec, entity map[string]any) {
	if spec == nil {
		return
	}
	field := strings.TrimSpace(spec.StoreAs)
	field = strings.TrimPrefix(field, "entity.")
	field = strings.TrimPrefix(field, "metadata.")
	if field == "" {
		return
	}
	entity[field] = "computed_value"
}

func applyCatalogFanOut(spec *runtimecontracts.FanOutSpec, payload, entity map[string]any, result *catalogRunResult) {
	if spec == nil || result == nil {
		return
	}
	items, _ := resolveCatalogPath(spec.ItemsPath, strings.TrimSpace(spec.ItemsFrom), entity, map[string]any{"payload": payload, "entity": entity}).([]any)
	if len(items) == 0 {
		if raw, ok := resolveCatalogPath(spec.ItemsPath, strings.TrimSpace(spec.ItemsFrom), entity, map[string]any{"payload": payload, "entity": entity}).([]map[string]any); ok {
			items = make([]any, 0, len(raw))
			for _, item := range raw {
				items = append(items, item)
			}
		}
	}
	if len(items) == 0 {
		return
	}
	entity["fan_out_count"] = len(items)
	for _, item := range items {
		if len(spec.EmitMapping) > 0 {
			for key, eventType := range spec.EmitMapping {
				if strings.EqualFold(strings.TrimSpace(key), strings.TrimSpace(asStringForCatalog(item))) {
					result.emittedEvents = append(result.emittedEvents, strings.TrimSpace(eventType))
				}
			}
			continue
		}
		if emit := strings.TrimSpace(spec.EmitPerItem); emit != "" {
			result.emittedEvents = append(result.emittedEvents, emit)
		}
	}
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

func catalogAccumulationKey(item map[string]any) string {
	if item == nil {
		return ""
	}
	if id := strings.TrimSpace(asStringForCatalog(item["item_id"])); id != "" {
		return "item_id:" + id
	}
	if id := strings.TrimSpace(asStringForCatalog(item["id"])); id != "" {
		return "id:" + id
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
	if len(expected.Trigger.Entity) > 0 {
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
