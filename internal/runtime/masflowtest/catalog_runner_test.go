package masflowtest

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	runtimecontracts "empireai/internal/runtime/contracts"
	"gopkg.in/yaml.v3"
)

type catalogTriggerStep struct {
	Event   string         `yaml:"event"`
	Payload map[string]any `yaml:"payload"`
}

type catalogExpectedDocument struct {
	Trigger struct {
		Event              string               `yaml:"event"`
		Payload            map[string]any       `yaml:"payload"`
		Sequence           []catalogTriggerStep `yaml:"sequence"`
		EntityFieldsBefore map[string]any       `yaml:"entity_fields_before"`
	} `yaml:"trigger"`
	Expected struct {
		HandlerOutcome string   `yaml:"handler_outcome"`
		EntityState    string   `yaml:"entity_state"`
		EmittedEvents  []string `yaml:"emitted_events"`
	} `yaml:"expected"`
}

type catalogRunResult struct {
	handlerOutcome string
	entityState    string
	emittedEvents  []string
}

func TestCatalogRunner_ExecutesCurrentMASTestPackages(t *testing.T) {
	for _, dir := range discoveredCatalogCaseDirs(t) {
		dir := dir
		t.Run(dir, func(t *testing.T) {
			root := filepath.Join(repoRootFromMASTest(t), "docs", "specs", "mas-platform", "tests", dir)
			result, expected := runSimpleCatalogCase(t, root)
			if got, want := result.handlerOutcome, strings.TrimSpace(expected.Expected.HandlerOutcome); got != want {
				t.Fatalf("handler outcome = %q, want %q", got, want)
			}
			if got, want := result.entityState, strings.TrimSpace(expected.Expected.EntityState); got != want {
				t.Fatalf("entity state = %q, want %q", got, want)
			}
			if diff := diffStringSet(normalizeSorted(result.emittedEvents), normalizeSorted(expected.Expected.EmittedEvents)); diff != "" {
				t.Fatalf("emitted events mismatch (%s)", diff)
			}
		})
	}
}

func discoveredCatalogCaseDirs(t testing.TB) []string {
	t.Helper()
	root := filepath.Join(repoRootFromMASTest(t), "docs", "specs", "mas-platform", "tests")
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read MAS test catalog root: %v", err)
	}
	dirs := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := strings.TrimSpace(entry.Name())
		if name == "" {
			continue
		}
		if !catalogCasePresent(filepath.Join(root, name)) {
			continue
		}
		dirs = append(dirs, name)
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
	loadYAMLForCatalogTest(t, filepath.Join(dir, "expected.yaml"), &expected)

	entity := map[string]any{}
	for key, value := range expected.Trigger.EntityFieldsBefore {
		entity[strings.TrimSpace(key)] = value
	}
	state := strings.TrimSpace(schema.InitialState)
	result := catalogRunResult{entityState: state}
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
			result = executeCatalogHandlerStep(t, handler, step.Payload, entity, policy, result)
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
		key := catalogAccumulationKey(step.Payload)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		received++
		entity["received_items"] = appendCatalogItem(entity["received_items"], step.Payload)
		if catalogAccumulationComplete(completion, received, expectedCount) {
			result = executeCatalogHandlerStep(t, handler, step.Payload, entity, policy, result)
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

func executeCatalogHandlerStep(t testing.TB, handler runtimecontracts.SystemNodeEventHandler, payload map[string]any, entity map[string]any, policy map[string]any, result catalogRunResult) catalogRunResult {
	t.Helper()
	result.handlerOutcome = "success"
	if !catalogGuardPasses(handler.Guard, payload, entity, policy) {
		result.handlerOutcome = guardFailOutcome(handler.Guard)
		if result.handlerOutcome == "" {
			result.handlerOutcome = "blocked"
		}
		return result
	}
	if next := strings.TrimSpace(handler.AdvancesTo); next != "" {
		result.entityState = next
	}
	result.emittedEvents = append(result.emittedEvents, handler.Emits.Values()...)
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

func repoRootFromMASTest(t testing.TB) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	return filepath.Clean(filepath.Join(wd, "..", "..", ".."))
}
