package testcases

import (
	"fmt"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"empireai/internal/models"
	runtimecontracts "empireai/internal/runtime/contracts"
)

type simulatedHandlerOutcome struct {
	nextState  string
	emitted    []string
	setsGate   string
	clearGates []string
}

func loadGenericMASBundle(t testing.TB) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	repoRoot := repoRootFromTestcases(t)
	bundleRoot := filepath.Join(repoRoot, "internal", "runtime", "testdata", "generic-mas-bundle")
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "mas-platform", "platform", "contracts", "platform-spec.yaml")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, bundleRoot, platformSpec)
	if err != nil {
		t.Fatalf("load generic MAS bundle: %v", err)
	}
	return bundle
}

func repoRootFromTestcases(t testing.TB) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve testcase file path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}

func mustHandler(t testing.TB, bundle *runtimecontracts.WorkflowContractBundle, nodeID, eventType string) runtimecontracts.SystemNodeEventHandler {
	t.Helper()
	handler, ok := bundle.NodeEventHandler(nodeID, eventType)
	if !ok {
		t.Fatalf("missing handler %s/%s", nodeID, eventType)
	}
	return handler
}

func gateName(spec *runtimecontracts.GateSpec) string {
	if spec == nil {
		return ""
	}
	return strings.TrimSpace(spec.Name)
}

func stringSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			out[trimmed] = struct{}{}
		}
	}
	return out
}

func hasAll(values []string, want ...string) bool {
	set := stringSet(values)
	for _, item := range want {
		if _, ok := set[strings.TrimSpace(item)]; !ok {
			return false
		}
	}
	return true
}

func simulateAccumulation(handler runtimecontracts.SystemNodeEventHandler, received, expected int) simulatedHandlerOutcome {
	outcome := simulatedHandlerOutcome{
		nextState:  strings.TrimSpace(handler.AdvancesTo),
		emitted:    handler.Emits.Values(),
		setsGate:   gateName(handler.SetsGate),
		clearGates: append([]string(nil), handler.ClearGates...),
	}
	if handler.Accumulate == nil || handler.Accumulate.OnComplete == nil {
		return outcome
	}
	if received >= expected && expected > 0 {
		outcome.nextState = strings.TrimSpace(handler.Accumulate.OnComplete.AdvancesTo)
		outcome.emitted = handler.Accumulate.OnComplete.Emits.Values()
	}
	return outcome
}

func chooseRuleForScore(handler runtimecontracts.SystemNodeEventHandler, score float64) (runtimecontracts.HandlerRuleEntry, bool) {
	for _, rule := range handler.Rules {
		condition := strings.TrimSpace(rule.Condition)
		switch {
		case strings.Contains(condition, ">="):
			parts := strings.SplitN(condition, ">=", 2)
			threshold, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
			if err == nil && score >= threshold {
				return rule, true
			}
		case strings.Contains(condition, "<"):
			parts := strings.SplitN(condition, "<", 2)
			threshold, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
			if err == nil && score < threshold {
				return rule, true
			}
		}
	}
	return runtimecontracts.HandlerRuleEntry{}, false
}

func evaluateGuard(guard *runtimecontracts.GuardSpec, payload, entity, policy map[string]any) bool {
	if guard == nil {
		return true
	}
	if check := strings.TrimSpace(guard.Check); check != "" && !evaluateComparison(check, payload, entity, policy) {
		return false
	}
	for _, check := range guard.Checks {
		if !evaluateComparison(strings.TrimSpace(check.Check), payload, entity, policy) {
			return false
		}
	}
	return true
}

func evaluateComparison(expr string, payload, entity, policy map[string]any) bool {
	for _, op := range []string{">=", "==", "!=", "<"} {
		if !strings.Contains(expr, op) {
			continue
		}
		parts := strings.SplitN(expr, op, 2)
		if len(parts) != 2 {
			return false
		}
		left := resolveRef(strings.TrimSpace(parts[0]), payload, entity, policy)
		right := resolveRef(strings.TrimSpace(parts[1]), payload, entity, policy)
		switch op {
		case ">=":
			return asFloat(left) >= asFloat(right)
		case "<":
			return asFloat(left) < asFloat(right)
		case "==":
			return fmt.Sprint(left) == fmt.Sprint(right)
		case "!=":
			return fmt.Sprint(left) != fmt.Sprint(right)
		}
	}
	return false
}

func resolveRef(expr string, payload, entity, policy map[string]any) any {
	expr = strings.TrimSpace(expr)
	switch expr {
	case "true":
		return true
	case "false":
		return false
	}
	if n, err := strconv.ParseFloat(expr, 64); err == nil {
		return n
	}
	if strings.HasPrefix(expr, "'") && strings.HasSuffix(expr, "'") && len(expr) >= 2 {
		return strings.Trim(expr, "'")
	}
	rootMaps := map[string]map[string]any{
		"payload": payload,
		"entity":  entity,
		"policy":  policy,
	}
	parts := strings.Split(expr, ".")
	if len(parts) == 0 {
		return nil
	}
	current, ok := rootMaps[strings.TrimSpace(parts[0])]
	if !ok {
		return expr
	}
	var value any = current
	for _, part := range parts[1:] {
		obj, ok := value.(map[string]any)
		if !ok {
			return nil
		}
		value = obj[strings.TrimSpace(part)]
	}
	return value
}

func asFloat(value any) float64 {
	switch typed := value.(type) {
	case int:
		return float64(typed)
	case int64:
		return float64(typed)
	case float64:
		return typed
	case float32:
		return float64(typed)
	case string:
		n, _ := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		return n
	default:
		return 0
	}
}

func weightedScore(handler runtimecontracts.SystemNodeEventHandler, payload map[string]any) float64 {
	if handler.Compute == nil {
		return 0
	}
	total := 0.0
	for _, tier := range handler.Compute.Tiers {
		sum := 0.0
		for _, dimension := range tier.Dimensions {
			sum += asFloat(resolveRef(dimension, payload, nil, nil))
		}
		if len(tier.Dimensions) > 0 {
			sum /= float64(len(tier.Dimensions))
		}
		total += sum * tier.Weight
	}
	return total
}

func agentConfigFromEntry(id string, entry runtimecontracts.AgentRegistryEntry) models.AgentConfig {
	return models.AgentConfig{
		ID:            id,
		Type:          entry.Type,
		Role:          entry.Role,
		Subscriptions: append([]string(nil), entry.Subscriptions...),
	}
}
