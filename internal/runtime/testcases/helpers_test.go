package testcases

import (
	"context"
	"encoding/json"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

func loadGenericSwarmBundle(t testing.TB) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	repoRoot := repoRootFromTestcases(t)
	bundleRoot := filepath.Join(repoRoot, "internal", "runtime", "testdata", "generic-swarm-bundle")
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, bundleRoot, platformSpec)
	if err != nil {
		t.Fatalf("load generic Swarm bundle: %v", err)
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

func agentConfigFromEntry(id string, entry runtimecontracts.AgentRegistryEntry) models.AgentConfig {
	return models.AgentConfig{
		ExecutionMode: "live",
		ID:            id,
		Type:          entry.Type,
		Role:          entry.Role,
		Permissions:   append([]string(nil), entry.Permissions...),
		Subscriptions: append([]string(nil), entry.Subscriptions...),
	}
}

func previewHandler(t testing.TB, bundle *runtimecontracts.WorkflowContractBundle, nodeID, eventType string, payload map[string]any, state runtimepipeline.WorkflowState, policyOverrides map[string]any) runtimepipeline.HandlerPreview {
	t.Helper()
	rawPayload, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	preview, err := runtimepipeline.PreviewContractHandlerExecution(context.Background(), bundle, nodeID, eventtest.RunCreatingRootIngress(
		"evt-"+strings.ReplaceAll(eventType, ".", "-"),
		events.EventType(eventType),
		"test-driver",
		"",
		rawPayload,
		0,
		"00000000-0000-0000-0000-000000000001",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "item-123"),
		time.Now().UTC(),
	),

		state, policyOverrides)
	if err != nil {
		t.Fatalf("preview handler %s/%s: %v", nodeID, eventType, err)
	}
	return preview
}
