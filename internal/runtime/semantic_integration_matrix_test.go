package runtime

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"empireai/internal/events"
	"empireai/internal/models"
	runtimetools "empireai/internal/runtime/tools"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

type semanticIntegrationMatrix struct {
	Cases []semanticIntegrationCase `yaml:"cases"`
}

type semanticIntegrationCase struct {
	ID string `yaml:"id"`
}

func TestSemanticFull30IntegrationMatrix(t *testing.T) {
	repoRoot := contractComplianceRepoRoot(t)
	raw, err := os.ReadFile(filepath.Join(repoRoot, "contracts", "test-vectors", "semantic-full-30.yaml"))
	if err != nil {
		t.Fatalf("read semantic full matrix: %v", err)
	}
	var matrix semanticIntegrationMatrix
	if err := yaml.Unmarshal(raw, &matrix); err != nil {
		t.Fatalf("parse semantic full matrix: %v", err)
	}
	if got, want := len(matrix.Cases), 30; got != want {
		t.Fatalf("semantic matrix case count mismatch: got=%d want=%d", got, want)
	}

	checks := map[string]func(*testing.T){
		"opco_org_creation_13_agents":      checkOpCoOrgCreation13Agents,
		"opco_routes_and_template_version": checkOpCoRoutesAndTemplateVersion,
		"cycle_counter_circuit_breaker":    checkCycleCounterCircuitBreaker,
		"budget_human_mailbox_contracts":   checkBudgetHumanMailboxContracts,
	}
	if got, want := len(checks), 4; got != want {
		t.Fatalf("integration semantic check count mismatch: got=%d want=%d", got, want)
	}

	for _, tc := range matrix.Cases {
		check := checks[strings.TrimSpace(tc.ID)]
		if check == nil {
			continue
		}
		t.Run(tc.ID, func(t *testing.T) { check(t) })
	}
}

func checkOpCoOrgCreation13Agents(t *testing.T) {
	roster := defaultOpCoRoster("v1")
	if len(roster) != 13 {
		t.Fatalf("expected 13-agent default opco roster, got %d", len(roster))
	}
	foundCEO := false
	for _, spec := range roster {
		if strings.TrimSpace(spec.Config.Role) == "opco-ceo" {
			foundCEO = true
			break
		}
	}
	if !foundCEO {
		t.Fatal("expected opco-ceo in default roster")
	}
}

func checkOpCoRoutesAndTemplateVersion(t *testing.T) {
	routes := defaultOpCoRoutes("v1")
	if len(routes) != 20 {
		t.Fatalf("expected 20 default opco routes, got %d", len(routes))
	}
	bootstrap := 0
	seeded := 0
	for _, rt := range routes {
		switch strings.TrimSpace(rt.Source) {
		case "bootstrap":
			bootstrap++
		case "seeded":
			seeded++
		}
	}
	if bootstrap != 20 || seeded != 0 {
		t.Fatalf("expected bootstrap=20 and seeded=0 routes, got bootstrap=%d seeded=%d", bootstrap, seeded)
	}

	bus := NewEventBus(InMemoryEventStore{})
	am := NewAgentManager(bus, nil)
	store := &templateStoreStub{bootstrapVersion: 7, info: VerticalInfo{ID: "v1", Name: "Acme Vertical", Slug: "acme", Geography: "US"}}
	am.store = store
	if err := am.SpawnOpCo("v1", models.MandateDocument{VerticalID: "v1"}); err != nil {
		t.Fatalf("SpawnOpCo: %v", err)
	}
	if store.setTplCalls != 1 || strings.TrimSpace(store.lastTplVersion) == "" {
		t.Fatalf("expected template version tracking call, calls=%d version=%q", store.setTplCalls, store.lastTplVersion)
	}
}

func checkCycleCounterCircuitBreaker(t *testing.T) {
	ctx := context.Background()
	tracker := NewOpCoCycleTracker(nil)
	verticalID := uuid.NewString()
	var escalated bool
	var escalation *events.Event
	for i := 0; i < defaultOpCoCycleLimit; i++ {
		escalated, escalation = tracker.Check(ctx, events.Event{ID: uuid.NewString(), Type: events.EventType("qa.validation_failed"), VerticalID: verticalID, SourceAgent: "opco-qa-" + verticalID, Payload: mustJSON(map[string]any{"cycle": i + 1})})
	}
	if !escalated || escalation == nil || strings.TrimSpace(string(escalation.Type)) != "cycle_limit_reached" {
		t.Fatalf("expected cycle_limit_reached escalation, got escalated=%v event=%+v", escalated, escalation)
	}
}

func checkBudgetHumanMailboxContracts(t *testing.T) {
	cases := map[string]events.EventType{"warning": events.EventType("budget.warning"), "throttle": events.EventType("budget.throttle"), "emergency": events.EventType("budget.emergency"), "ok": events.EventType("budget.resumed")}
	for state, want := range cases {
		if got := budgetEventTypeForState(state); got != want {
			t.Fatalf("budget state mapping mismatch state=%s got=%s want=%s", state, got, want)
		}
	}
	for _, evt := range []string{"human_task.requested", "human_task.approved", "human_task.rejected", "human_task.deferred", "mailbox.item_decided"} {
		if _, ok := contractEventPayloadFields[evt]; !ok {
			t.Fatalf("missing contract payload fields for %s", evt)
		}
	}
	if mt, err := runtimetools.NormalizeMailboxType("vertical_approval"); err != nil || mt != "vertical_approval" {
		t.Fatalf("mailbox type normalization mismatch type=%q err=%v", mt, err)
	}
	if mp, err := runtimetools.NormalizeMailboxPriority("critical"); err != nil || mp != "critical" {
		t.Fatalf("mailbox priority normalization mismatch priority=%q err=%v", mp, err)
	}
}
