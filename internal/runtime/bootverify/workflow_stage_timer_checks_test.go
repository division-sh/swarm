package bootverify

import (
	"context"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestTimerValidationAcceptsAdvanceOnlyStageTimerInternalEvent(t *testing.T) {
	findings := stageTimerValidationFindings(runtimecontracts.WorkflowTimerContract{
		ID:         "awaiting_review.expired",
		Stage:      "awaiting_review",
		Event:      runtimecontracts.WorkflowStageTimerInternalEvent,
		Owner:      "runtime",
		StageOwned: true,
		AdvancesTo: "expired",
		Delay:      "48h",
		StartOn:    "state:awaiting_review",
	})
	if len(findings) != 0 {
		t.Fatalf("findings = %#v, want none for internal advance-only stage timer", findings)
	}
}

func TestTimerValidationRejectsStageTimerUnknownAdvanceTarget(t *testing.T) {
	findings := stageTimerValidationFindings(runtimecontracts.WorkflowTimerContract{
		ID:         "awaiting_review.missing",
		Stage:      "awaiting_review",
		Event:      runtimecontracts.WorkflowStageTimerInternalEvent,
		Owner:      "runtime",
		StageOwned: true,
		AdvancesTo: "missing",
		Delay:      "48h",
		StartOn:    "state:awaiting_review",
	})
	if !stageTimerFindingContains(findings, "advances_to references unknown stage missing") {
		t.Fatalf("findings = %#v, want unknown advances_to target", findings)
	}
}

func TestTimerValidationRejectsLegacyStateTimerInStagesFlow(t *testing.T) {
	source := semanticview.Wrap(stageTimerValidationBundle(runtimecontracts.WorkflowTimerContract{
		ID:      "legacy_sla",
		Stage:   "awaiting_review",
		Event:   "timer.legacy_sla",
		Owner:   "runtime",
		Delay:   "48h",
		StartOn: "state:awaiting_review",
	}))
	findings := checkTimerValidation(newCheckerContext(context.Background(), source, Options{}))
	if !stageTimerFindingContains(findings, "legacy node-level start_on state:awaiting_review") {
		t.Fatalf("findings = %#v, want legacy state timer fence", findings)
	}
}

func stageTimerValidationFindings(timer runtimecontracts.WorkflowTimerContract) []Finding {
	source := semanticview.Wrap(stageTimerValidationBundle(timer))
	return checkTimerValidation(newCheckerContext(context.Background(), source, Options{}))
}

func stageTimerValidationBundle(timer runtimecontracts.WorkflowTimerContract) *runtimecontracts.WorkflowContractBundle {
	return &runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{
			StageDeclarations: runtimecontracts.FlowStageDeclarations{
				Declared: true,
				Entries: []runtimecontracts.FlowStageDeclaration{
					{ID: "awaiting_review", Initial: true},
					{ID: "expired", Terminal: true},
				},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"timer.legacy_sla": {
				Swarm: runtimecontracts.EventSwarmMetadata{
					Consumer: []string{"operator"},
				},
			},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{
			Timers: []runtimecontracts.WorkflowTimerContract{timer},
		},
	}
}

func stageTimerFindingContains(findings []Finding, want string) bool {
	for _, finding := range findings {
		if strings.Contains(finding.Message, want) {
			return true
		}
	}
	return false
}
