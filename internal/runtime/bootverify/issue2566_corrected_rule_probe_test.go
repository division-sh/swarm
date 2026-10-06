package bootverify

import (
	"context"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestIssue2566CorrectedServiceStillHasOldCoherenceVeto(t *testing.T) {
	schema := contracts.FlowSchemaDocument{StageDeclarations: contracts.FlowStageDeclarations{
		Declared: true, Entries: []contracts.FlowStageDeclaration{{ID: "waiting", Initial: true}},
	}}
	findings := stageDeclarationCoherenceFindings(".", schema)
	for _, finding := range findings {
		if strings.Contains(finding.Message, "at least one terminal stage") {
			t.Log("baseline structural coherence still rejects a no-end service flow; the corrected rule moves this veto to finite run admission")
			return
		}
	}
	t.Fatal("baseline witness no longer reproduces; update the audit rather than assuming a live veto")
}

func TestIssue2566CorrectedMarkedEndTimerSemanticAdmissionGap(t *testing.T) {
	timer := contracts.WorkflowTimerContract{ID: "expired.notice", Stage: "expired", Event: "timer.legacy_sla", Owner: "runtime", StageOwned: true, Delay: "1h"}
	source := semanticview.Wrap(stageTimerValidationBundle(timer))
	graph, ok := semanticview.WorkflowStageTopology(source, ".")
	if !ok {
		t.Fatal("missing compiled graph")
	}
	end, err := graph.ResolveStage("expired")
	if err != nil || !end.IsTerminal() {
		t.Fatalf("probe must use the actual marked end: %v", err)
	}
	c := newCheckerContext(context.Background(), source, Options{})
	c.validateStageTimerSemantics(timer)
	if len(c.timerFindings) != 0 {
		t.Fatalf("baseline witness no longer reproduces: %+v", c.timerFindings)
	}
	t.Log("stage-specific timer validation admits an emit-only timer on the marked end; no final-stage no-arm rule is enforced here")
}
