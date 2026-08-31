package runforkpersistence

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func TestSelectedContractWorkflowReadinessComposesExactForkAgentTopology(t *testing.T) {
	const runID = "00000000-0000-0000-0000-000000000227"
	identity := agentidentitytest.DeclaredForRun(t, runID, "worker", "flow/worker", "flow", "instance", "flow/instance")
	declaration, err := identity.Plan()
	if err != nil {
		t.Fatalf("agent plan: %v", err)
	}
	source, err := correlation.NewPersistedBundleSourceFact("bundle-v1:sha256:" + strings.Repeat("a", 64))
	if err != nil {
		t.Fatalf("bundle source: %v", err)
	}
	plan, topologies, encoded, err := selectedContractWorkflowReadiness(source, selectedContractWorkflowState{
		RunID: runID, EntityID: "00000000-0000-0000-0000-000000000228", WorkflowName: "flow",
		WorkflowVersion: "v1", ExecutionMode: executionmode.Mock, Mode: "template", Route: "flow/instance",
		Agents: []runfork.RunForkSelectedContractAgentExpectation{{
			Plan: declaration, ConfigRevision: strings.Repeat("b", 64),
		}},
	})
	if err != nil {
		t.Fatalf("selectedContractWorkflowReadiness: %v", err)
	}
	if plan == nil || len(encoded) == 0 || plan.RunID != runID || plan.Identity.InstancePath != "flow/instance" ||
		plan.CreationEvent != nil || len(plan.Agents) != 1 || plan.Agents[0].Identity != identity {
		t.Fatalf("readiness plan = %#v", plan)
	}
	if len(topologies) != 1 || topologies[0].Identity != identity ||
		topologies[0].Admission.Authority.Kind != agenttopology.AuthorityFlowReadinessPlan ||
		topologies[0].Admission.Authority.Readiness == nil ||
		topologies[0].Admission.Authority.Readiness.RunID != runID ||
		topologies[0].Admission.Authority.Readiness.InstancePath != "flow/instance" {
		t.Fatalf("committed topology = %#v", topologies)
	}
}

func TestSelectedContractWorkflowReadinessRejectsAgentOutsideExactFlowOwner(t *testing.T) {
	const runID = "00000000-0000-0000-0000-000000000227"
	identity := agentidentitytest.DeclaredForRun(t, runID, "worker", "flow/worker", "flow", "other", "flow/other")
	declaration, err := identity.Plan()
	if err != nil {
		t.Fatalf("agent plan: %v", err)
	}
	source, err := correlation.NewPersistedBundleSourceFact("bundle-v1:sha256:" + strings.Repeat("a", 64))
	if err != nil {
		t.Fatalf("bundle source: %v", err)
	}
	_, _, _, err = selectedContractWorkflowReadiness(source, selectedContractWorkflowState{
		RunID: runID, EntityID: "00000000-0000-0000-0000-000000000228", WorkflowName: "flow",
		WorkflowVersion: "v1", ExecutionMode: executionmode.Mock, Mode: "template", Route: "flow/instance",
		Agents: []runfork.RunForkSelectedContractAgentExpectation{{
			Plan: declaration, ConfigRevision: strings.Repeat("b", 64),
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "invalid agent declaration") {
		t.Fatalf("error = %v, want exact-flow rejection", err)
	}
}
