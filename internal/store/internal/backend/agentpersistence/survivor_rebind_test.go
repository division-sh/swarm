package agentpersistence

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/google/uuid"
)

func TestAgentLifecycleGenerationMutationClassShapes(t *testing.T) {
	identity := agentidentitytest.RuntimeForRun(t, uuid.NewString(), "worker", "survivor-test", "flow", "one", "flow/one")
	bundle := "bundle-v2:sha256:" + strings.Repeat("a", 64)
	old, err := agenttopology.StaticAdmission("old-plan", bundle, agenttopology.LifetimeDurableManaged)
	if err != nil {
		t.Fatal(err)
	}
	next, err := agenttopology.StaticAdmission("next-plan", bundle, agenttopology.LifetimeDurableManaged)
	if err != nil {
		t.Fatal(err)
	}
	previous := manager.AgentLifecycleState{
		Identity: identity, AgentID: identity.AgentID(), RuntimeEpoch: 9, Generation: 4,
		Phase: manager.AgentLifecycleRunning, ConfigRevision: "config", RunMode: manager.AgentRunModeStandard,
		Topology: old, ProcessBinding: manager.ProcessExecutionBinding{
			ProcessAuthorityID: uuid.NewString(), ProcessOwnerID: "owner", ProcessBootID: uuid.NewString(),
			GenerationGrantID: uuid.NewString(), BundleHash: bundle, RuntimeInstanceID: uuid.NewString(), RuntimeGeneration: 1,
		},
	}
	request := manager.AgentLifecycleTransition{
		Identity: identity, AgentID: identity.AgentID(), OperationKind: "source_set_rebind",
		ExpectedEpoch: previous.RuntimeEpoch, ExpectedGeneration: previous.Generation, ExpectedPhase: previous.Phase,
		TargetEpoch: previous.RuntimeEpoch, TargetGeneration: previous.Generation, TargetPhase: previous.Phase,
		ConfigRevision: previous.ConfigRevision, RunMode: previous.RunMode, Topology: next,
		ProcessBinding: previous.ProcessBinding, Subordinate: sessions.LifecycleMutationPlan{Action: sessions.LifecycleMutationNone},
	}
	request.ProcessBinding.GenerationGrantID = uuid.NewString()
	for _, test := range []struct {
		name   string
		mutate func(*manager.AgentLifecycleTransition)
		valid  bool
	}{
		{name: "static_survivor", valid: true},
		{name: "same_named_replacement", valid: true, mutate: func(r *manager.AgentLifecycleTransition) { r.TargetGeneration++ }},
		{name: "skipped_generation", mutate: func(r *manager.AgentLifecycleTransition) { r.TargetGeneration += 2 }},
		{name: "reused_restart_generation", mutate: func(r *manager.AgentLifecycleTransition) { r.OperationKind = "restart" }},
		{name: "changed_epoch", mutate: func(r *manager.AgentLifecycleTransition) { r.TargetEpoch++ }},
		{name: "changed_phase", mutate: func(r *manager.AgentLifecycleTransition) { r.TargetPhase = manager.AgentLifecycleRegistered }},
		{name: "changed_config", mutate: func(r *manager.AgentLifecycleTransition) { r.ConfigRevision = "changed" }},
		{name: "changed_mode", mutate: func(r *manager.AgentLifecycleTransition) { r.RunMode = manager.AgentRunModeStopped }},
		{name: "replacement_agent", mutate: func(r *manager.AgentLifecycleTransition) { r.Agent = &manager.PersistedAgent{} }},
		{name: "session_mutation", mutate: func(r *manager.AgentLifecycleTransition) {
			r.Subordinate.Action = sessions.LifecycleMutationRotateCurrentSet
		}},
		{name: "foreign_lease", mutate: func(r *manager.AgentLifecycleTransition) { r.ProcessBinding.ProcessBootID = uuid.NewString() }},
		{name: "missing_lease", mutate: func(r *manager.AgentLifecycleTransition) { r.ProcessBinding = manager.ProcessExecutionBinding{} }},
		{name: "unchanged_grant", mutate: func(r *manager.AgentLifecycleTransition) {
			r.ProcessBinding.GenerationGrantID = previous.ProcessBinding.GenerationGrantID
		}},
		{name: "unchanged_topology", mutate: func(r *manager.AgentLifecycleTransition) { r.Topology = old }},
	} {
		t.Run(test.name, func(t *testing.T) {
			req := request
			if test.mutate != nil {
				test.mutate(&req)
			}
			if err := validateLifecycleGeneration(req, previous, true, false); (err == nil) != test.valid {
				t.Fatalf("generation class validation=%v, want accepted=%v", err, test.valid)
			}
		})
	}
	readiness, err := agenttopology.FlowReadinessAdmission(identity.RunID, identity.FlowInstance(), "readiness-plan")
	if err != nil {
		t.Fatal(err)
	}
	readiness, err = readiness.WithFlowActivationAttempt("1")
	if err != nil {
		t.Fatal(err)
	}
	previous.Topology, request.Topology = readiness, readiness
	if err := validateLifecycleGeneration(request, previous, true, true); err != nil {
		t.Fatalf("complete-instance survivor refused: %v", err)
	}
	if err := validateLifecycleGeneration(request, previous, true, false); err == nil {
		t.Fatal("individual readiness restamp admitted")
	}
	request.Topology, err = readiness.WithFlowActivationAttempt("2")
	if err != nil {
		t.Fatal(err)
	}
	if err := validateLifecycleGeneration(request, previous, true, true); err == nil {
		t.Fatal("changed attachment admitted as a survivor")
	}
}
