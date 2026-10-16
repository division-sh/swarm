package agentpersistence

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/google/uuid"
)

func TestStartupTakeoverFactRequiresExactImmutableResult(t *testing.T) {
	bundle := "bundle-v2:sha256:" + strings.Repeat("a", 64)
	identity := agentidentitytest.RootRuntimeForRun(t, uuid.NewString(), "worker", "takeover-recovery")
	topology, err := agenttopology.StaticAdmission("source-plan", bundle, agenttopology.LifetimeDurableManaged)
	if err != nil {
		t.Fatal(err)
	}
	current := manager.AgentLifecycleState{
		Identity: identity, AgentID: identity.AgentID(), RuntimeEpoch: 8, Generation: 4,
		Phase: manager.AgentLifecycleRunning, ConfigRevision: "config", RunMode: manager.AgentRunModeStandard,
		Topology: topology, ProcessBinding: manager.ProcessExecutionBinding{
			ProcessAuthorityID: uuid.NewString(), ProcessOwnerID: "restart", ProcessBootID: uuid.NewString(),
			GenerationGrantID: uuid.NewString(), BundleHash: bundle, RuntimeInstanceID: uuid.NewString(), RuntimeGeneration: 3,
		}, LastTransitionID: uuid.NewString(),
	}
	fact := startupTakeoverFact{
		transitionID: current.LastTransitionID, operationID: uuid.NewString(), trigger: "process_takeover", kind: "process_takeover", operationState: "succeeded",
		previousPhase: current.Phase, phase: current.Phase, previousGeneration: 3, generation: 4,
		epoch: 8, expectedEpoch: 7, expectedGeneration: 3, targetGeneration: 4,
		configRevision: current.ConfigRevision, runMode: current.RunMode, binding: current.ProcessBinding,
	}
	fact.result = manager.AgentLifecycleTransitionResult{
		OperationID: fact.operationID, TransitionID: fact.transitionID, Identity: identity, AgentID: current.AgentID,
		PreviousEpoch: 7, RuntimeEpoch: 8, PreviousGeneration: 3, Generation: 4,
		PreviousPhase: current.Phase, Phase: current.Phase, ConfigRevision: current.ConfigRevision, RunMode: current.RunMode,
		Topology: current.Topology, ProcessBinding: current.ProcessBinding,
		Subordinate: sessions.LifecycleMutationOutcome{Action: sessions.LifecycleMutationNone},
		DiagnosticProvenance: manager.LifecycleDiagnosticProvenance{
			Origin:    manager.LifecycleDiagnosticOrigin{Owner: manager.LifecycleDiagnosticNormal, Causality: manager.LifecycleDiagnosticObservation},
			ActorMode: executionmode.Live, EventMode: executionmode.Live,
		},
	}
	if !fact.matches(current, 7, 3) {
		t.Fatal("exact immutable startup takeover rejected")
	}
	for _, test := range []struct {
		name   string
		change func(*startupTakeoverFact)
	}{
		{"ordinary_restart", func(f *startupTakeoverFact) { f.kind = "restart" }},
		{"ordinary_trigger", func(f *startupTakeoverFact) { f.trigger = "restart" }},
		{"pending_operation", func(f *startupTakeoverFact) { f.operationState = "pending" }},
		{"nonrunning_predecessor", func(f *startupTakeoverFact) { f.previousPhase = manager.AgentLifecycleRegistered }},
		{"changed_phase", func(f *startupTakeoverFact) { f.phase = manager.AgentLifecycleTerminated }},
		{"skipped_epoch", func(f *startupTakeoverFact) { f.epoch++ }},
		{"skipped_generation", func(f *startupTakeoverFact) { f.generation++ }},
		{"wrong_expected_epoch", func(f *startupTakeoverFact) { f.expectedEpoch-- }},
		{"wrong_expected_generation", func(f *startupTakeoverFact) { f.expectedGeneration-- }},
		{"wrong_previous_generation", func(f *startupTakeoverFact) { f.previousGeneration-- }},
		{"wrong_target_generation", func(f *startupTakeoverFact) { f.targetGeneration++ }},
		{"changed_config", func(f *startupTakeoverFact) { f.configRevision = "other" }},
		{"changed_run_mode", func(f *startupTakeoverFact) { f.runMode = manager.AgentRunModeStopped }},
		{"foreign_bundle", func(f *startupTakeoverFact) { f.binding.BundleHash = "bundle-v2:sha256:" + strings.Repeat("b", 64) }},
		{"missing_binding", func(f *startupTakeoverFact) { f.binding = manager.ProcessExecutionBinding{} }},
		{"foreign_result_identity", func(f *startupTakeoverFact) {
			f.result.Identity = agentidentitytest.RootRuntimeForRun(t, uuid.NewString(), "worker", "other")
		}},
		{"foreign_operation", func(f *startupTakeoverFact) { f.result.OperationID = uuid.NewString() }},
		{"foreign_transition", func(f *startupTakeoverFact) { f.result.TransitionID = uuid.NewString() }},
		{"result_epoch", func(f *startupTakeoverFact) { f.result.RuntimeEpoch++ }},
		{"result_generation", func(f *startupTakeoverFact) { f.result.Generation++ }},
		{"result_predecessor", func(f *startupTakeoverFact) { f.result.PreviousGeneration-- }},
		{"result_phase", func(f *startupTakeoverFact) { f.result.Phase = manager.AgentLifecycleTerminated }},
		{"result_config", func(f *startupTakeoverFact) { f.result.ConfigRevision = "other" }},
		{"result_topology", func(f *startupTakeoverFact) { f.result.Topology = agenttopology.Admission{} }},
		{"result_binding", func(f *startupTakeoverFact) { f.result.ProcessBinding.GenerationGrantID = uuid.NewString() }},
		{"missing_provenance", func(f *startupTakeoverFact) { f.result.DiagnosticProvenance = manager.LifecycleDiagnosticProvenance{} }},
		{"session_rotation", func(f *startupTakeoverFact) { f.result.Subordinate.Action = sessions.LifecycleMutationRotateCurrentSet }},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := fact
			test.change(&candidate)
			if candidate.matches(current, 7, 3) {
				t.Fatal("contradictory immutable takeover result accepted")
			}
		})
	}
}
