package eventreceiver_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

func TestSelectedRecoveryPublicationRejectsExecutableComposition(t *testing.T) {
	id, runID := uuid.NewString(), uuid.NewString()
	actor := agentidentitytest.RootRuntimeForRun(t, runID, "recovered", "recovery-test")
	variant, err := eventreceiver.SelectedRecoveryPublication(effects.Authority{
		Kind: effects.AuthoritySelectedContractFork, ID: id, ExecutionMode: executionmode.Live,
		SelectedFork: effects.SelectedContractForkAuthority{
			ExecutionID: id, ForkRunID: runID, Generation: 1, AdmissionFingerprint: "admission",
			ContainerPlanFingerprint: "container", ActorCensusFingerprint: "actors", EffectiveConfigFingerprint: "config",
		}, ExecutionOwner: "old-owner", LeaseExpiresAt: time.Now().UTC().Add(time.Minute), FenceGeneration: 1,
		Target: effects.UsageTarget{Kind: effects.UsageTargetAgentTurn, ID: uuid.NewString(), RunID: runID,
			AgentID: actor.AgentID(), AgentIdentity: actor, SessionID: uuid.NewString()},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Run("manager", func(t *testing.T) {
		defer func() {
			if value := recover(); value == nil || !strings.Contains(fmt.Sprint(value), "cannot authorize receiver execution") {
				t.Fatalf("publication-only Manager construction was not refused: %v", value)
			}
		}()
		manager.NewAgentManagerWithOptions(nil, nil, manager.AgentManagerOptions{ReceiverExecution: variant})
	})
	t.Run("pipeline", func(t *testing.T) {
		events, err := bus.NewEphemeralEventBus(nil)
		if err != nil {
			t.Fatal(err)
		}
		if value := pipeline.NewPipelineCoordinatorWithOptions(events, pipeline.PipelineCoordinatorOptions{
			ReceiverExecution: variant, ExecutionPosture: executionposture.Live,
		}); value != nil {
			t.Fatal("publication-only posture constructed an executable Pipeline")
		}
	})
}
