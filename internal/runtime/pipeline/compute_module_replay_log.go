package pipeline

import (
	"context"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/computemodule"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
)

func logComputeModuleReplayEvidence(ctx context.Context, bus Bus, nodeID string, evt events.Event, traces []runtimeengine.ComputeModuleTrace) {
	if bus == nil || len(traces) == 0 {
		return
	}
	detail := computemodule.NewReplayEvidenceDetail(traces)
	detail["node_id"] = strings.TrimSpace(nodeID)
	_ = bus.LogRuntime(ctx, RuntimeLogEntry{
		Level:     diaglog.LevelInfo,
		Message:   "Compute module replay evidence recorded",
		Component: "compute_module",
		Action:    computemodule.ReplayEvidenceAction,
		EventID:   strings.TrimSpace(evt.ID()),
		EventType: strings.TrimSpace(string(evt.Type())),
		EntityID:  workflowEventEntityID(evt),
		Detail:    detail,
	})
}
