package pipeline_test

import (
	"context"
	"fmt"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
)

func (*exactJoinRuntimeLogger) ProjectLifecycleDiagnostic(context.Context, diaglog.LifecycleDiagnostic) error {
	return fmt.Errorf("lifecycle diagnostic persistence is not configured for exactJoinRuntimeLogger")
}

func (*proposedEffectRouteProofBus) ProjectLifecycleDiagnostic(context.Context, diaglog.LifecycleDiagnostic) error {
	return fmt.Errorf("lifecycle diagnostic persistence is not configured for proposedEffectRouteProofBus")
}
