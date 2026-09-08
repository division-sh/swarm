package runtimepersistence

import (
	"context"
	"fmt"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
)

func (*sqliteFlowActivationBus) ProjectLifecycleDiagnostic(context.Context, diaglog.LifecycleDiagnostic) error {
	return fmt.Errorf("lifecycle diagnostic persistence is not configured for sqliteFlowActivationBus")
}

func (*selectedRouteRecoveryPostgresBus) ProjectLifecycleDiagnostic(context.Context, diaglog.LifecycleDiagnostic) error {
	return fmt.Errorf("lifecycle diagnostic persistence is not configured for selectedRouteRecoveryPostgresBus")
}
