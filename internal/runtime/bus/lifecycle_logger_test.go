package bus_test

import (
	"context"
	"fmt"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
)

func (*recordingLoggerHook) ProjectLifecycleDiagnostic(context.Context, diaglog.LifecycleDiagnostic) error {
	return fmt.Errorf("lifecycle diagnostic persistence is not configured for recordingLoggerHook")
}
