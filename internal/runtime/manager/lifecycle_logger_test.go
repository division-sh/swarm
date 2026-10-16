package manager

import (
	"context"
	"fmt"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
)

func (*recordingReceiptBus) ProjectLifecycleDiagnostic(context.Context, diaglog.LifecycleDiagnostic) error {
	return fmt.Errorf("lifecycle diagnostic persistence is not configured for recordingReceiptBus")
}

func (*partialOutputRetryBus) ProjectLifecycleDiagnostic(context.Context, diaglog.LifecycleDiagnostic) error {
	return fmt.Errorf("lifecycle diagnostic persistence is not configured for partialOutputRetryBus")
}

func (*directiveTestBus) ProjectLifecycleDiagnostic(context.Context, diaglog.LifecycleDiagnostic) error {
	return fmt.Errorf("lifecycle diagnostic persistence is not configured for directiveTestBus")
}

func (*recoveryTestBus) ProjectLifecycleDiagnostic(context.Context, diaglog.LifecycleDiagnostic) error {
	return fmt.Errorf("lifecycle diagnostic persistence is not configured for recoveryTestBus")
}

func (b *projectionTestBus) ProjectLifecycleDiagnostic(ctx context.Context, item diaglog.LifecycleDiagnostic) error {
	if b.lifecycleDiagnostic != nil {
		return b.lifecycleDiagnostic(ctx, item)
	}
	return fmt.Errorf("lifecycle diagnostic persistence is not configured for projectionTestBus")
}

func (*flowActivationTestBus) ProjectLifecycleDiagnostic(context.Context, diaglog.LifecycleDiagnostic) error {
	return fmt.Errorf("lifecycle diagnostic persistence is not configured for flowActivationTestBus")
}

func (*resetTestBus) ProjectLifecycleDiagnostic(context.Context, diaglog.LifecycleDiagnostic) error {
	return fmt.Errorf("lifecycle diagnostic persistence is not configured for resetTestBus")
}
