package storetest

import (
	"context"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type StandingEventNameDiagnostic = private.StandingEventNameDiagnostic
type StandingDeliveryDiagnostic = private.StandingDeliveryDiagnostic

func ReadStandingEventNameDiagnostics(ctx context.Context, selected any) ([]StandingEventNameDiagnostic, error) {
	return private.ReadStandingEventNameDiagnosticsForTest(ctx, selected)
}

func ReadStandingRuntimeLogDiagnostics(ctx context.Context, selected any) ([]string, error) {
	return private.ReadStandingRuntimeLogDiagnosticsForTest(ctx, selected)
}

func ReadStandingDeliveryDiagnostics(ctx context.Context, selected any) ([]StandingDeliveryDiagnostic, error) {
	return private.ReadStandingDeliveryDiagnosticsForTest(ctx, selected)
}
