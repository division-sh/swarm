package storetest

import (
	"context"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
	"time"
)

func InstallDeliveryDiagnosticFault(ctx context.Context, selected any) error {
	return private.InstallDeliveryDiagnosticFaultForTest(ctx, selected)
}
func RemoveDeliveryDiagnosticFault(ctx context.Context, selected any) error {
	return private.RemoveDeliveryDiagnosticFaultForTest(ctx, selected)
}
func InstallNodeRecoveryPoisonFault(ctx context.Context, selected any, id string) error {
	return private.InstallNodeRecoveryPoisonFaultForTest(ctx, selected, id)
}
func RemoveNodeRecoveryPoisonFault(ctx context.Context, selected any) error {
	return private.RemoveNodeRecoveryPoisonFaultForTest(ctx, selected)
}
func TerminateDeliveryConformanceSession(ctx context.Context, selected any, id string, at time.Time) error {
	return private.TerminateDeliveryConformanceSessionForTest(ctx, selected, id, at)
}
