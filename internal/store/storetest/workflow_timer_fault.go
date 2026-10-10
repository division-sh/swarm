package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func RemoveWorkflowTimer(ctx context.Context, selected any, run, activation string) error {
	return private.RemoveWorkflowTimerForTest(ctx, selected, run, activation)
}

func SetWorkflowTimerForeignDeclaration(ctx context.Context, selected any, run, activation string) error {
	return private.SetWorkflowTimerForeignDeclarationForTest(ctx, selected, run, activation)
}

func SetWorkflowTimerMalformedName(ctx context.Context, selected any, run, activation string) error {
	return private.SetWorkflowTimerMalformedNameForTest(ctx, selected, run, activation)
}

func SetWorkflowTimerForeignRouteAndMalformedName(ctx context.Context, selected any, run, activation string) error {
	return private.SetWorkflowTimerForeignRouteAndMalformedNameForTest(ctx, selected, run, activation)
}
