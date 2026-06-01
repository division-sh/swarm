package tools

import (
	"context"
	"strings"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
)

func toolExecutorRuntimeLogContext(ctx context.Context) context.Context {
	if ctx == nil {
		return nil
	}
	if inbound, ok := runtimebus.InboundEventFromContext(ctx); ok {
		return runtimecorrelation.WithRuntimeDiagnosticLineage(
			ctx,
			strings.TrimSpace(inbound.ID),
			strings.TrimSpace(string(inbound.Type)),
		)
	}
	if lineage, ok := runtimecorrelation.RuntimeLineageFromContext(ctx); ok {
		return runtimecorrelation.WithRuntimeDiagnosticLineage(
			ctx,
			strings.TrimSpace(lineage.SubjectEventID),
			strings.TrimSpace(lineage.SubjectEventType),
		)
	}
	return ctx
}
