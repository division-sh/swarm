package store

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
)

const (
	diagnosticDirectRuntimeLog     = string(events.EventTypePlatformRuntimeLog)
	diagnosticDirectInboundRecord  = string(events.EventTypePlatformInboundRecord)
	diagnosticDirectAgentDirective = string(events.EventTypePlatformAgentDirective)
)

type diagnosticDirectOwnerContextKey struct{}

func withDiagnosticDirectOwner(ctx context.Context, eventType string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, diagnosticDirectOwnerContextKey{}, strings.TrimSpace(eventType))
}

func validateDiagnosticDirectOwner(ctx context.Context, evt events.Event) error {
	if evt.AdmissionClass() != events.EventAdmissionDiagnosticDirect {
		return nil
	}
	eventType := strings.TrimSpace(string(evt.Type()))
	if !events.IsDiagnosticDirectEventType(events.EventType(eventType)) {
		return fmt.Errorf("diagnostic-direct event type %q is not in the closed catalog", eventType)
	}
	owner, _ := ctx.Value(diagnosticDirectOwnerContextKey{}).(string)
	if strings.TrimSpace(owner) != eventType {
		return fmt.Errorf("diagnostic-direct event %s requires its typed persistence owner", eventType)
	}
	return nil
}
