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
	eventType := strings.TrimSpace(string(evt.Type()))
	inClosedCatalog := events.IsDiagnosticDirectEventType(events.EventType(eventType))
	if evt.AdmissionClass() != events.EventAdmissionDiagnosticDirect {
		if inClosedCatalog {
			return fmt.Errorf("diagnostic-direct event %s requires its typed constructor and persistence owner", eventType)
		}
		return nil
	}
	if !inClosedCatalog {
		return fmt.Errorf("diagnostic-direct event type %q is not in the closed catalog", eventType)
	}
	owner, _ := ctx.Value(diagnosticDirectOwnerContextKey{}).(string)
	if strings.TrimSpace(owner) != eventType {
		return fmt.Errorf("diagnostic-direct event %s requires its typed persistence owner", eventType)
	}
	return nil
}

func rejectDiagnosticDirectDeliveryPersistence(evt events.Event) error {
	eventType := strings.TrimSpace(string(evt.Type()))
	if evt.AdmissionClass() == events.EventAdmissionDiagnosticDirect || events.IsDiagnosticDirectEventType(events.EventType(eventType)) {
		return fmt.Errorf("diagnostic-direct event %s cannot use generic event delivery persistence", eventType)
	}
	return nil
}
