package server

import (
	"context"
	"fmt"
	"strings"

	"swarm/internal/store"
)

type schemaCapabilitySource interface {
	ResolveSchemaCapabilities(ctx context.Context) (store.StoreSchemaCapabilities, error)
}

func missingDashboardCapabilityOwner(surface string) error {
	return fmt.Errorf("dashboard: %s requires explicit schema capability owner", strings.TrimSpace(surface))
}

func unsupportedDashboardSchemaCapability(subject string, flavor store.SchemaFlavor) error {
	subject = strings.TrimSpace(subject)
	switch flavor {
	case store.SchemaFlavorUnavailable:
		return fmt.Errorf("dashboard: %s schema is unavailable at the explicit capability boundary", subject)
	case store.SchemaFlavorUnsupported, store.SchemaFlavorLegacy:
		return fmt.Errorf("dashboard: %s schema is unsupported by the explicit capability boundary", subject)
	default:
		return fmt.Errorf("dashboard: %s schema capability is invalid (%s)", subject, strings.TrimSpace(string(flavor)))
	}
}

func requireConversationSurfaceCapabilities(caps store.StoreSchemaCapabilities) error {
	if caps.Conversations.Sessions == store.SchemaFlavorCanonical || caps.Conversations.Audits == store.SchemaFlavorCanonical {
		return nil
	}
	if caps.Conversations.Sessions == store.SchemaFlavorUnavailable && caps.Conversations.Audits == store.SchemaFlavorUnavailable {
		return missingDashboardCapabilityOwner("conversation reader capability surface")
	}
	if caps.Conversations.Audits != store.SchemaFlavorUnavailable {
		return unsupportedDashboardSchemaCapability("agent_conversation_audits", caps.Conversations.Audits)
	}
	return unsupportedDashboardSchemaCapability("agent_sessions", caps.Conversations.Sessions)
}

func requireConversationTurnCapabilities(caps store.StoreSchemaCapabilities, surface string) error {
	if caps.Conversations.Turns == store.SchemaFlavorCanonical {
		return nil
	}
	if caps.Conversations.Turns == store.SchemaFlavorUnavailable {
		return missingDashboardCapabilityOwner(surface)
	}
	return unsupportedDashboardSchemaCapability("agent_turns", caps.Conversations.Turns)
}

func requireAgentOperatorProjectionCapabilities(caps store.StoreSchemaCapabilities) error {
	if err := requireConversationTurnCapabilities(caps, "agent operator projection capability surface"); err != nil {
		return err
	}
	return store.RequireCanonicalPendingAgentDeliveryCapabilities(caps)
}
