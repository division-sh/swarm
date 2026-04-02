package llm

import (
	"context"
	"strings"

	runtimebus "swarm/internal/runtime/bus"
	runtimepipeline "swarm/internal/runtime/pipeline"
)

type runtimeLogPublisher interface {
	LogRuntime(ctx context.Context, entry runtimepipeline.RuntimeLogEntry)
}

func logPublisherRuntime(ctx context.Context, publisher EventPublisher, level, action, agentID, sessionID, entityID string, detail any, err error) {
	if publisher == nil {
		return
	}
	logger, ok := publisher.(runtimeLogPublisher)
	if !ok || logger == nil {
		return
	}
	inbound, _ := runtimebus.InboundEventFromContext(ctx)
	errText := ""
	if err != nil {
		errText = strings.TrimSpace(err.Error())
	}
	logger.LogRuntime(ctx, runtimepipeline.RuntimeLogEntry{
		Level:     strings.TrimSpace(level),
		Component: "llm-runtime",
		Action:    strings.TrimSpace(action),
		EventID:   strings.TrimSpace(inbound.ID),
		EventType: strings.TrimSpace(string(inbound.Type)),
		AgentID:   strings.TrimSpace(agentID),
		EntityID:  strings.TrimSpace(entityID),
		SessionID: strings.TrimSpace(sessionID),
		Detail:    detail,
		Error:     errText,
	})
}
