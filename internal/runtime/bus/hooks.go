package bus

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/diaglog"
)

type LoggerHook interface {
	Log(ctx context.Context, level diaglog.Level, message, component, action, eventID, eventType, agentID, entityID, sessionID string, correlation map[string]string, detail any, errText string, durationUS int) error
}
