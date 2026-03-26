package bus

import "context"

type LoggerHook interface {
	Log(ctx context.Context, level, component, action, eventID, eventType, agentID, entityID, sessionID string, correlation map[string]string, detail any, errText string, durationUS int)
}
