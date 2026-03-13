package bus

import "context"

type LoggerHook interface {
	Log(ctx context.Context, level, component, action, eventID, eventType, agentID, entityID, campaignID, scanID, sessionID string, detail any, errText string, durationUS int)
}
