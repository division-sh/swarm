package bus

import "context"

type TransitionHook interface {
	Begin(ctx context.Context) (context.Context, any)
	Flush(ctx context.Context, state any)
}

type LoggerHook interface {
	Log(ctx context.Context, level, component, action, agentID, verticalID string, detail any, errText string)
}

type WarningHook interface {
	Warnf(component, format string, args ...any)
}
