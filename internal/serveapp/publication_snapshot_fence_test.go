package serveapp

import (
	"context"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

type snapshotPublicationLogBarrier struct {
	logger  *runtimepkg.RuntimeLogger
	entered chan string
	release <-chan struct{}
}

func (b *snapshotPublicationLogBarrier) ProjectLifecycleDiagnostic(ctx context.Context, diagnostic diaglog.LifecycleDiagnostic) error {
	return b.logger.ProjectLifecycleDiagnostic(ctx, diagnostic)
}

func (b *snapshotPublicationLogBarrier) Log(ctx context.Context, level diaglog.Level, message, component, action, eventID, eventType, agentID, entityID, sessionID string, correlation map[string]string, detail any, failure *runtimefailures.Envelope, durationUS int) error {
	if component == "eventbus" && action == "published" && eventType == "left.work.requested" {
		b.entered <- eventID
		select {
		case <-b.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return b.logger.Log(ctx, runtimepkg.RuntimeLogEntry{
		Level: level, Message: message, Component: component, Action: action,
		EventID: eventID, EventType: eventType, AgentID: agentID, EntityID: entityID,
		SessionID: sessionID, Correlation: correlation, Detail: detail,
		Failure: runtimefailures.CloneEnvelope(failure), DurationUS: durationUS,
	})
}
