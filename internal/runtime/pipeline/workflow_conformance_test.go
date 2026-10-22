package pipeline

import (
	"context"

	"github.com/division-sh/swarm/internal/events"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
)

type pipelineTestBus struct{}

func (pipelineTestBus) Publish(context.Context, events.Event) error { return nil }
func (pipelineTestBus) SubscribeInternal(string, ...events.EventType) <-chan events.Event {
	return make(chan events.Event)
}
func (pipelineTestBus) DirectSubscribe(string) <-chan events.Event { return make(chan events.Event) }
func (pipelineTestBus) PublishDirect(context.Context, events.Event, []string) error {
	return nil
}
func (pipelineTestBus) SubscribeAll(string) <-chan events.Event           { return make(chan events.Event) }
func (pipelineTestBus) ResetSubscribers()                                 {}
func (pipelineTestBus) LogRuntime(context.Context, RuntimeLogEntry) error { return nil }
func (pipelineTestBus) ResolveSubscribedRecipients(string) []string       { return nil }
func (pipelineTestBus) EngineDispatcher() runtimeengine.PostCommitDispatcher {
	return noOpEngineDispatcher{}
}
