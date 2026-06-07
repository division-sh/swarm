package lifecycletest

import (
	"context"
	"time"

	runtimelifecycleprobe "github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
)

type Expectation struct {
	probe   *Probe
	eventID string
	steps   []expectationStep
}

type expectationStep struct {
	label string
	want  runtimelifecycleprobe.Signal
}

func (e *Expectation) EventPersisted() *Expectation {
	return e.append(runtimelifecycleprobe.Signal{Kind: runtimelifecycleprobe.EventPersisted})
}

func (e *Expectation) DeliveryPersisted(subscriberType, subscriberID string) *Expectation {
	return e.append(runtimelifecycleprobe.Signal{
		Kind:           runtimelifecycleprobe.DeliveryPersisted,
		SubscriberType: subscriberType,
		SubscriberID:   subscriberID,
	})
}

func (e *Expectation) DeliveryStatus(subscriberType, subscriberID, status string) *Expectation {
	return e.append(runtimelifecycleprobe.Signal{
		Kind:           runtimelifecycleprobe.DeliveryStatusChanged,
		SubscriberType: subscriberType,
		SubscriberID:   subscriberID,
		Status:         status,
	})
}

func (e *Expectation) AgentStatus(agentID, status string) *Expectation {
	return e.DeliveryStatus(SubscriberAgent, agentID, status)
}

func (e *Expectation) NodeStatus(nodeID, status string) *Expectation {
	return e.DeliveryStatus(SubscriberNode, nodeID, status)
}

func (e *Expectation) NodePending(nodeID string) *Expectation {
	return e.NodeStatus(nodeID, StatusPending)
}

func (e *Expectation) NodeInProgress(nodeID string) *Expectation {
	return e.NodeStatus(nodeID, StatusInProgress)
}

func (e *Expectation) NodeDelivered(nodeID string) *Expectation {
	return e.NodeStatus(nodeID, StatusDelivered)
}

func (e *Expectation) NodeFailed(nodeID string) *Expectation {
	return e.NodeStatus(nodeID, StatusFailed)
}

func (e *Expectation) NodeDeadLetter(nodeID string) *Expectation {
	return e.NodeStatus(nodeID, StatusDeadLetter)
}

func (e *Expectation) AgentPending(agentID string) *Expectation {
	return e.AgentStatus(agentID, StatusPending)
}

func (e *Expectation) AgentInProgress(agentID string) *Expectation {
	return e.AgentStatus(agentID, StatusInProgress)
}

func (e *Expectation) AgentDelivered(agentID string) *Expectation {
	return e.AgentStatus(agentID, StatusDelivered)
}

func (e *Expectation) AgentFailed(agentID string) *Expectation {
	return e.AgentStatus(agentID, StatusFailed)
}

func (e *Expectation) AgentDeadLetter(agentID string) *Expectation {
	return e.AgentStatus(agentID, StatusDeadLetter)
}

func (e *Expectation) HandlerStarted(nodeID string) *Expectation {
	return e.append(runtimelifecycleprobe.Signal{
		Kind:           runtimelifecycleprobe.HandlerStarted,
		SubscriberType: SubscriberNode,
		SubscriberID:   nodeID,
	})
}

func (e *Expectation) HandlerCompleted(nodeID string) *Expectation {
	return e.append(runtimelifecycleprobe.Signal{
		Kind:           runtimelifecycleprobe.HandlerCompleted,
		SubscriberType: SubscriberNode,
		SubscriberID:   nodeID,
	})
}

func (e *Expectation) PostCommitDispatchStarted() *Expectation {
	return e.append(runtimelifecycleprobe.Signal{Kind: runtimelifecycleprobe.PostCommitDispatchStarted})
}

func (e *Expectation) PostCommitDispatchCompleted() *Expectation {
	return e.append(runtimelifecycleprobe.Signal{Kind: runtimelifecycleprobe.PostCommitDispatchCompleted})
}

func (e *Expectation) Require() []runtimelifecycleprobe.Signal {
	return e.Within(0)
}

func (e *Expectation) Within(timeout time.Duration) []runtimelifecycleprobe.Signal {
	e.probe.t.Helper()
	if timeout <= 0 {
		timeout = e.probe.timeout
	}
	if e.eventID == "" {
		e.probe.t.Fatal("lifecycle expectation event_id is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	signals := make([]runtimelifecycleprobe.Signal, 0, len(e.steps))
	var cursor runtimelifecycleprobe.Cursor
	for i, step := range e.steps {
		got, next, err := e.probe.inner.WaitAfter(ctx, cursor, step.want)
		if err != nil {
			e.probe.t.Fatalf("wait for lifecycle step %d %s: %v", i+1, step.label, err)
		}
		signals = append(signals, got)
		cursor = next
	}
	return signals
}

func (e *Expectation) append(signal runtimelifecycleprobe.Signal) *Expectation {
	e.probe.t.Helper()
	signal.EventID = e.eventID
	e.steps = append(e.steps, expectationStep{
		label: stepLabel(signal),
		want:  signal,
	})
	return e
}
