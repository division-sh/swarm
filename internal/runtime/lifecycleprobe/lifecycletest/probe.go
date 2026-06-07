package lifecycletest

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	runtimelifecycleprobe "github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
)

const (
	SubscriberAgent = "agent"
	SubscriberNode  = "node"

	StatusPending    = "pending"
	StatusInProgress = "in_progress"
	StatusDelivered  = "delivered"
	StatusFailed     = "failed"
	StatusDeadLetter = "dead_letter"

	defaultTimeout = 2 * time.Second
)

type Option func(*Probe)

func WithTimeout(timeout time.Duration) Option {
	return func(p *Probe) {
		if timeout > 0 {
			p.timeout = timeout
		}
	}
}

type Probe struct {
	t       testing.TB
	inner   *runtimelifecycleprobe.Probe
	timeout time.Duration
}

func New(t testing.TB, opts ...Option) *Probe {
	t.Helper()
	return Wrap(t, runtimelifecycleprobe.New(), opts...)
}

func Wrap(t testing.TB, probe *runtimelifecycleprobe.Probe, opts ...Option) *Probe {
	t.Helper()
	if probe == nil {
		t.Fatal("lifecycle test probe is nil")
	}
	out := &Probe{
		t:       t,
		inner:   probe,
		timeout: defaultTimeout,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(out)
		}
	}
	return out
}

func (p *Probe) NotifyLifecycle(ctx context.Context, signal runtimelifecycleprobe.Signal) {
	if p == nil || p.inner == nil {
		return
	}
	p.inner.NotifyLifecycle(ctx, signal)
}

func (p *Probe) Raw() *runtimelifecycleprobe.Probe {
	p.t.Helper()
	return p.inner
}

func (p *Probe) RequireEventPersisted(eventID string) runtimelifecycleprobe.Signal {
	p.t.Helper()
	return p.require(runtimelifecycleprobe.Signal{
		Kind:    runtimelifecycleprobe.EventPersisted,
		EventID: eventID,
	})
}

func (p *Probe) RequireDeliveryPersisted(eventID, subscriberType, subscriberID string) runtimelifecycleprobe.Signal {
	p.t.Helper()
	return p.require(runtimelifecycleprobe.Signal{
		Kind:           runtimelifecycleprobe.DeliveryPersisted,
		EventID:        eventID,
		SubscriberType: subscriberType,
		SubscriberID:   subscriberID,
	})
}

func (p *Probe) RequireDeliveryStatus(eventID, subscriberType, subscriberID, status string) runtimelifecycleprobe.Signal {
	p.t.Helper()
	return p.require(runtimelifecycleprobe.Signal{
		Kind:           runtimelifecycleprobe.DeliveryStatusChanged,
		EventID:        eventID,
		SubscriberType: subscriberType,
		SubscriberID:   subscriberID,
		Status:         status,
	})
}

func (p *Probe) RequireAgentStatus(eventID, agentID, status string) runtimelifecycleprobe.Signal {
	p.t.Helper()
	return p.RequireDeliveryStatus(eventID, SubscriberAgent, agentID, status)
}

func (p *Probe) RequireNodeStatus(eventID, nodeID, status string) runtimelifecycleprobe.Signal {
	p.t.Helper()
	return p.RequireDeliveryStatus(eventID, SubscriberNode, nodeID, status)
}

func (p *Probe) RequireNodePending(eventID, nodeID string) runtimelifecycleprobe.Signal {
	p.t.Helper()
	return p.RequireNodeStatus(eventID, nodeID, StatusPending)
}

func (p *Probe) RequireNodeInProgress(eventID, nodeID string) runtimelifecycleprobe.Signal {
	p.t.Helper()
	return p.RequireNodeStatus(eventID, nodeID, StatusInProgress)
}

func (p *Probe) RequireNodeDelivered(eventID, nodeID string) runtimelifecycleprobe.Signal {
	p.t.Helper()
	return p.RequireNodeStatus(eventID, nodeID, StatusDelivered)
}

func (p *Probe) RequireNodeFailed(eventID, nodeID string) runtimelifecycleprobe.Signal {
	p.t.Helper()
	return p.RequireNodeStatus(eventID, nodeID, StatusFailed)
}

func (p *Probe) RequireNodeDeadLetter(eventID, nodeID string) runtimelifecycleprobe.Signal {
	p.t.Helper()
	return p.RequireNodeStatus(eventID, nodeID, StatusDeadLetter)
}

func (p *Probe) RequireAgentPending(eventID, agentID string) runtimelifecycleprobe.Signal {
	p.t.Helper()
	return p.RequireAgentStatus(eventID, agentID, StatusPending)
}

func (p *Probe) RequireAgentInProgress(eventID, agentID string) runtimelifecycleprobe.Signal {
	p.t.Helper()
	return p.RequireAgentStatus(eventID, agentID, StatusInProgress)
}

func (p *Probe) RequireAgentDelivered(eventID, agentID string) runtimelifecycleprobe.Signal {
	p.t.Helper()
	return p.RequireAgentStatus(eventID, agentID, StatusDelivered)
}

func (p *Probe) RequireAgentFailed(eventID, agentID string) runtimelifecycleprobe.Signal {
	p.t.Helper()
	return p.RequireAgentStatus(eventID, agentID, StatusFailed)
}

func (p *Probe) RequireAgentDeadLetter(eventID, agentID string) runtimelifecycleprobe.Signal {
	p.t.Helper()
	return p.RequireAgentStatus(eventID, agentID, StatusDeadLetter)
}

func (p *Probe) RequireHandlerStarted(eventID, nodeID string) runtimelifecycleprobe.Signal {
	p.t.Helper()
	return p.require(runtimelifecycleprobe.Signal{
		Kind:           runtimelifecycleprobe.HandlerStarted,
		EventID:        eventID,
		SubscriberType: SubscriberNode,
		SubscriberID:   nodeID,
	})
}

func (p *Probe) RequireHandlerCompleted(eventID, nodeID string) runtimelifecycleprobe.Signal {
	p.t.Helper()
	return p.require(runtimelifecycleprobe.Signal{
		Kind:           runtimelifecycleprobe.HandlerCompleted,
		EventID:        eventID,
		SubscriberType: SubscriberNode,
		SubscriberID:   nodeID,
	})
}

func (p *Probe) RequirePostCommitDispatchStarted(eventID string) runtimelifecycleprobe.Signal {
	p.t.Helper()
	return p.require(runtimelifecycleprobe.Signal{
		Kind:    runtimelifecycleprobe.PostCommitDispatchStarted,
		EventID: eventID,
	})
}

func (p *Probe) RequirePostCommitDispatchCompleted(eventID string) runtimelifecycleprobe.Signal {
	p.t.Helper()
	return p.require(runtimelifecycleprobe.Signal{
		Kind:    runtimelifecycleprobe.PostCommitDispatchCompleted,
		EventID: eventID,
	})
}

func (p *Probe) Expect(eventID string) *Expectation {
	p.t.Helper()
	return &Expectation{probe: p, eventID: strings.TrimSpace(eventID)}
}

func (p *Probe) require(want runtimelifecycleprobe.Signal) runtimelifecycleprobe.Signal {
	p.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
	defer cancel()
	got, err := p.inner.Wait(ctx, want)
	if err != nil {
		p.t.Fatalf("wait for lifecycle %s: %v", describeSignal(want), err)
	}
	return got
}

func describeSignal(signal runtimelifecycleprobe.Signal) string {
	parts := []string{string(signal.Kind), "event_id=" + signal.EventID}
	if signal.EventType != "" {
		parts = append(parts, "event_type="+signal.EventType)
	}
	if signal.SubscriberType != "" {
		parts = append(parts, "subscriber_type="+signal.SubscriberType)
	}
	if signal.SubscriberID != "" {
		parts = append(parts, "subscriber_id="+signal.SubscriberID)
	}
	if signal.Status != "" {
		parts = append(parts, "status="+signal.Status)
	}
	return strings.Join(parts, " ")
}

func stepLabel(signal runtimelifecycleprobe.Signal) string {
	if signal.Status != "" {
		return fmt.Sprintf("%s:%s", signal.Kind, signal.Status)
	}
	return string(signal.Kind)
}
