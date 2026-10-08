package bus

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/google/uuid"
)

type queueReasonReadProbe struct {
	contexts                []context.Context
	paused, blocked, parked bool
	failure                 error
}

func (p *queueReasonReadProbe) QueueableIngressPaused(ctx context.Context) (bool, error) {
	p.contexts = append(p.contexts, ctx)
	return p.paused, p.failure
}

func (p *queueReasonReadProbe) QueueableRunDispatchBlocked(ctx context.Context, _ string) (bool, error) {
	p.contexts = append(p.contexts, ctx)
	return p.blocked, p.failure
}

func (p *queueReasonReadProbe) QueueableRunDispatchParked(ctx context.Context, _ string) (bool, error) {
	p.contexts = append(p.contexts, ctx)
	return p.parked, p.failure
}

func TestDispatchQueueReasonReadContextAndLiveDecisions(t *testing.T) {
	for _, owned := range []bool{false, true} {
		name := "caller_scoped"
		if owned {
			name = "accepted_work"
		}
		for _, state := range []string{"running", "ingress_paused", "run_blocked", "run_parked", "independent", "joined", "canceled_before", "settled_before"} {
			t.Run(name+"/"+state, func(t *testing.T) {
				process := worklifetime.NewProcess()
				owner := newReceiverProjectionRuntimeOwner(t, process, "queue-reason-read")
				parent, cancel := context.WithCancel(context.Background())
				defer cancel()
				lease, err := owner.Begin(parent)
				if err != nil {
					t.Fatal(err)
				}
				settled := false
				t.Cleanup(func() {
					if !settled {
						if err := lease.Done(); err != nil {
							t.Error(err)
						}
					}
					if _, err := owner.RetireAndWait(context.Background()); err != nil {
						t.Error(err)
					}
					process.Retire()
					if _, err := process.Join(context.Background()); err != nil {
						t.Error(err)
					}
				})
				ctx := parent
				if owned {
					ctx = bindWorkContext(parent, lease, owner)
				}
				probe := &queueReasonReadProbe{paused: state == "ingress_paused", blocked: state == "run_blocked" || state == "run_parked", parked: state == "run_parked"}
				independent := errors.New("independent gate read failure")
				switch state {
				case "independent":
					probe.failure = independent
				case "joined":
					probe.failure = errors.Join(context.Canceled, independent)
				case "canceled_before":
					cancel()
				case "settled_before":
					if err := lease.Done(); err != nil {
						t.Fatal(err)
					}
					settled = true
					if owned {
						// Detached caller control cannot revive opaque evidence for a
						// lease whose original logical context has already settled.
						ctx = context.WithoutCancel(ctx)
					} else {
						cancel()
					}
				}
				eb := &EventBus{runtimeIngressDispatchGate: probe, runDispatchGate: probe}
				event := eventtest.ExistingRunRootIngress(uuid.NewString(), "queue.read", "test", "", []byte(`{}`), 0, uuid.NewString(), events.EventEnvelope{}, time.Now().UTC())
				reason, err := eb.dispatchQueueReason(ctx, event)
				wantReason := map[string]string{"ingress_paused": dispatchQueueRuntimeIngress, "run_blocked": dispatchQueueRunBlocked, "run_parked": "run_paused"}[state]
				if state == "canceled_before" || state == "settled_before" {
					if !errors.Is(err, context.Canceled) || len(probe.contexts) != 0 || reason != "" {
						t.Fatalf("stopped admission: reads=%d reason=%q err=%v", len(probe.contexts), reason, err)
					}
					return
				}
				if state == "independent" || state == "joined" {
					if !errors.Is(err, independent) || reason != "" || len(probe.contexts) != 1 {
						t.Fatalf("independent read evidence lost: reads=%d reason=%q err=%v", len(probe.contexts), reason, err)
					}
				} else if err != nil || reason != wantReason {
					t.Fatalf("live decision=%q err=%v, want %q", reason, err, wantReason)
				}
				for _, readCtx := range probe.contexts {
					if (readCtx.Done() == nil) != owned {
						t.Fatalf("non-cancellable metadata context=%t, want exact accepted evidence=%t", readCtx.Done() == nil, owned)
					}
				}
			})
		}
	}
}
