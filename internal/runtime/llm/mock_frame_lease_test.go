package llm

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/runtime/workspace"
)

type frameRenewObservation struct {
	sessions.Registry
	renewed  chan error
	renewErr error
	allow    sync.Once
	ready    chan struct{}
}

func (r *frameRenewObservation) Renew(ctx context.Context, lease *sessions.Lease) (*sessions.Lease, error) {
	next, err := r.Registry.Renew(ctx, lease)
	r.allow.Do(func() { r.renewErr = err; r.renewed <- err; close(r.ready) })
	return next, err
}

type frameHeldWorkspace struct {
	mockHostWorkspace
	observation *frameRenewObservation
}

func (w frameHeldWorkspace) ResolveWorkspace(ctx context.Context, _ models.AgentConfig) (*workspace.Target, error) {
	select {
	case <-w.observation.ready:
		// A failed renewal must be allowed to cancel the provider context, not
		// race a model dispatch after the expired grant is observed.
		if err := <-w.observation.renewed; err != nil {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return w.target, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestMockFrameFixtureLeaseMustSurviveFirstHeartbeat(t *testing.T) {
	for _, test := range []struct {
		name    string
		ttl     time.Duration
		expired bool
	}{
		{"original_one_second_lease", time.Second, true},
		{"renewable_fixture_lease", 15 * time.Second, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			observation := &frameRenewObservation{Registry: sessions.NewInMemoryRegistry(test.ttl), renewed: make(chan error, 1), ready: make(chan struct{})}
			options := mockHostRuntimeOptions(t)
			options.Workspaces = frameHeldWorkspace{mockHostWorkspace: options.Workspaces.(mockHostWorkspace), observation: observation}
			harness, response, err := runMockManagedFrameFixture(t, observation, options)
			if test.expired {
				if !errors.Is(err, context.Canceled) || !errors.Is(observation.renewErr, sessions.ErrSessionLeased) || response != nil || harness.CompletionCount() != 0 {
					t.Fatalf("short lease did not reproduce pre-model cancellation: response=%+v err=%v completions=%d", response, err, harness.CompletionCount())
				}
				return
			}
			if err != nil || response == nil || response.Message.Content != "done" || harness.CompletionCount() != 1 {
				t.Fatalf("renewed lease lost canonical frame: response=%+v err=%v completions=%d", response, err, harness.CompletionCount())
			}
		})
	}
}
