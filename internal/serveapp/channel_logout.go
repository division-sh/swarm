package serveapp

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
)

type serveSessionLogoutDispatcher struct {
	sessions serveSessionBootstrapOwner
	effects  effects.Store
	posture  executionposture.Posture
}

func (d *serveSessionLogoutDispatcher) PrepareSessionLogout(ctx context.Context, op channelonboarding.Operation) (channelonboarding.SessionLogoutTarget, error) {
	if d == nil || d.sessions == nil || d.effects == nil || !d.posture.Valid() {
		return channelonboarding.SessionLogoutTarget{}, fmt.Errorf("session logout requires its installed owners")
	}
	if err := d.posture.Admit(executionmode.Live, "explicit session logout"); err != nil {
		return channelonboarding.SessionLogoutTarget{}, err
	}
	return d.sessions.PrepareSessionLogout(ctx, op)
}

func (d *serveSessionLogoutDispatcher) DispatchSessionLogout(ctx context.Context, op channelonboarding.TeardownOperation) error {
	if d == nil || d.sessions == nil || d.effects == nil || op.Logout == nil || op.Logout.Validate() != nil || !d.posture.Valid() {
		return channelonboarding.ErrInvalidRequest
	}
	coordinate := op.Logout.Coordinate
	owned := effects.WithController(ctx, effects.NewController(d.effects).WithExecutionPosture(d.posture))
	owned = authoractivity.WithScope(owned, authoractivity.BundleScope(coordinate.RuntimeInstanceID, coordinate.BundleHash))
	return d.sessions.DispatchSessionLogout(owned, op)
}
