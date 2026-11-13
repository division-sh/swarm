//go:build linux || darwin

package serveapp

import (
	"context"

	"github.com/division-sh/swarm/internal/channelonboarding"
)

func (s *serveSessionBootstrap) ReadSessionConnection(ctx context.Context, op channelonboarding.Operation) (channelonboarding.SessionConnectionReadback, error) {
	view := channelonboarding.SessionConnectionReadback{State: "disconnected", Linked: op.SessionAccount.Validate() == nil}
	if op.Phase == channelonboarding.PhaseFailed || op.Phase == channelonboarding.PhaseRetired {
		return view, nil
	}
	connection := s.connection(op.OperationID)
	if connection != nil {
		view.Reason, view.Cause = connection.FailureReport()
		if view.Cause != "" && !(op.Phase == channelonboarding.PhaseSucceeded && view.Linked && view.Reason == "") {
			view.State = "error"
		}
	}
	if op.SessionAccount.Validate() != nil {
		if view.Reason == "" && view.Cause == "" {
			view.State = "connecting"
		}
		return view, nil
	}
	provider, _, current, err := s.ObserveSession(ctx, op)
	defer provider.CloseExecution()
	if err != nil || !current {
		return view, nil
	}
	if connection == nil {
		return view, nil
	}
	view.AccountPresentation, view.AccountName, err = connection.AccountPresentation(ctx)
	if err != nil {
		return view, err
	}
	view.ClaimCodeNotice, err = connection.ReadClaimCodeNotice(ctx, op)
	if err != nil {
		return view, err
	}
	view.State, view.Reason = "connected", ""
	return view, nil
}
