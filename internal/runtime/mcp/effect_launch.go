package mcp

import (
	"context"
	"errors"
	"time"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

func continueCommittedEffectLaunch(ctx context.Context, handle *runtimeeffects.Handle, err error) error {
	if err == nil {
		return nil
	}
	if handle == nil {
		return err
	}
	attempt := handle.Attempt()
	if !attempt.AuthorizationAcknowledged || !runtimeeffects.CommittedMutationPhase(err, runtimeeffects.MutationLaunch, attempt) {
		return err
	}
	blocked := ""
	var blocker error
	if ctxErr := ctx.Err(); ctxErr != nil {
		blocked, blocker = "caller_canceled", ctxErr
	} else if !attempt.Authority.Valid() || !time.Now().Before(attempt.Authority.LeaseExpiresAt) {
		blocked = "authority_not_current"
		blocker = runtimefailures.New(runtimefailures.ClassLifecycleConflict, "effect_launch_authority_not_current", "mcp-client", "dispatch", nil)
	}
	if blocked != "" {
		// The adapter has not invoked the primitive, even though the launch marker committed.
		evidence := map[string]any{"launch_rejected": true, "dispatch_attempted": false, "reason": blocked}
		failure := runtimefailures.Normalize(runtimefailures.New(runtimefailures.ClassLifecycleConflict,
			"effect_launch_dispatch_not_attempted", "mcp-client", "dispatch", evidence), "mcp-client", "dispatch")
		settleErr := handle.Settle(ctx, runtimeeffects.StateTerminalFailure, &failure, evidence)
		return errors.Join(err, blocker, settleErr)
	}
	return nil
}
