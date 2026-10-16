package startupownership

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

// ReconcileDisplacedHTTPActivities is a pre-dispatch startup step. Its result
// contains readback obligations, never a permit to repeat an activity request.
func ReconcileDisplacedHTTPActivities(ctx context.Context, capability ProcessCapability) (pipeline.ActivityRecoverySummary, error) {
	owner, ok := capability.(*processCapability)
	if !ok || owner == nil {
		return pipeline.ActivityRecoverySummary{}, errors.New("activity recovery requires the original retained process capability")
	}
	owner.opMu.Lock()
	defer owner.opMu.Unlock()
	if err := owner.proveCurrent(ctx); err != nil {
		return pipeline.ActivityRecoverySummary{}, err
	}
	role, ok := owner.session.(interface {
		ReconcileDisplacedHTTPActivities(context.Context) (pipeline.ActivityRecoverySummary, error)
	})
	if !ok {
		return pipeline.ActivityRecoverySummary{}, errors.New("retained process session has no activity recovery owner")
	}
	result, err := role.ReconcileDisplacedHTTPActivities(ctx)
	if err != nil {
		err = owner.retireOnPossessionFailure(err)
	}
	return result, errors.Join(err, owner.requireLive())
}
