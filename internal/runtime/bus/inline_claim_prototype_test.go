package bus

import (
	"context"
	"time"

	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

func (unexpectedDurableTestRoles) AdmitInlineClaim(context.Context, runtimedelivery.Claim) (time.Duration, error) {
	return 0, errUnexpectedDurableTestRole
}
