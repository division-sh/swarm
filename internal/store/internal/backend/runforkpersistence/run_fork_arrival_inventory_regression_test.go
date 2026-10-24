package runforkpersistence

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/runtime/genericschedule"

	storegenericschedule "github.com/division-sh/swarm/internal/store/internal/backend/genericschedule"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

type arrivalGapProbeOwner struct{ rows []genericschedule.Activation }

func (o *arrivalGapProbeOwner) MaterializeRunForkArrivalJoinScheduleTx(context.Context, *mutationprotocol.Attempt, storegenericschedule.ForkJoinRequest) (genericschedule.Activation, error) {
	return genericschedule.Activation{}, errors.New("require must not write")
}

func (o *arrivalGapProbeOwner) ReadRunForkArrivalJoinScheduleInventoryTx(context.Context, *mutationprotocol.Attempt, string) ([]genericschedule.Activation, error) {
	return o.rows, nil
}
