package runfork

import (
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
)

// ProjectFanOutBarrierRegistration projects only admitted owner correspondence.
// Publication evidence, schedule admission and execution authority stay separate.
func ProjectFanOutBarrierRegistration(source fanoutbarrier.Registration, childRunID string, selected contracts.FanOutPlanRef, generation *loopruntime.ForkChildReference, at time.Time) (fanoutbarrier.Registration, error) {
	if err := source.Validate(); err != nil {
		return fanoutbarrier.Registration{}, err
	}
	if at.IsZero() || selected.ElementRef != source.IntentKey.ElementRef {
		return fanoutbarrier.Registration{}, fmt.Errorf("fork barrier requires its exact selected declaration and creation time")
	}
	entity, err := ProjectEntityOwnership(source.IntentKey.RunID, childRunID, source.EntityID, source.Route.InstancePath)
	if err != nil {
		return fanoutbarrier.Registration{}, err
	}
	ref, _ := source.Handle.JoinRef()
	childGeneration, err := projectFanOutBarrierGeneration(ref.Generation(), generation, childRunID, entity.Fork.EntityID)
	if err != nil {
		return fanoutbarrier.Registration{}, err
	}
	declaration, err := selected.ElementRef.DeclarationIdentity()
	if err != nil {
		return fanoutbarrier.Registration{}, err
	}
	childRef, err := timeridentity.NewFanOutDeliveryJoinRef(ref.Node(), ref.HandlerEvent(), ref.JoinID(), declaration, selected.BundleHash, selected.SemanticDigest)
	if err != nil {
		return fanoutbarrier.Registration{}, err
	}
	childRef, err = childRef.BindFanOutIntent(source.IntentKey.TriggeringDeliveryID, childGeneration)
	if err != nil {
		return fanoutbarrier.Registration{}, err
	}
	child := source
	child.Handle, err = timeridentity.JoinCompleteHandle(childRef)
	if err != nil {
		return fanoutbarrier.Registration{}, err
	}
	child.Route, err = ProjectExecutionRoute(source.IntentKey.RunID, childRunID, ref.FlowPath(), source.Route)
	if err != nil {
		return fanoutbarrier.Registration{}, err
	}
	child.RoutingSource, err = ProjectProducerOwnership(source.IntentKey.RunID, childRunID, source.RoutingSource)
	if err != nil {
		return fanoutbarrier.Registration{}, err
	}
	child.EntityID, child.IntentKey.RunID = entity.Fork.EntityID, childRunID
	child.IntentKey.ElementRef, child.PlanRef, child.CreatedAt = selected.ElementRef, selected, at.UTC()
	return child, child.Validate()
}

func projectFanOutBarrierGeneration(source attemptgeneration.Generation, correspondence *loopruntime.ForkChildReference, childRunID, childEntityID string) (attemptgeneration.Generation, error) {
	if correspondence == nil {
		if source != (attemptgeneration.Generation{}) {
			return attemptgeneration.Generation{}, fmt.Errorf("fork barrier requires its admitted child generation")
		}
		return attemptgeneration.Generation{}, nil
	}
	if correspondence.Source().Generation() != source {
		return attemptgeneration.Generation{}, fmt.Errorf("fork barrier source generation contradicts admitted correspondence")
	}
	if err := correspondence.RequireDestination(childRunID, childEntityID); err != nil {
		return attemptgeneration.Generation{}, err
	}
	return correspondence.Generation(), nil
}
