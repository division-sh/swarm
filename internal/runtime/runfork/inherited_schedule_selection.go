package runfork

import (
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
)

// Both families consume the same sealed selected readiness, while retaining
// their distinct native projection and lifecycle algorithms.
type InheritedScheduleSelection interface {
	InheritedWorkflowTimerSelection
	SelectInheritedArrivalJoin(genericschedule.Activation) (genericschedule.ForkJoinDisposition, error)
	RemovedInheritedArrivalRefs() ([]timeridentity.JoinRef, error)
}
