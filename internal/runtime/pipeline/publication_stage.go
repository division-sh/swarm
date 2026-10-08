package pipeline

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

// WorkflowPublicationStageRequest names the author's own constructed instance,
// independently of the publication's receivers.
type WorkflowPublicationStageRequest struct {
	Instance flowidentity.RunScopedFlowInstance
	// Empty is an entityless producer, not an absent constructed instance.
	EntityID string
}

// WorkflowPublicationStageEvidence is immutable occurrence evidence, not a
// claim that dispatch finished. Acceptance and executed handler facts stay
// separate even when they happen to name the same stage.
type WorkflowPublicationStageEvidence struct {
	Acceptance pipelineobligation.CommittedStageReceipt
	Handlers   []pipelineobligation.CommittedStageReceipt
}

func (e WorkflowPublicationStageEvidence) Validate() error {
	if err := e.Acceptance.Validate(); err != nil {
		return err
	}
	for _, handler := range e.Handlers {
		if err := handler.Validate(); err != nil {
			return err
		}
		if handler.EventID() != e.Acceptance.EventID() || handler.Stage().Instance != e.Acceptance.Stage().Instance || handler.Stage().EntityID != e.Acceptance.Stage().EntityID {
			return fmt.Errorf("publication feedback contains another occurrence or instance")
		}
	}
	return nil
}

func (r WorkflowPublicationStageRequest) ValidateEvent(event events.Event) error {
	if err := r.Instance.Validate(); err != nil {
		return err
	}
	source := event.SourceRoute()
	if strings.TrimSpace(r.EntityID) != r.EntityID || event.RunID() != r.Instance.RunID || source.EntityID != r.EntityID {
		return fmt.Errorf("publication stage feedback requires the exact source instance and entity (event run=%s requested run=%s source entity=%s requested entity=%s)", event.RunID(), r.Instance.RunID, source.EntityID, r.EntityID)
	}
	if event.RoutingSource().Kind() == events.RoutingSourceRoot {
		if r.Instance.Route.ScopeKey != "." || r.Instance.Route.InstanceID != r.Instance.RunID || r.Instance.Route.InstancePath != r.Instance.RunID {
			return fmt.Errorf("root publication stage feedback requires its canonical constructed root")
		}
	} else if source.FlowID != r.Instance.Route.ScopeKey || source.FlowInstance != r.Instance.Route.InstancePath {
		return fmt.Errorf("publication stage feedback requires the exact source path")
	}
	return nil
}
