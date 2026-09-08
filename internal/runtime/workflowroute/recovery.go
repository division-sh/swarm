package workflowroute

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
)

type RecoveryReader interface {
	LoadActiveWorkflowRoute(context.Context, runtimeflowidentity.RunScopedFlowInstance) (RecoveryRecord, error)
}

type RecoveryRecord struct {
	WorkflowName string
	EntityID     string
	Config       json.RawMessage
}

type ActiveRouteNotFound struct {
	InstancePath string
}

func (e *ActiveRouteNotFound) Error() string {
	return fmt.Sprintf("active flow instance not found for route recovery: %s", strings.TrimSpace(e.InstancePath))
}
