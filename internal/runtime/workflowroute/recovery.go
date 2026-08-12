package workflowroute

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

type RecoveryReader interface {
	LoadActiveWorkflowRoute(context.Context, string) (RecoveryRecord, error)
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
