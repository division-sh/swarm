package events

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// InstanceLife is copied continuation evidence, not a new instance identity or
// permission to mutate it. Native owners compare it with the selected header.
type InstanceLife struct {
	RunID        string `json:"run_id"`
	FlowInstance string `json:"flow_instance"`
	Life         int64  `json:"life"`
}

func (l InstanceLife) Validate() error {
	id, err := uuid.Parse(l.RunID)
	if err != nil || id == uuid.Nil || id.String() != l.RunID || l.Life <= 0 ||
		l.FlowInstance == "" || l.FlowInstance != strings.Trim(strings.TrimSpace(l.FlowInstance), "/") {
		return fmt.Errorf("continuation requires its exact run, instance and positive captured life")
	}
	return nil
}

func (s RoutingSource) WithInstanceLife(runID string, life InstanceLife) (RoutingSource, error) {
	if err := life.Validate(); err != nil {
		return RoutingSource{}, err
	}
	path := s.Route().FlowInstance
	if s.Kind() == RoutingSourceRoot {
		path = runID
	}
	if life.RunID != runID || life.FlowInstance != path {
		return RoutingSource{}, fmt.Errorf("routing source life crosses its exact execution coordinate")
	}
	return s.WithLife(life.Life)
}
