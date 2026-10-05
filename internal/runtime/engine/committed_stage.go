package engine

import (
	"fmt"
	"strings"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
)

// CommittedStage is the exact workflow projection acknowledged by one mutation,
// not a prediction, a later diagnostic read, or another recipient's stage.
type CommittedStage struct {
	Instance     runtimeflowidentity.RunScopedFlowInstance
	EntityID     string
	Stage        string
	StageDefined bool
	Revision     int64
}

func (s CommittedStage) Validate() error {
	if err := s.Instance.Validate(); err != nil {
		return fmt.Errorf("committed stage instance: %w", err)
	}
	if s.EntityID == "" || strings.TrimSpace(s.EntityID) != s.EntityID || s.Stage == "" || strings.TrimSpace(s.Stage) != s.Stage || s.Revision <= 0 {
		return fmt.Errorf("committed stage requires exact entity, stage, and positive revision")
	}
	return nil
}
