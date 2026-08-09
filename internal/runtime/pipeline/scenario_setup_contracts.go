package pipeline

import (
	"strings"
	"time"
)

type ScenarioSetupRequest struct {
	RunID     string
	Entities  []ScenarioSetupEntityRequest
	CreatedAt time.Time
}

type ScenarioSetupEntityRequest struct {
	Alias        string
	EntityID     string
	FlowInstance string
	EntityType   string
	CurrentState string
	Fields       map[string]any
	Gates        map[string]bool
}

type ScenarioSetupResult struct {
	RunID    string
	Entities []ScenarioSetupEntityResult
}

type ScenarioSetupEntityResult struct {
	Alias        string
	EntityID     string
	FlowInstance string
	EntityType   string
	CurrentState string
}

func (r ScenarioSetupResult) Normalized() ScenarioSetupResult {
	r.RunID = strings.TrimSpace(r.RunID)
	for i := range r.Entities {
		r.Entities[i].Alias = strings.TrimSpace(r.Entities[i].Alias)
		r.Entities[i].EntityID = strings.TrimSpace(r.Entities[i].EntityID)
		r.Entities[i].FlowInstance = strings.Trim(strings.TrimSpace(r.Entities[i].FlowInstance), "/")
		r.Entities[i].EntityType = strings.TrimSpace(r.Entities[i].EntityType)
		r.Entities[i].CurrentState = strings.TrimSpace(r.Entities[i].CurrentState)
	}
	return r
}
