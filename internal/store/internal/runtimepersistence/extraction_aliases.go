package runtimepersistence

import (
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	storepipeline "github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	storerunstate "github.com/division-sh/swarm/internal/store/internal/backend/runstate"
)

const (
	runLifecycleActiveStateSQLValues        = storerunstate.ActiveStateSQLValues
	activeRunQuiescencePipelineSubscriberID = "pipeline"
)

type ScenarioSetupRequest = storepipeline.ScenarioSetupRequest
type ScenarioSetupEntityRequest = storepipeline.ScenarioSetupEntityRequest
type ScenarioSetupEntityResult = storepipeline.ScenarioSetupEntityResult
type ScenarioSetupResult = storepipeline.ScenarioSetupResult

func activeRunQuiescenceRunStatusActive(status string) bool {
	state, err := runtimerunlifecycle.ParseState(status)
	return err == nil && state.Active()
}
