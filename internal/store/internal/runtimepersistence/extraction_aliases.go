package runtimepersistence

import (
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	storerunstate "github.com/division-sh/swarm/internal/store/internal/backend/runstate"
)

const (
	runLifecycleActiveStateSQLValues        = storerunstate.ActiveStateSQLValues
	activeRunQuiescencePipelineSubscriberID = "pipeline"
)

func activeRunQuiescenceRunStatusActive(status string) bool {
	state, err := runtimerunlifecycle.ParseState(status)
	return err == nil && state.Active()
}
