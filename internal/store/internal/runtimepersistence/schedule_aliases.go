package runtimepersistence

import (
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	storetimerobligation "github.com/division-sh/swarm/internal/store/internal/backend/timerobligation"
)

func scheduleAgentIdentityFields(schedule runtimepipeline.Schedule) (agentidentity.StorageFields, error) {
	return storetimerobligation.ScheduleAgentIdentityFields(schedule)
}

func exactScheduleTaskIDSQL() string {
	return storetimerobligation.ExactScheduleTaskIDSQL()
}

func genericScheduleTimerName(schedule runtimepipeline.Schedule) (string, error) {
	return storetimerobligation.GenericScheduleTimerName(schedule)
}

func persistedSchedulePayload(schedule runtimepipeline.Schedule) []byte {
	return storetimerobligation.PersistedSchedulePayload(schedule)
}

func persistedScheduleRoutingSource(schedule runtimepipeline.Schedule) ([]byte, error) {
	return storetimerobligation.PersistedScheduleRoutingSource(schedule)
}
