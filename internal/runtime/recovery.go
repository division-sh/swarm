package runtime

import runtimepipeline "empireai/internal/runtime/pipeline"

type RecoveryManager = runtimepipeline.RecoveryManager

func NewRecoveryManager() *RecoveryManager {
	return runtimepipeline.NewRecoveryManager()
}

func NewRecoveryManagerWith(store EventStore, bus *EventBus) *RecoveryManager {
	return runtimepipeline.NewRecoveryManagerWith(store, bus)
}
