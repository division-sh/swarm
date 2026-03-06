package runtime

import (
	"database/sql"

	runtimepipeline "empireai/internal/runtime/pipeline"
)

type FactoryPipelineCoordinator = runtimepipeline.FactoryPipelineCoordinator

func NewFactoryPipelineCoordinator(bus *EventBus, db *sql.DB) *FactoryPipelineCoordinator {
	if bus == nil {
		return nil
	}
	return runtimepipeline.NewFactoryPipelineCoordinator(bus, db)
}
