package selected

import (
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func ManagerDeliveryExecutionForTest(original any) (runforkexecution.SelectedContractExecutionOwner, error) {
	var fork RunFork
	var err error
	switch owner := original.(type) {
	case *private.PostgresStore:
		if owner == nil {
			return runforkexecution.SelectedContractExecutionOwner{}, fmt.Errorf("original PostgreSQL manager delivery owner is required")
		}
		fork, err = newPostgresRunFork(owner, pipeline.NewWorkflowPersistence(owner))
	case *private.SQLiteRuntimeStore:
		if owner == nil {
			return runforkexecution.SelectedContractExecutionOwner{}, fmt.Errorf("original SQLite manager delivery owner is required")
		}
		fork, err = newSQLiteRunFork(owner, pipeline.NewWorkflowPersistence(owner))
	default:
		return runforkexecution.SelectedContractExecutionOwner{}, fmt.Errorf("manager delivery fork requires the original selected owner, got %T", original)
	}
	if err != nil {
		return runforkexecution.SelectedContractExecutionOwner{}, err
	}
	return fork.executionOwner, nil
}
