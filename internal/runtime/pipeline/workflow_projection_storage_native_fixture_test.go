package pipeline

import (
	"context"
	"encoding/json"
)

type WorkflowProjectionStorageNativeFixtureForTest struct {
	WorkflowProjectionNativeFixtureForTest
	ReadControl   func(context.Context, string, string) (WorkflowControlProjectionStorageForTest, error)
	ReadDuplicate func(context.Context, string, string) (WorkflowDuplicateProjectionStorageForTest, error)
}

type WorkflowControlProjectionStorageForTest struct {
	CurrentState, ControlStatus string
	Fields                      json.RawMessage
}

type WorkflowDuplicateProjectionStorageForTest struct {
	Revision  int
	FieldName string
	Fields    json.RawMessage
}
