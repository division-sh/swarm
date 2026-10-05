package pipelinepersistence

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func TestCanonicalEntityWritesRetainSchemaGuardBeforeMutation(t *testing.T) {
	refused := errors.New("schema is not current")
	for _, tc := range []struct {
		name  string
		owner pipeline.WorkflowEngineMutationOwner
	}{
		{"sqlite", &PipelineSQLiteOwner{requireCurrent: func() error { return refused }}},
		{"postgres", &PipelinePostgresOwner{requireCurrent: func() error { return refused }}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := tc.owner.CommitWorkflowEngineMutation(context.Background(), pipeline.WorkflowEngineMutationCommand{})
			if !errors.Is(err, refused) || result.Committed {
				t.Fatalf("schema refusal not preserved: %+v %v", result, err)
			}
		})
	}
	for _, owner := range []pipeline.WorkflowEngineMutationOwner{&PipelineSQLiteOwner{}, &PipelinePostgresOwner{}} {
		result, err := owner.CommitWorkflowEngineMutation(context.Background(), pipeline.WorkflowEngineMutationCommand{})
		if result.Committed || err == nil || !strings.Contains(err.Error(), "owner is required") {
			t.Fatalf("missing schema guard not refused: %+v %v", result, err)
		}
	}
}
