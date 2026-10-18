package mutationlog

import (
	"context"
	"strings"
	"testing"
	"time"

	runtimemutationlog "github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func TestMutationLogWriterRequiresProtocolAttempt(t *testing.T) {
	ctx := context.Background()
	for name, write := range map[string]func() error{
		"postgres record": func() error { return Insert(ctx, nil, nil, runtimemutationlog.Record{}) },
		"sqlite record":   func() error { return InsertSQLite(ctx, nil, nil, runtimemutationlog.Record{}) },
		"postgres diff": func() error {
			return InsertEntityStateDiff(ctx, nil, nil, "", runtimemutationlog.EntityStateProjection{}, runtimemutationlog.EntityStateProjection{}, runtimemutationlog.Writer{})
		},
		"sqlite diff": func() error {
			return InsertSQLiteEntityStateDiff(ctx, nil, nil, "", runtimemutationlog.EntityStateProjection{}, runtimemutationlog.EntityStateProjection{}, runtimemutationlog.Writer{}, time.Time{})
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := write(); err == nil || !strings.Contains(err.Error(), "mutation attempt is required") {
				t.Fatalf("write without attempt: %v", err)
			}
		})
	}
}

func TestConstructionWriterRequiresLiveAttempt(t *testing.T) {
	err := InsertConstructionEntityStateDiff(context.Background(), nil, nil, pipeline.WorkflowEngineStateRecord{}, runtimemutationlog.EntityStateProjection{}, runtimemutationlog.EntityStateProjection{}, runtimemutationlog.Writer{}, &ConstructionOccurrence{}, true)
	if err == nil || err.Error() != "mutation attempt is not active" {
		t.Fatalf("construction without live attempt: %v", err)
	}
}
