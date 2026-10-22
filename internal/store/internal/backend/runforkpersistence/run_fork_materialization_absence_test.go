package runforkpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/google/uuid"
)

func TestExactForkAbsenceDoesNotHideIndependentReadFailures(t *testing.T) {
	child := uuid.NewString()
	absent := &runlifecycle.RunNotFoundError{RunID: child}
	fault := errors.New("independent snapshot read failure")
	for _, tc := range []struct {
		name string
		err  error
		want error
	}{
		{"absent", absent, nil},
		{"wrapped absence", fmt.Errorf("read child: %w", absent), nil},
		{"independent read", errors.Join(absent, fault), fault},
		{"cancelled read", errors.Join(absent, context.Canceled), context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, found, err := loadExactRunForkMaterialization(context.Background(),
				func(context.Context, *sql.Tx, string) (runlifecycle.Snapshot, error) {
					return runlifecycle.Snapshot{}, tc.err
				}, nil, false, child, runfork.RunForkPlan{}, runForkBundleInsertIdentity{}, nil)
			if found || (tc.want == nil && err != nil) || (tc.want != nil && !errors.Is(err, tc.want)) {
				t.Fatalf("read disposition: found=%t err=%v want=%v", found, err, tc.want)
			}
		})
	}
}
