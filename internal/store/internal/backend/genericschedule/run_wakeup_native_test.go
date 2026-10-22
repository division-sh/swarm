package genericschedule_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
)

func TestForkJoinWakeupCensusIsChildOnlyAndNonmintingBothStores(t *testing.T) {
	for _, dialect := range []string{"sqlite", "postgres"} {
		t.Run(dialect, func(t *testing.T) {
			f := openForkJoinNativeFixture(t, dialect)
			ctx, request := forkJoinNativeRequest(t, f, "orders", true, false)
			child := forkJoinNativeRestore(t, f, ctx, request)
			reader := f.generic.(interface {
				ListActiveGenericScheduleActivationsForRun(context.Context, string) ([]runtimegenericschedule.Activation, error)
			})
			for _, run := range []struct {
				id  string
				row runtimegenericschedule.Activation
			}{
				{request.Child.RunID, child}, {request.Source.Command.RunID, request.Source},
			} {
				rows, err := reader.ListActiveGenericScheduleActivationsForRun(correlation.WithRunID(ctx, run.id), run.id)
				if err != nil || len(rows) != 1 || !reflect.DeepEqual(rows[0].Canonical(), run.row.Canonical()) {
					t.Fatalf("exact run census: run=%s rows=%v err=%v", run.id, rows, err)
				}
			}
			if rows, err := reader.ListActiveGenericScheduleActivationsForRun(ctx, ""); err == nil || rows != nil {
				t.Fatal("empty run became a global schedule scan")
			}
			rows, err := reader.ListActiveGenericScheduleActivationsForRun(correlation.WithRunID(ctx, request.Source.Command.RunID), request.Child.RunID)
			if err == nil && len(rows) != 0 {
				t.Fatal("foreign actor context selected child work")
			}
			if actual, found, err := f.generic.LoadGenericScheduleActivation(ctx, child.ID); err != nil || !found || !reflect.DeepEqual(actual.Canonical(), child.Canonical()) {
				t.Fatalf("census changed inherited child history: found=%t err=%v", found, err)
			}
		})
	}
}
