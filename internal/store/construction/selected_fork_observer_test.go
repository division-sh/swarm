package construction_test

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/store/construction"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

// The complete control belongs to independent construction, not to a runtime
// consumer reconstructing an observer around its writer's connection.
func TestSelectedForkApplicationSnapshotObserverDoesNotAcquireRuntimeAuthorityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			var observer any
			var runtimeCall func() error
			const unacceptedEvent = "11111111-1111-4111-8111-111111111111"
			if backend == "sqlite" {
				writer := storetest.StartSQLiteRuntimeStore(t)
				before, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, writer)
				if err != nil {
					t.Fatal(err)
				}
				selected, err := construction.OpenSQLiteRuntimeReadOnly(writer.Path())
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := selected.Close(); err != nil {
						t.Error(err)
					}
				})
				observer = selected
				runtimeCall = func() error {
					_, err := selected.ListActiveAgentDescriptors(ctx, unacceptedEvent)
					return err
				}
				if err := private.AttemptSQLiteInspectionTableCreationForTest(ctx, selected); err == nil {
					t.Fatal("read-only physical observer acquired write authority")
				}
				after, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, observer)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("read-only observer changed or lost physical evidence: err=%v", err)
				}
			} else {
				selected, err := construction.NewPostgres(testutil.StartPostgresDSN(t))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := selected.Close(); err != nil {
						t.Error(err)
					}
				})
				observer = selected
				runtimeCall = func() error {
					_, err := selected.ListActiveAgentDescriptors(ctx, unacceptedEvent)
					return err
				}
			}
			requireUnaccepted := func() {
				t.Helper()
				if err := runtimeCall(); err == nil || !strings.Contains(err.Error(), "schema is unaccepted") {
					t.Fatalf("runtime call error=%v, want unaccepted admission failure", err)
				}
			}
			requireUnaccepted()
			before, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, observer)
			if err != nil || len(before["events"].Columns) == 0 {
				t.Fatalf("physical observer could not inspect native storage: err=%v", err)
			}
			requireUnaccepted()
			after, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, observer)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("runtime refusal changed inspection evidence: err=%v", err)
			}
		})
	}
}
