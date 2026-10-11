package runtimepersistence_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestDynamicFlowRuntimeReadinessStorageFaultsKeepNativeScopeBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected dynamicFlowSourceProjectionStore
			if backend == "sqlite" {
				selected = storetest.StartSQLiteRuntimeStore(t)
			} else {
				selected = storetest.StartPostgresRuntimeStore(t)
			}
			ctx := testAuthorActivityContext()
			run, foreign := uuid.NewString(), uuid.NewString()
			const path = "account/readiness-storage"
			for _, id := range []string{run, foreign} {
				requireReadinessRun(t, ctx, selected, id, mustExternalStoreTestSourceArtifactFact().BundleHash())
				seedExactFlowInstanceDescriptorOwner(t, selected, id, uuid.NewString(), path, mustExternalStoreTestSourceArtifactFact().BundleHash())
			}
			original, err := storetest.ReadDescriptorReadinessStampStorage(ctx, selected, run, path)
			if err != nil || original.Phase != "planned" || original.PlanWire == "" || original.UpdatedAt.IsZero() {
				t.Fatalf("physical readiness stamp: %+v %v", original, err)
			}
			foreignBefore, err := storetest.ReadDescriptorReadinessStampStorage(ctx, selected, foreign, path)
			if err != nil {
				t.Fatal(err)
			}
			for _, scope := range [][2]string{{uuid.NewString(), path}, {run, "account/absent"}} {
				if changed, err := storetest.SetDescriptorReadinessMalformedPlan(ctx, selected, scope[0], scope[1], json.RawMessage(`{}`)); err == nil || changed != 0 {
					t.Fatalf("absent exact fault returned success: %d %v", changed, err)
				}
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if changed, err := storetest.SetDescriptorReadinessMalformedPlan(cancelled, selected, run, path, json.RawMessage(`{}`)); !errors.Is(err, context.Canceled) || changed != 0 {
				t.Fatalf("cancelled fault returned success: %d %v", changed, err)
			}
			if after, err := storetest.ReadDescriptorReadinessStampStorage(ctx, selected, run, path); err != nil || !reflect.DeepEqual(original, after) {
				t.Fatalf("refused fault changed stamp: before=%+v after=%+v err=%v", original, after, err)
			}
			if value, err := storetest.ReadDescriptorReadinessStampStorage(cancelled, selected, run, path); !errors.Is(err, context.Canceled) || !reflect.DeepEqual(value, storetest.DescriptorReadinessStampStorage{}) {
				t.Fatalf("cancelled read returned provisional evidence: %+v %v", value, err)
			}
			probe := storetest.CollectTransactions(t, selected, storetest.TransactionProbeOptions{})
			if changed, err := storetest.SetDescriptorReadinessMalformedPlan(ctx, selected, run, path, json.RawMessage(`{}`)); err != nil || changed != 1 {
				t.Fatalf("exact malformed-plan fault: %d %v", changed, err)
			}
			after, err := storetest.ReadDescriptorReadinessStampStorage(ctx, selected, run, path)
			want := original
			want.PlanWire = "{}"
			if err != nil || !reflect.DeepEqual(want, after) {
				t.Fatalf("wire fault changed an unselected column: before=%+v after=%+v err=%v", original, after, err)
			}
			foreignAfter, err := storetest.ReadDescriptorReadinessStampStorage(ctx, selected, foreign, path)
			if err != nil || !reflect.DeepEqual(foreignBefore, foreignAfter) {
				t.Fatalf("wire fault changed same-path foreign run: %+v %v", foreignAfter, err)
			}
			counts := probe.Snapshot()
			if counts.Total.Begun != 3 || counts.Total.WriteCommits != 1 || counts.Total.ReadCommits != 2 || counts.Active != 0 {
				t.Fatalf("stamp/fault bypassed original transaction owner: %+v", counts)
			}
			if _, err := storetest.SetDescriptorReadinessMalformedPlan(ctx, selected, run, path, json.RawMessage(original.PlanWire)); err != nil {
				t.Fatal(err)
			}
			cleanup, err := storetest.InstallDescriptorReadinessBatchUpdateFailure(ctx, selected, run, path)
			if err != nil {
				t.Fatal(err)
			}
			active := true
			t.Cleanup(func() {
				if active {
					if err := cleanup(context.Background()); err != nil {
						t.Error(err)
					}
				}
			})
			if _, err := storetest.SetDescriptorReadinessMalformedPlan(ctx, selected, foreign, path, json.RawMessage(foreignBefore.PlanWire)); err != nil {
				t.Fatalf("exact batch fault reached a foreign run: %v", err)
			}
			if changed, err := storetest.SetDescriptorReadinessMalformedPlan(ctx, selected, run, path, json.RawMessage(`{}`)); err == nil || changed != 0 {
				t.Fatalf("exact later UPDATE escaped installed fault: %d %v", changed, err)
			}
			if after, err := storetest.ReadDescriptorReadinessStampStorage(ctx, selected, run, path); err != nil || !reflect.DeepEqual(original, after) {
				t.Fatalf("failed UPDATE changed durable readiness: %+v %v", after, err)
			}
			if err := cleanup(ctx); err != nil {
				t.Fatal(err)
			}
			active = false
			creation := time.Now().UTC().Truncate(time.Microsecond)
			if _, err := storetest.SetDescriptorReadinessCreationClock(ctx, selected, run, path, creation); err != nil {
				t.Fatal(err)
			}
			row, found, err := selected.LoadDynamicFlowRuntimeReadiness(ctx, run, runtimeflowidentity.RouteForInstancePath(path))
			if err != nil || !found || !row.CreationEventEmittedAt.Equal(creation) {
				t.Fatalf("creation fault changed its exact timestamp: %+v %t %v", row, found, err)
			}
			other, found, err := selected.LoadDynamicFlowRuntimeReadiness(ctx, foreign, runtimeflowidentity.RouteForInstancePath(path))
			if err != nil || !found || !other.CreationEventEmittedAt.IsZero() {
				t.Fatalf("creation fault reached same-path foreign run: %+v %t %v", other, found, err)
			}
			if _, err := storetest.RemoveDescriptorReadiness(ctx, selected, run, path); err != nil {
				t.Fatal(err)
			}
			if value, err := storetest.ReadDescriptorReadinessStampStorage(ctx, selected, run, path); err == nil || !reflect.DeepEqual(value, storetest.DescriptorReadinessStampStorage{}) {
				t.Fatalf("absent stamp returned success: %+v %v", value, err)
			}
			if _, err := storetest.ReadDescriptorReadinessStampStorage(ctx, struct{}{}, run, path); err == nil {
				t.Fatal("stamp accepted a foreign owner")
			}
			if err := selected.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if value, err := storetest.ReadDescriptorReadinessStampStorage(ctx, selected, foreign, path); err == nil || !reflect.DeepEqual(value, storetest.DescriptorReadinessStampStorage{}) {
				t.Fatalf("closed read returned provisional evidence: %+v %v", value, err)
			}
			if changed, err := storetest.SetDescriptorReadinessMalformedPlan(ctx, selected, foreign, path, json.RawMessage(`{}`)); err == nil || changed != 0 {
				t.Fatalf("closed fault returned mutation success: %d %v", changed, err)
			}
		})
	}
}
