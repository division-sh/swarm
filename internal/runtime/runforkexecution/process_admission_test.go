package runforkexecution

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestSelectedForkProcessAdmissionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var owner SelectedContractExecutionOwner
			var selected startupownership.Store
			if backend == "sqlite" {
				s := storetest.StartSQLiteRuntimeStore(t)
				selected, owner = s, newSelectedContractSQLiteExecutionOwnerForTest(t, s)
			} else {
				_, db, _ := testutil.StartPostgres(t)
				s := storetest.AdmitPostgresRuntimeStore(t, db)
				selected, owner = s, newSelectedContractExecutionOwnerForTest(t, s)
			}
			ctx := runForkTestContext(t)
			process, _ := worklifetime.ProcessFromContext(ctx)
			capability := selectedContractTestProcessCapability(t, ctx, selected)
			baseline := process.ActiveCount()
			requireRefusal := func(ctx context.Context) {
				t.Helper()
				op, err := owner.beginPreparation(ctx)
				if op != nil || err == nil || !strings.Contains(err.Error(), "reconciled process owner") {
					t.Fatalf("admission bypassed process composition: operation=%v err=%v", op, err)
				}
				if process.ActiveCount() != baseline {
					t.Fatal("refused preparation leaked process possession")
				}
			}
			requireRefusal(ctx)
			if err := owner.BindSelectedProcess(ctx, process, capability); err != nil {
				t.Fatal(err)
			}
			requireRefusal(ctx)
			if _, err := owner.RecoverSelectedForkContexts(ctx, effects.NewRecoveryRequest(time.Now().UTC(), executionposture.MockOnly)); err != nil {
				t.Fatal(err)
			}
			foreign := worklifetime.NewProcess()
			requireRefusal(worklifetime.WithProcess(ctx, foreign))
			if foreign.ActiveCount() != 0 {
				t.Fatal("foreign preparation leaked its process possession")
			}
			foreign.Retire()
			if _, err := foreign.Join(context.Background()); err != nil {
				t.Fatal(err)
			}
			op, err := owner.beginPreparation(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := (&PreparedSelectedFork{owner: owner, operation: op}).Close(); err != nil {
				t.Fatal(err)
			}
			if process.ActiveCount() != baseline {
				t.Fatal("accepted preparation did not release exact process possession")
			}
		})
	}
}
