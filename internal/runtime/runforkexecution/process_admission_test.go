package runforkexecution

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type crossedSelectedProcessCapability struct {
	startupownership.ProcessCapability
	cross func(*startupownership.Authority)
}

func (c crossedSelectedProcessCapability) Evidence() (startupownership.Authority, error) {
	evidence, err := c.ProcessCapability.Evidence()
	if err == nil {
		c.cross(&evidence)
	}
	return evidence, err
}

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
			if err := owner.requirePreparationProcess(ctx, capability); err != nil {
				t.Fatalf("exact bound capability: %v", err)
			}
			if err := owner.requirePreparationProcess(ctx, nil); err == nil {
				t.Fatal("missing supplied capability admitted")
			}
			for _, axis := range []struct {
				name  string
				cross func(*startupownership.Authority)
			}{
				{"authority", func(a *startupownership.Authority) { a.AuthorityID = uuid.NewString() }},
				{"generation", func(a *startupownership.Authority) { a.AuthorityGeneration++ }},
				{"acquisition", func(a *startupownership.Authority) { a.AcquisitionID = uuid.NewString() }},
				{"owner", func(a *startupownership.Authority) { a.OwnerID += "-foreign" }},
				{"boot", func(a *startupownership.Authority) { a.BootID = uuid.NewString() }},
				{"runtime", func(a *startupownership.Authority) { a.RuntimeInstanceID = uuid.NewString() }},
				{"backend", func(a *startupownership.Authority) { a.Backend += "-foreign" }},
			} {
				t.Run(axis.name, func(t *testing.T) {
					err := owner.requirePreparationProcess(ctx, crossedSelectedProcessCapability{ProcessCapability: capability, cross: axis.cross})
					if err == nil || !strings.Contains(err.Error(), "differs from its bound owner") {
						t.Fatalf("crossed %s accepted: %v", axis.name, err)
					}
				})
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if err := owner.requirePreparationProcess(cancelled, capability); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled process validation: %v", err)
			}
			if err := capability.Release(context.Background()); err != nil {
				t.Fatal(err)
			}
			successor := selectedContractTestProcessCapability(t, ctx, selected)
			if err := owner.requirePreparationProcess(ctx, successor); err == nil {
				t.Fatal("successor capability revived predecessor selected owner")
			}
			if err := successor.ProveCurrent(ctx); err != nil {
				t.Fatalf("predecessor refusal invalidated successor: %v", err)
			}
			if process.ActiveCount() != baseline {
				t.Fatal("capability refusals leaked process work")
			}
		})
	}
}
