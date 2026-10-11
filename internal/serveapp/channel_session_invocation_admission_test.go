//go:build linux || darwin

package serveapp

import (
	"context"
	"os"
	"testing"

	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/sessionprovider"
	"github.com/google/uuid"
)

func TestServeNativeInvocationStateRejectsUnadmittedConstructionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newServeBootstrapTestFixture(t, backend)
			parent := t.TempDir()
			t.Setenv("TMPDIR", parent)
			state, err := sessionprovider.NewInvocationState(worklifetime.NewProcess())
			if err != nil {
				t.Fatal(err)
			}
			for _, kind := range []string{"unowned_context", "missing_reservation", "missing_plan", "foreign_process", "ambiguous_lifetime"} {
				ctx := f.owned
				options := sessionprovider.RuntimeConnectionOptions{InvocationState: state,
					OperationID: f.op.OperationID, Store: f.adapter.store, Plan: f.candidate.Plan}
				switch kind {
				case "unowned_context":
					ctx = context.Background()
				case "missing_reservation":
					options.OperationID = uuid.NewString()
				case "missing_plan":
					options.Plan = packs.SatisfactionPlan{}
				case "ambiguous_lifetime":
					options.Directory = f.adapter.directory
				}
				connection, err := sessionprovider.OpenRuntimeBootstrap(ctx, options)
				if connection != nil {
					t.Error("unadmitted construction returned a connection", kind)
					if err := connection.Close(f.ctx); err != nil {
						t.Error(err)
					}
				}
				if err == nil || f.owner.ActiveCount() != 0 {
					t.Fatal("unadmitted construction succeeded or stranded counted work", kind, err, f.owner.ActiveCount())
				}
				if entries, err := os.ReadDir(parent); err != nil || len(entries) != 0 {
					t.Fatal("unadmitted construction allocated native state", kind, entries, err)
				}
			}
		})
	}
}
