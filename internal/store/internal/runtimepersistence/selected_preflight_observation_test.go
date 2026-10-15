package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestSelectedPreflightGlobalCountsAndSurfaceUseOriginalReadOwnerBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			selected, ctx := backend.open(t).store, context.Background()
			probe, restore, err := InstallTransactionProbeForTest(selected, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			readers := []func(context.Context, any) (int, error){
				CountGlobalRunStorageForTest, CountGlobalAgentStorageForTest,
				CountGlobalSelectedRuntimeExecutionStorageForTest, CountGlobalGenerationGrantStorageForTest,
			}
			for _, read := range readers {
				if got, err := read(ctx, selected); err != nil || got != 0 {
					t.Fatalf("runless store supplied executable state: %d/%v", got, err)
				}
			}
			if counts := probe.Snapshot(); counts.Total.ReadCommits != 4 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("global zero witnesses escaped original owner or collapsed cuts: %+v", counts)
			}
			surface, preparation := uuid.NewString(), uuid.NewString()
			readSurface := func(ctx context.Context, owner any) ([]byte, error) {
				return ReadRunlessSelectedPreparationSurfaceStorageForTest(ctx, owner, surface, preparation, "selected-health-agent")
			}
			if raw, err := readSurface(ctx, selected); !errors.Is(err, sql.ErrNoRows) || raw != nil {
				t.Fatalf("missing preparation supplied provisional surface: %q/%v", raw, err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if raw, err := readSurface(cancelled, selected); !errors.Is(err, context.Canceled) || raw != nil {
				t.Fatalf("cancelled preparation supplied evidence: %q/%v", raw, err)
			}
			for _, owner := range []any{nil, struct{}{}, &sql.DB{}} {
				if raw, err := readSurface(ctx, owner); err == nil || raw != nil {
					t.Fatalf("raw/foreign preparation owner supplied evidence: %T/%q/%v", owner, raw, err)
				}
			}
			for _, invalid := range []string{"", "invalid", uuid.Nil.String()} {
				if raw, err := ReadRunlessSelectedPreparationSurfaceStorageForTest(ctx, selected, invalid, preparation, "selected-health-agent"); err == nil || raw != nil {
					t.Fatalf("invalid surface coordinate supplied evidence: %q/%v", raw, err)
				}
				if raw, err := ReadRunlessSelectedPreparationSurfaceStorageForTest(ctx, selected, surface, invalid, "selected-health-agent"); err == nil || raw != nil {
					t.Fatalf("invalid preparation coordinate supplied evidence: %q/%v", raw, err)
				}
			}
			for _, invalid := range []string{"", " selected-health-agent", "selected-health-agent "} {
				if raw, err := ReadRunlessSelectedPreparationSurfaceStorageForTest(ctx, selected, surface, preparation, invalid); err == nil || raw != nil {
					t.Fatalf("invalid actor coordinate supplied evidence: %q/%v", raw, err)
				}
			}
			for _, read := range readers {
				if got, err := read(cancelled, selected); !errors.Is(err, context.Canceled) || got != 0 {
					t.Fatalf("cancelled global witness supplied evidence: %d/%v", got, err)
				}
				for _, owner := range []any{nil, struct{}{}, &sql.DB{}} {
					if got, err := read(ctx, owner); err == nil || got != 0 {
						t.Fatalf("foreign/raw global witness supplied evidence: %T/%d/%v", owner, got, err)
					}
				}
			}
			if err := selected.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			for _, read := range readers {
				if got, err := read(ctx, selected); err == nil || got != 0 {
					t.Fatalf("closed global witness supplied evidence: %d/%v", got, err)
				}
			}
			if raw, err := readSurface(ctx, selected); err == nil || raw != nil {
				t.Fatalf("closed preparation supplied evidence: %q/%v", raw, err)
			}
		})
	}
}
