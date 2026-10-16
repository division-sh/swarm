package runtimepersistence

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestDeploymentResourceDataPortsKeepNativeBytesCountsAndIndependentCutsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, run, version := nativeDeploymentObservationFixture(t, backend)
			probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			for _, read := range []func(context.Context, any) (int, error){
				CountRootReadyResourceVersionsStorageForTest, CountRootReadyResourceHeadHistoryStorageForTest,
				CountGlobalResourceSourceInvocationsStorageForTest, CountGlobalDataCreationOperationsStorageForTest,
				CountGlobalResourceVersionsStorageForTest, CountGlobalResourcePinsStorageForTest,
			} {
				if count, err := read(f.ctx, f.store); err != nil || count != 1 {
					t.Fatalf("native scalar scope or count changed: %d %v", count, err)
				}
			}
			for _, read := range []func(context.Context, any, string) (int, error){CountRunResourcePinsStorageForTest, CountRunDataCreationOperationsStorageForTest} {
				if count, err := read(f.ctx, f.store, run); err != nil || count != 1 {
					t.Fatalf("exact run observation changed: %d %v", count, err)
				}
				if count, err := read(f.ctx, f.store, uuid.NewString()); err != nil || count != 0 {
					t.Fatalf("foreign run borrowed native rows: %d %v", count, err)
				}
			}
			if count, err := CountResourceVersionStorageForTest(f.ctx, f.store, version); err != nil || count != 1 {
				t.Fatalf("exact physical version count changed: %d %v", count, err)
			}
			if meta, err := ReadResourceVersionMetadataStorageForTest(f.ctx, f.store, version); err != nil || meta.BusinessKey != "" || meta.RowCount != 2 {
				t.Fatalf("keyless metadata changed: %+v %v", meta, err)
			}
			if raw, err := ReadResourceVersionJSONLStorageForTest(f.ctx, f.store, version); err != nil || !bytes.Equal(raw, []byte("{\"body\":\"first\"}\n{\"body\":\"second\"}\n")) {
				t.Fatalf("canonical storage bytes changed: %q %v", raw, err)
			}
			if head, err := ReadRootReadyResourceHeadStorageForTest(f.ctx, f.store); err != nil || head.Version != version || head.Revision != 1 {
				t.Fatalf("native coalesced head/revision changed: %+v %v", head, err)
			}
			if head, err := ReadResourceHeadVersionStorageForTest(f.ctx, f.store, durabledata.DeclarationRef{FlowPath: ".", EventName: "root.ready"}); err != nil || head != version {
				t.Fatalf("exact declaration head changed: %q %v", head, err)
			}
			if counts := probe.Snapshot(); counts.Active != 0 || counts.Total.Begun != 15 || counts.Total.ReadCommits != 15 || counts.Total.WriteCommits != 0 {
				t.Fatalf("scalar observations collapsed cuts or wrote data: %+v", counts)
			}
		})
	}
}

func deploymentResourceDataUnavailableCases(run, source, version string) []fanOutSemanticReadCase {
	return []fanOutSemanticReadCase{
		{"portfolio_pin", func(ctx context.Context, owner any) (any, error) {
			return ReadPortfolioResourcePinStorageForTest(ctx, owner, run, "portfolio/account.registered")
		}, "", true},
		{"run_pins", func(ctx context.Context, owner any) (any, error) {
			return CountRunResourcePinsStorageForTest(ctx, owner, run)
		}, 0, false},
		{"run_operation", func(ctx context.Context, owner any) (any, error) {
			return CountRunDataCreationOperationsStorageForTest(ctx, owner, run)
		}, 0, false},
		{"version_count", func(ctx context.Context, owner any) (any, error) {
			return CountResourceVersionStorageForTest(ctx, owner, version)
		}, 0, false},
		{"version_metadata", func(ctx context.Context, owner any) (any, error) {
			return ReadResourceVersionMetadataStorageForTest(ctx, owner, version)
		}, ResourceVersionMetadataStorage{}, true},
		{"version_bytes", func(ctx context.Context, owner any) (any, error) {
			return ReadResourceVersionJSONLStorageForTest(ctx, owner, version)
		}, []byte(nil), true},
		{"root_versions", func(ctx context.Context, owner any) (any, error) {
			return CountRootReadyResourceVersionsStorageForTest(ctx, owner)
		}, 0, false},
		{"root_history", func(ctx context.Context, owner any) (any, error) {
			return CountRootReadyResourceHeadHistoryStorageForTest(ctx, owner)
		}, 0, false},
		{"root_head", func(ctx context.Context, owner any) (any, error) {
			return ReadRootReadyResourceHeadStorageForTest(ctx, owner)
		}, RootReadyResourceHeadStorage{}, true},
		{"head", func(ctx context.Context, owner any) (any, error) {
			return ReadResourceHeadVersionStorageForTest(ctx, owner, durabledata.DeclarationRef{FlowPath: ".", EventName: "root.ready"})
		}, "", true},
		{"source_receipt", func(ctx context.Context, owner any) (any, error) {
			return CountResourceSourceInvocationStorageForTest(ctx, owner, source)
		}, 0, false},
		{"global_sources", func(ctx context.Context, owner any) (any, error) {
			return CountGlobalResourceSourceInvocationsStorageForTest(ctx, owner)
		}, 0, false},
		{"global_operations", func(ctx context.Context, owner any) (any, error) {
			return CountGlobalDataCreationOperationsStorageForTest(ctx, owner)
		}, 0, false},
		{"global_versions", func(ctx context.Context, owner any) (any, error) {
			return CountGlobalResourceVersionsStorageForTest(ctx, owner)
		}, 0, false},
		{"global_pins", func(ctx context.Context, owner any) (any, error) {
			return CountGlobalResourcePinsStorageForTest(ctx, owner)
		}, 0, false},
	}
}

func TestDeploymentResourceDataPortsDiscardEvidenceFromForeignCancelledAndClosedOwnersBothStores(t *testing.T) {
	refuse := func(ctx context.Context, owner any, cases []fanOutSemanticReadCase, cause error) {
		for _, tc := range cases {
			got, err := tc.read(ctx, owner)
			if err == nil || cause != nil && !errors.Is(err, cause) || !reflect.DeepEqual(got, tc.empty) {
				t.Fatalf("unavailable %s returned evidence: %+v %v", tc.name, got, err)
			}
		}
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, run, version := nativeDeploymentObservationFixture(t, backend)
			cases := deploymentResourceDataUnavailableCases(run, uuid.NewString(), version)
			refuse(f.ctx, nil, cases, nil)
			cancelled, cancel := context.WithCancel(f.ctx)
			cancel()
			refuse(cancelled, f.store, cases, context.Canceled)
			if _, err := ReadResourceVersionMetadataStorageForTest(f.ctx, f.store, "invalid"); err == nil {
				t.Fatal("invalid version accepted")
			}
			if _, err := ReadResourceHeadVersionStorageForTest(f.ctx, f.store, durabledata.DeclarationRef{}); err == nil {
				t.Fatal("invalid declaration accepted")
			}
			if err := f.manager.Shutdown(); err != nil {
				t.Fatal(err)
			}
			if err := f.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			refuse(f.ctx, f.store, cases, nil)
		})
	}
}
