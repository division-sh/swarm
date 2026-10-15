package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
)

func deploymentImportShapeReader(t *testing.T, selected any) interface {
	GetDeclarationImportShape(context.Context, string, durabledata.DeclarationRef) (durabledata.ImportShape, error)
} {
	t.Helper()
	reader, ok := selected.(interface {
		GetDeclarationImportShape(context.Context, string, durabledata.DeclarationRef) (durabledata.ImportShape, error)
	})
	if !ok {
		t.Fatal("native selected owner lacks its import-shape reader")
	}
	return reader
}

func TestDeploymentImportShapeFaultRetainsExactBundleAndDeclarationScopeBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, _, _ := nativeDeploymentObservationFixture(t, backend)
			reader := deploymentImportShapeReader(t, f.store)
			hash := f.bundle.SourceArtifact.BundleHash()
			ref := durabledata.DeclarationRef{FlowPath: ".", EventName: "root.ready"}
			before, err := reader.GetDeclarationImportShape(f.ctx, hash, ref)
			if err != nil {
				t.Fatal(err)
			}
			if err := CorruptRootReadyImportShapeFaultForTest(f.ctx, f.store, "bundle-v2:sha256:"+strings.Repeat("f", 64)); err != nil {
				t.Fatal(err)
			}
			cancelled, cancel := context.WithCancel(f.ctx)
			cancel()
			if err := CorruptRootReadyImportShapeFaultForTest(cancelled, f.store, hash); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled named fault did not refuse: %v", err)
			}
			if err := CorruptRootReadyImportShapeFaultForTest(f.ctx, nil, hash); err == nil {
				t.Fatal("foreign owner accepted")
			}
			if err := CorruptRootReadyImportShapeFaultForTest(f.ctx, f.store, "invalid"); err == nil {
				t.Fatal("invalid bundle accepted")
			}
			if after, err := reader.GetDeclarationImportShape(f.ctx, hash, ref); err != nil || !reflect.DeepEqual(after, before) {
				t.Fatalf("foreign/cancelled fault mutated live shape: %+v %v", after, err)
			}
			if err := CorruptRootReadyImportShapeFaultForTest(f.ctx, f.store, hash); err != nil {
				t.Fatal(err)
			}
			var domain *durabledata.DomainError
			if _, err := reader.GetDeclarationImportShape(f.ctx, hash, ref); !errors.As(err, &domain) || domain.Code != durabledata.CodeIntegrity {
				t.Fatalf("fixed malformed shape did not reach the real integrity refusal: %v", err)
			}
		})
	}
}

func TestDeploymentImportShapeFaultUsesOriginalSQLiteWriterAdmissionAndCancelsWithoutMutation(t *testing.T) {
	f, _, _ := nativeDeploymentObservationFixture(t, "sqlite")
	owner := f.store.(*SQLiteRuntimeStore)
	hash := f.bundle.SourceArtifact.BundleHash()
	ref := durabledata.DeclarationRef{FlowPath: ".", EventName: "root.ready"}
	reader := deploymentImportShapeReader(t, owner)
	before, err := reader.GetDeclarationImportShape(f.ctx, hash, ref)
	if err != nil {
		t.Fatal(err)
	}
	probe, restore, err := InstallTransactionProbeForTest(owner, transactiontest.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer restore()
	ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	holderDone := make(chan error, 1)
	go func() {
		holderDone <- owner.backend.RunTransaction(ctx, "deployment import-shape fault admission proof", func(context.Context, *sql.Tx) error {
			close(entered)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	holderJoined := false
	defer func() {
		unblock()
		if !holderJoined {
			if err := <-holderDone; err != nil {
				t.Errorf("joined writer holder: %v", err)
			}
		}
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	faultCtx, cancelFault := context.WithCancel(ctx)
	defer cancelFault()
	faultDone := make(chan error, 1)
	go func() { faultDone <- CorruptRootReadyImportShapeFaultForTest(faultCtx, owner, hash) }()
	faultJoined := false
	defer func() {
		cancelFault()
		unblock()
		if !faultJoined {
			<-faultDone
		}
	}()
	waitDeploymentShapeFaultQueued(t, ctx, owner, faultDone, &faultJoined)
	if counts := probe.Snapshot(); counts.Total.Begun != 0 || counts.Total.WriteCommits != 0 || counts.Active != 1 {
		t.Fatalf("fault bypassed the original writer permit: %+v", counts)
	}
	cancelFault()
	select {
	case err := <-faultDone:
		faultJoined = true
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("queued cancellation did not settle: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if owner.backend.QueuedWritersForTest() != 0 {
		t.Fatal("cancelled fault retained writer admission")
	}
	unblock()
	if err := <-holderDone; err != nil {
		holderJoined = true
		t.Fatal(err)
	}
	holderJoined = true
	if after, err := reader.GetDeclarationImportShape(f.ctx, hash, ref); err != nil || !reflect.DeepEqual(after, before) {
		t.Fatalf("queued cancelled fault mutated the complete shape: %+v %v", after, err)
	}
}

func waitDeploymentShapeFaultQueued(t *testing.T, ctx context.Context, owner *SQLiteRuntimeStore, done <-chan error, joined *bool) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for owner.backend.QueuedWritersForTest() != 1 {
		select {
		case err := <-done:
			*joined = true
			t.Fatalf("fault finished before original writer admission: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
		}
	}
}
