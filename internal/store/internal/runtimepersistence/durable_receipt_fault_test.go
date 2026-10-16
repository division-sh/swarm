package runtimepersistence

import (
	"context"
	"database/sql/driver"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	runtimedata "github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestResourceReceiptFaultReconcilesCommitUncertaintyBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, kind := range []string{"receipt", "payload", "provenance", "clock", "added-provenance", "head-history"} {
			for _, cut := range []string{"install_committed", "install_rolled_back", "restore_committed", "restore_rolled_back"} {
				t.Run(backend+"/"+kind+"/"+cut, func(t *testing.T) {
					proveResourceReceiptCommitCut(t, backend, kind, cut)
				})
			}
		}
	}
}

func proveResourceReceiptCommitCut(t *testing.T, backend, kind, cut string) {
	t.Helper()
	selected, source, version, connector := resourceReceiptFaultFixture(t, backend)
	ctx := testAuthorActivityContext()
	fault := captureResourceFaultControl(t, ctx, selected, source, version, kind)
	original := resourceFaultSnapshot(t, ctx, selected)
	operation := fault.InstallForTest
	if cut == "restore_committed" || cut == "restore_rolled_back" {
		if err := fault.InstallForTest(ctx); err != nil {
			t.Fatal(err)
		}
		operation = fault.RestoreForTest
	}
	cause, commits := errors.New("resource receipt COMMIT acknowledgement lost"), 0
	connector.arm(func(tx driver.Tx) error {
		commits++
		if cut == "install_rolled_back" || cut == "restore_rolled_back" {
			return errors.Join(cause, tx.Rollback())
		}
		return errors.Join(cause, tx.Commit())
	})
	if err := operation(ctx); !errors.Is(err, cause) {
		t.Fatalf("lost original acknowledgement error: %v", err)
	}
	unchanged := reflect.DeepEqual(original, resourceFaultSnapshot(t, ctx, selected))
	if want := cut == "install_rolled_back" || cut == "restore_committed"; unchanged != want || commits != 1 {
		t.Fatalf("COMMIT cut unchanged=%t want=%t commits=%d", unchanged, want, commits)
	}
	for attempt := range 2 {
		if err := fault.RestoreForTest(ctx); err != nil {
			t.Fatalf("exact cleanup %d: %v", attempt, err)
		}
		if !reflect.DeepEqual(original, resourceFaultSnapshot(t, ctx, selected)) {
			t.Fatal("cleanup changed an original physical field or row multiplicity")
		}
	}
}

func TestResourceReceiptFaultRefusesCancellationAndReplacementBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			proveResourceReceiptCancellationAndReplacement(t, backend)
		})
	}
}

func proveResourceReceiptCancellationAndReplacement(t *testing.T, backend string) {
	t.Helper()
	selected, source, _, _ := resourceReceiptFaultFixture(t, backend)
	ctx := testAuthorActivityContext()
	fault := captureResourceFaultControl(t, ctx, selected, source, "", "receipt")
	original := resourceFaultSnapshot(t, ctx, selected)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := fault.InstallForTest(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled installation: %v", err)
	}
	if err := fault.RestoreForTest(ctx); err != nil || !reflect.DeepEqual(original, resourceFaultSnapshot(t, ctx, selected)) {
		t.Fatalf("refused installation lost exact original: %v", err)
	}
	if err := fault.InstallForTest(ctx); err != nil {
		t.Fatal(err)
	}
	replacement, err := CaptureResourceReceiptFaultForTest(ctx, selected, source, "", "source/typed actor")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := replacement.RestoreForTest(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if err := replacement.InstallForTest(ctx); err != nil {
		t.Fatal(err)
	}
	foreign := resourceFaultSnapshot(t, ctx, selected)
	if err := fault.RestoreForTest(ctx); err == nil || !reflect.DeepEqual(foreign, resourceFaultSnapshot(t, ctx, selected)) {
		t.Fatalf("cleanup overwrote a foreign replacement: %v", err)
	}
	if err := replacement.RestoreForTest(ctx); err != nil {
		t.Fatal(err)
	}
	if err := fault.RestoreForTest(ctx); err != nil || !reflect.DeepEqual(original, resourceFaultSnapshot(t, ctx, selected)) {
		t.Fatalf("exact replacement reconciliation: %v", err)
	}
}

func TestResourceReceiptFaultUsesOriginalSQLiteWriterAdmission(t *testing.T) {
	owner, source, version, connector := resourceReceiptFaultFixture(t, "sqlite")
	selected := owner.(*SQLiteRuntimeStore)
	ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 10*time.Second)
	defer cancel()
	holder := captureResourceFaultControl(t, ctx, selected, source, version, "receipt")
	fault := captureResourceFaultControl(t, ctx, selected, source, version, "payload")
	original := resourceFaultSnapshot(t, ctx, selected)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unlock := func() { releaseOnce.Do(func() { close(release) }) }
	connector.arm(func(tx driver.Tx) error {
		close(entered)
		select {
		case <-release:
			return tx.Commit()
		case <-ctx.Done():
			return errors.Join(ctx.Err(), tx.Rollback())
		}
	})
	done := make(chan error, 1)
	go func() { done <- holder.InstallForTest(ctx) }()
	joined := false
	defer func() {
		unlock()
		if !joined {
			if err := <-done; err != nil {
				t.Error(err)
			}
		}
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	proveResourceReceiptQueuedCancellation(t, ctx, selected, fault)
	unlock()
	err := <-done
	joined = true
	if err != nil {
		t.Fatal(err)
	}
	if err := holder.RestoreForTest(ctx); err != nil || !reflect.DeepEqual(original, resourceFaultSnapshot(t, ctx, selected)) {
		t.Fatalf("writer admission control lost the exact original: %v", err)
	}
}

func proveResourceReceiptQueuedCancellation(t *testing.T, ctx context.Context, selected *SQLiteRuntimeStore, fault *ResourceReceiptFault) {
	t.Helper()
	queued, cancel := context.WithCancel(ctx)
	cut := &durableFixtureAdmissionCut{Context: queued, entered: make(chan struct{}), resume: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- fault.InstallForTest(cut) }()
	joined := false
	var once sync.Once
	resume := func() { once.Do(func() { close(cut.resume) }) }
	defer func() {
		cancel()
		resume()
		if !joined {
			<-done
		}
	}()
	select {
	case <-cut.entered:
	case err := <-done:
		joined = true
		t.Fatalf("fault bypassed original writer admission: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if count := selected.backend.QueuedWritersForTest(); count != 1 {
		t.Errorf("fault queued on a reconstructed coordinator: count=%d", count)
	}
	cancel()
	resume()
	err := <-done
	joined = true
	if !errors.Is(err, context.Canceled) || selected.backend.QueuedWritersForTest() != 0 {
		t.Fatalf("queued cancellation did not join original admission: %v", err)
	}
}

func resourceReceiptFaultFixture(t *testing.T, backend string) (any, string, string, *stopCommitConnector) {
	t.Helper()
	selected, _, connector := newStopCommitStore(t, backend)
	ctx := testAuthorActivityContext()
	sourceartifactfixture.Require(t, ctx, selected.(sourceartifactfixture.Writer))
	catalog := durableDataFixtureCatalog(t)
	if err := RegisterDurableDataCatalogForTest(ctx, selected, catalog); err != nil {
		t.Fatal(err)
	}
	id := uuid.NewString()
	result, err := selected.(durableDataFixtureStore).ExecuteDataSourceOperation(ctx, runtimedata.SourceCommand{
		Operation: "import", SourceInvocationID: id, Actor: "fixture", BundleHash: catalog.BundleHash,
		Declaration: catalog.Declarations[0].Ref, ExpectedHead: runtimedata.AbsentHead(),
		InputFormat: "jsonl", Input: []byte("{\"label\":\"exact\"}\n"),
	})
	if err != nil || result.Outcome != "accepted" {
		t.Fatalf("native source receipt: %#v %v", result, err)
	}
	return selected, id, string(result.Candidate.VersionID), connector
}

func captureResourceFaultControl(t *testing.T, ctx context.Context, selected any, source, version, kind string) *ResourceReceiptFault {
	t.Helper()
	var fault *ResourceReceiptFault
	var err error
	switch kind {
	case "receipt":
		fault, err = CaptureResourceReceiptFaultForTest(ctx, selected, source, "", "source/typed operation")
	case "payload":
		fault, err = CaptureResourcePayloadCorruptionForTest(ctx, selected, version, "truncated JSON")
	case "provenance":
		fault, err = CaptureImportProvenanceRemovalForTest(ctx, selected, source)
	case "clock":
		fault, err = CaptureSourceClockRewriteForTest(ctx, selected, source, time.Unix(42, 0).UTC())
	case "added-provenance":
		fault, err = CaptureProvenanceReadArmForTest(ctx, selected, version, "normal_run", uuid.NewString(), time.Unix(42, 0).UTC())
	case "head-history":
		catalog := durableDataFixtureCatalog(t)
		id := uuid.NewString()
		result, checkErr := selected.(durableDataFixtureStore).ExecuteDataSourceOperation(ctx, runtimedata.SourceCommand{
			Operation: "check", SourceInvocationID: id, Actor: "fixture", BundleHash: catalog.BundleHash,
			Declaration: catalog.Declarations[0].Ref, ExpectedHead: runtimedata.VersionHead(runtimedata.VersionID(version)),
			InputFormat: "jsonl", Input: []byte("{\"label\":\"exact\"}\n"),
		})
		if checkErr != nil || result.Outcome != "accepted" {
			t.Fatalf("native non-mutating head receipt: %#v %v", result, checkErr)
		}
		fault, err = CaptureNonMutatingHeadHistoryForTest(ctx, selected, id)
	default:
		t.Fatalf("unknown resource fault control %q", kind)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fault.RestoreForTest(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return fault
}

func resourceFaultSnapshot(t *testing.T, ctx context.Context, selected any) map[string][]string {
	t.Helper()
	state, err := ReadResourcePhysicalSnapshotForTest(ctx, selected)
	if err != nil {
		t.Fatal(err)
	}
	return state
}
