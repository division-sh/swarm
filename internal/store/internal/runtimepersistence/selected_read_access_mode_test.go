package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestSQLiteStandaloneSelectedReadsBypassMutationAdmission(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	db := store.backend.ConstructionHandle()
	ctx := testAuthorActivityContext()
	requireDefaultSourceArtifactForTest(t, ctx, store)
	runID := uuid.NewString()
	source := mustStoreTestSourceArtifactFact(authorActivityTestBundleHash)
	createSelectedReadProofRun(t, ctx, store, runID, source)

	capability, err := store.AcquireProcessCapability(ctx, testStartupAcquireRequest("selected-read-proof"))
	if err != nil {
		t.Fatalf("acquire process capability: %v", err)
	}
	capabilityReleased := false
	t.Cleanup(func() {
		if !capabilityReleased {
			_ = capability.Release(context.Background())
		}
	})

	authority, err := runtimedelivery.NewNormalExecutionAuthority(source, "selected-read-proof", 1)
	if err != nil {
		t.Fatalf("construct delivery authority: %v", err)
	}
	reads := selectedSQLiteReadProofs(store, capability.CurrentSourceSet, authority, runID)
	before := captureSelectedReadSideEffects(t, db, false, runID)

	holderStarted := make(chan struct{})
	holderRelease := make(chan struct{})
	holderDone := make(chan error, 1)
	go func() {
		holderDone <- store.runRuntimeMutation(ctx, "selected read proof holder", func(context.Context, *sql.Tx) error {
			close(holderStarted)
			<-holderRelease
			return nil
		})
	}()
	<-holderStarted

	for _, read := range reads {
		read := read
		t.Run(read.name, func(t *testing.T) {
			readCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := read.run(readCtx); err != nil {
				t.Fatalf("read while mutation admission held: %v", err)
			}
		})
	}
	after := captureSelectedReadSideEffects(t, db, false, runID)
	if after != before {
		t.Fatalf("standalone reads changed mutation side effects: before=%+v after=%+v", before, after)
	}

	close(holderRelease)
	if err := <-holderDone; err != nil {
		t.Fatalf("release mutation holder: %v", err)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, read := range reads {
		if err := read.run(cancelled); !errors.Is(err, context.Canceled) {
			t.Errorf("%s cancellation = %v, want context.Canceled", read.name, err)
		}
	}
	if err := capability.Release(context.Background()); err != nil {
		t.Fatalf("release process capability: %v", err)
	}
	capabilityReleased = true
}

func TestStandaloneDeliveryAndRunLifecycleReadsHaveNoMutationSideEffectsParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			runID := uuid.NewString()
			source := mustStoreTestSourceArtifactFact(authorActivityTestBundleHash)
			var store selectedReaderAsWriterStore
			var db *sql.DB
			postgres := backend == "postgres"
			if postgres {
				_, db, _ = testutil.StartPostgres(t)
				selected := admitTestPostgresStore(t, db)
				store = selected
				requireDefaultSourceArtifactForTest(t, ctx, selected)
				createSelectedReadProofRun(t, ctx, selected, runID, source)
			} else {
				selected := newBootstrappedSQLiteRuntimeStoreForTest(t)
				store = selected
				db = selected.backend.ConstructionHandle()
				requireDefaultSourceArtifactForTest(t, ctx, selected)
				createSelectedReadProofRun(t, ctx, selected, runID, source)
			}
			authority, err := runtimedelivery.NewNormalExecutionAuthority(source, "reader-side-effect-proof", 1)
			if err != nil {
				t.Fatalf("construct delivery authority: %v", err)
			}
			before := captureSelectedReadSideEffects(t, db, postgres, runID)
			for _, read := range selectedReaderAsWriterProofs(store, authority, runID) {
				if err := read.run(ctx); err != nil {
					t.Fatalf("%s: %v", read.name, err)
				}
			}
			after := captureSelectedReadSideEffects(t, db, postgres, runID)
			if after != before {
				t.Fatalf("reader-as-writer side effects changed: before=%+v after=%+v", before, after)
			}

			terminalStore := store.(interface {
				MarkTerminalRun(context.Context, runtimerunlifecycle.TerminalRequest) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error)
			})
			if _, _, err := terminalStore.MarkTerminalRun(ctx, runtimerunlifecycle.TerminalRequest{
				RunID: runID, State: runtimerunlifecycle.StateCancelled,
				EndedAt: time.Date(2026, 8, 27, 2, 1, 0, 0, time.UTC),
			}); err != nil {
				t.Fatalf("terminalize selected-read proof run: %v", err)
			}
			beforeTerminalRead := captureSelectedReadSideEffects(t, db, postgres, runID)
			err = store.RequirePublicationRunActive(ctx, runID)
			var notActive *runtimerunlifecycle.RunNotActiveError
			if !errors.As(err, &notActive) || notActive.State != runtimerunlifecycle.StateCancelled {
				t.Fatalf("terminal publication preflight error = %v, want cancelled RunNotActiveError", err)
			}
			afterTerminalRead := captureSelectedReadSideEffects(t, db, postgres, runID)
			if afterTerminalRead != beforeTerminalRead {
				t.Fatalf("terminal refusal changed mutation side effects: before=%+v after=%+v", beforeTerminalRead, afterTerminalRead)
			}
		})
	}
}

type selectedReadProof struct {
	name string
	run  func(context.Context) error
}

type selectedReaderAsWriterStore interface {
	RequirePresentRun(context.Context, string) error
	RequireActiveRun(context.Context, string) error
	RequirePublicationRunActive(context.Context, string) error
	RequirePresentRunSource(context.Context, string) (runtimecorrelation.SourceArtifactFact, error)
	RequireActiveRunSource(context.Context, string) (runtimecorrelation.SourceArtifactFact, error)
	ScanDeliveryContinuations(context.Context, runtimedelivery.ExecutionAuthority, runtimedelivery.ContinuationCursor, int) (runtimedelivery.ContinuationPage, error)
	ObserveDeliveryContinuation(context.Context, runtimedelivery.ExecutionAuthority, string) (runtimedelivery.ContinuationObservation, error)
}

func selectedReaderAsWriterProofs(store selectedReaderAsWriterStore, authority runtimedelivery.ExecutionAuthority, runID string) []selectedReadProof {
	return []selectedReadProof{
		{name: "require present run", run: func(ctx context.Context) error { return store.RequirePresentRun(ctx, runID) }},
		{name: "require active run", run: func(ctx context.Context) error { return store.RequireActiveRun(ctx, runID) }},
		{name: "require publication run active", run: func(ctx context.Context) error { return store.RequirePublicationRunActive(ctx, runID) }},
		{name: "require present run source", run: func(ctx context.Context) error { _, err := store.RequirePresentRunSource(ctx, runID); return err }},
		{name: "require active run source", run: func(ctx context.Context) error { _, err := store.RequireActiveRunSource(ctx, runID); return err }},
		{name: "scan delivery continuations", run: func(ctx context.Context) error {
			_, err := store.ScanDeliveryContinuations(ctx, authority, runtimedelivery.ContinuationCursor{}, 1)
			return err
		}},
		{name: "observe delivery continuation", run: func(ctx context.Context) error {
			_, err := store.ObserveDeliveryContinuation(ctx, authority, uuid.NewString())
			return err
		}},
	}
}

func selectedSQLiteReadProofs(
	store *SQLiteRuntimeStore,
	loadSourceSet func(context.Context) (runtimeagenttopology.SourceSetPlan, bool, error),
	authority runtimedelivery.ExecutionAuthority,
	runID string,
) []selectedReadProof {
	reads := selectedReaderAsWriterProofs(store, authority, runID)
	return append(reads,
		selectedReadProof{name: "load source set", run: func(ctx context.Context) error { _, _, err := loadSourceSet(ctx); return err }},
		selectedReadProof{name: "load inbound publication", run: func(ctx context.Context) error {
			_, _, err := store.LoadInboundPublicationByIdentity(ctx, "test", uuid.NewString(), "missing")
			return err
		}},
		selectedReadProof{name: "load run bundle", run: func(ctx context.Context) error { _, err := store.LoadRunBundleAvailability(ctx, runID); return err }},
	)
}

func createSelectedReadProofRun(t *testing.T, ctx context.Context, store interface {
	CreateRun(context.Context, runtimerunlifecycle.CreateRequest) (runtimerunlifecycle.MutationDisposition, error)
}, runID string, source runtimecorrelation.SourceArtifactFact) {
	t.Helper()
	if _, err := store.CreateRun(ctx, runtimerunlifecycle.CreateRequest{
		RunID: runID, Origin: runtimerunlifecycle.ScenarioSetupRunOrigin(), Source: source,
		StartedAt: time.Date(2026, 8, 27, 2, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatalf("create selected-read proof run: %v", err)
	}
}

type selectedReadSideEffects struct {
	authorSequence int64
	authorRows     int64
	revisionHead   int64
	revisionRows   int64
	candidateRev   int64
	candidateDue   string
}

func captureSelectedReadSideEffects(t *testing.T, db *sql.DB, postgres bool, runID string) selectedReadSideEffects {
	t.Helper()
	var state selectedReadSideEffects
	if err := db.QueryRow(`SELECT last_sequence FROM author_activity_order WHERE singleton_id = 1`).Scan(&state.authorSequence); err != nil {
		t.Fatalf("read author sequence: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM author_activity_occurrences`).Scan(&state.authorRows); err != nil {
		t.Fatalf("count author activity: %v", err)
	}
	headQuery := `SELECT COALESCE((SELECT last_revision FROM run_fork_revision_heads WHERE run_id = ?), 0)`
	rowsQuery := `SELECT COUNT(*) FROM run_fork_revisions WHERE run_id = ?`
	candidateQuery := `SELECT completion_revision, COALESCE(CAST(completion_due_at AS TEXT), '') FROM runs WHERE run_id = ?`
	if postgres {
		headQuery = `SELECT COALESCE((SELECT last_revision FROM run_fork_revision_heads WHERE run_id = $1::uuid), 0)`
		rowsQuery = `SELECT COUNT(*) FROM run_fork_revisions WHERE run_id = $1::uuid`
		candidateQuery = `SELECT completion_revision, COALESCE(completion_due_at::text, '') FROM runs WHERE run_id = $1::uuid`
	}
	if err := db.QueryRow(headQuery, runID).Scan(&state.revisionHead); err != nil {
		t.Fatalf("read revision head: %v", err)
	}
	if err := db.QueryRow(rowsQuery, runID).Scan(&state.revisionRows); err != nil {
		t.Fatalf("count revisions: %v", err)
	}
	if err := db.QueryRow(candidateQuery, runID).Scan(&state.candidateRev, &state.candidateDue); err != nil {
		t.Fatalf("read completion candidate state: %v", err)
	}
	return state
}

func TestStandaloneSelectedReadAccessModeGuard(t *testing.T) {
	root := repoRootForRuntimeWriterGuard(t)
	tests := []selectedReadGuard{
		{path: "internal/store/internal/backend/delivery/lifecycle.go", receiver: "DeliverySQLiteOwner", method: "ScanDeliveryContinuations", required: "RunReadTransaction"},
		{path: "internal/store/internal/backend/delivery/lifecycle.go", receiver: "DeliverySQLiteOwner", method: "ObserveDeliveryContinuation", required: "RunReadTransaction"},
		{path: "internal/store/internal/backend/delivery/lifecycle.go", receiver: "DeliveryPostgresOwner", method: "ScanDeliveryContinuations", required: "RunReadTransaction"},
		{path: "internal/store/internal/backend/delivery/lifecycle.go", receiver: "DeliveryPostgresOwner", method: "ObserveDeliveryContinuation", required: "RunReadTransaction"},
		{path: "internal/store/internal/startupownership/owner.go", receiver: "sqliteSession", method: "LoadSourceSet", required: "RunReadTransaction"},
		{path: "internal/store/internal/backend/eventpersistence/sqlite_inbound_publication.go", receiver: "EventSQLiteOwner", method: "loadInboundPublicationByIdentity", required: "RunReadTransaction"},
		{path: "internal/store/internal/runbundle/owner.go", receiver: "SQLite", method: "Load", required: "RunReadTransaction"},
	}
	for _, backend := range []string{"SQLite", "Postgres"} {
		wrapper := "run" + backend + "LifecycleRead"
		for _, method := range []string{"RequirePresentRun", "RequireActiveRun", "RequirePresentRunSource", "RequireActiveRunSource"} {
			tests = append(tests, selectedReadGuard{path: "internal/store/internal/backend/runlifecycle/run_lifecycle_mutation_adapter.go", receiver: "RunLifecycle" + backend + "Owner", method: method, required: wrapper})
		}
		tests = append(tests, selectedReadGuard{path: "internal/store/internal/backend/runlifecycle/run_lifecycle_mutation_adapter.go", receiver: "RunLifecycle" + backend + "Owner", method: "RequirePublicationRunActive", required: "runRead"})
	}
	for _, test := range tests {
		calls := selectedReadMethodCalls(t, filepath.Join(root, test.path), test.receiver, test.method)
		if !calls[test.required] {
			t.Errorf("%s.%s does not consume %s: calls=%v", test.receiver, test.method, test.required, calls)
		}
		for _, forbidden := range []string{"RunTransaction", "runPrivateAuthorActivityMutation", "runSQLiteLifecycleOperation", "runPostgresLifecycleOperation", "WithCandidateHandoffResult"} {
			if calls[forbidden] {
				t.Errorf("%s.%s still consumes mutation-only owner %s", test.receiver, test.method, forbidden)
			}
		}
	}

	path := filepath.Join(root, "internal/store/internal/backend/runlifecycle/run_lifecycle_mutation_adapter.go")
	for _, function := range []string{"runSQLiteLifecycleRead", "runPostgresLifecycleRead"} {
		calls := selectedReadMethodCalls(t, path, "", function)
		if !calls["runRead"] {
			t.Errorf("%s does not consume the private read lifecycle: calls=%v", function, calls)
		}
		for _, forbidden := range []string{"runPrivateAuthorActivityMutation", "runSQLiteLifecycleOperation", "runPostgresLifecycleOperation", "WithCandidateHandoffResult"} {
			if calls[forbidden] {
				t.Errorf("%s still consumes mutation-only owner %s", function, forbidden)
			}
		}
	}
	for _, backend := range []string{"SQLite", "Postgres"} {
		for _, method := range []string{"RequireActiveTx", "RequirePresentTx", "RequireActiveSourceTx", "RequirePresentSourceTx"} {
			calls := selectedReadMethodCalls(t, path, "RunLifecycle"+backend+"Owner", method)
			if calls["runRead"] || calls["runSQLiteLifecycleRead"] || calls["runPostgresLifecycleRead"] {
				t.Errorf("transaction-scoped %s.%s escaped its owning mutation: calls=%v", backend, method, calls)
			}
		}
	}
}

type selectedReadGuard struct {
	path, receiver, method, required string
}

func selectedReadMethodCalls(t *testing.T, path, receiver, method string) map[string]bool {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, contents, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	calls := map[string]bool{}
	found := false
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok || fn.Name.Name != method || selectedReadReceiverName(fn) != receiver {
			continue
		}
		found = true
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			switch target := call.Fun.(type) {
			case *ast.Ident:
				calls[target.Name] = true
			case *ast.SelectorExpr:
				calls[target.Sel.Name] = true
			}
			return true
		})
	}
	if !found {
		t.Fatalf("method %s.%s not found in %s", receiver, method, path)
	}
	return calls
}

func selectedReadReceiverName(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) != 1 {
		return ""
	}
	expression := fn.Recv.List[0].Type
	if pointer, ok := expression.(*ast.StarExpr); ok {
		expression = pointer.X
	}
	identifier, _ := expression.(*ast.Ident)
	if identifier == nil {
		return ""
	}
	return strings.TrimSpace(identifier.Name)
}
