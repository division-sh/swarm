package pipelinepersistence_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/division-sh/swarm/internal/store/internal/schemastore"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
)

// Keep the production store assembly outside its private backend's import graph.
func prepareStatementProcess(t testing.TB, db *sql.DB, postgres bool, request schemastore.SchemaBootstrapRequest) context.Context {
	t.Helper()
	var selected interface {
		agentfixture.Store
		BootstrapSchema(context.Context, schemastore.SchemaBootstrapRequest) error
	}
	if postgres {
		selected = storetest.NewPostgresStoreForTest(db)
	} else {
		selected = storetest.NewSQLiteRuntimeStoreForTest(db)
	}
	ctx := context.Background()
	if err := selected.BootstrapSchema(ctx, request); err != nil {
		t.Fatal(err)
	}
	process, err := agentfixture.ProcessCapability(t, ctx, selected)
	if err != nil {
		t.Fatal(err)
	}
	ctx, err = startupownership.WithProcessMutationAuthority(ctx, process)
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}

func TestFanOutEntitySourceRevisionWriterPreservesNumberKindsOnBothStores(t *testing.T) {
	pipelinepersistence.VerifyFanOutEntitySourceRevisionWriterPreservesNumberKindsForTest(t, prepareStatementProcess)
}

func TestFanOutReadMixedOriginPaginationBothStores(t *testing.T) {
	pipelinepersistence.VerifyFanOutReadMixedOriginPaginationForTest(t, prepareStatementProcess)
}

func TestFanOutUnacknowledgedCommitCannotBorrowSuccessorOnBothStores(t *testing.T) {
	pipelinepersistence.VerifyFanOutUnacknowledgedCommitCannotBorrowSuccessorForTest(t, prepareStatementProcess)
}

func TestQ10DataClockWriteTriggersBothStores(t *testing.T) {
	pipelinepersistence.VerifyQ10DataClockWriteTriggersForTest(t, prepareStatementProcess)
}

func TestSQLiteEntityStateDiffRequiresExistingCanonicalRunBeforeMutation(t *testing.T) {
	pipelinepersistence.VerifyMissingMutationRunForTest(t, false, prepareStatementProcess)
}

func TestSQLiteInitialValueMutationRequiresExistingCanonicalRunBeforeMutation(t *testing.T) {
	pipelinepersistence.VerifyMissingMutationRunForTest(t, true, prepareStatementProcess)
}
