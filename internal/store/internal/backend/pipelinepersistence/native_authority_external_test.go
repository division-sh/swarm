package pipelinepersistence_test

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/construction"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/division-sh/swarm/internal/store/internal/schemastore"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
)

// Keep the production store assembly outside its private backend's import graph.
func prepareStatementProcess(t testing.TB, location string, postgres bool, request schemastore.SchemaBootstrapRequest) context.Context {
	t.Helper()
	var selected interface {
		agentfixture.Store
		BootstrapSchema(context.Context, schemastore.SchemaBootstrapRequest) error
		Close() error
	}
	var err error
	if postgres {
		selected, err = construction.NewPostgres(location)
	} else {
		selected, err = construction.NewSQLiteRuntime(location)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := selected.Close(); err != nil {
			t.Error(err)
		}
	})
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
