package pipelinepersistence_test

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
)

func init() {
	pipelinepersistence.OriginalNestedAdmissionContextForTest = func(t *testing.T, postgres bool) context.Context {
		t.Helper()
		var selected agentfixture.Store
		if postgres {
			selected = storetest.StartPostgresRuntimeStore(t)
		} else {
			selected = storetest.StartSQLiteRuntimeStore(t)
		}
		ctx := t.Context()
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
}
