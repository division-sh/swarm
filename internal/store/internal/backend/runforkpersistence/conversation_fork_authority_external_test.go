package runforkpersistence_test

import (
	"context"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/construction"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkpersistence"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestConversationForkGracefulMutation(t *testing.T) {
	runforkpersistence.VerifyConversationForkGracefulMutationForTest(t, func(t *testing.T, dsn string) context.Context {
		t.Helper()
		ctx := context.Background()
		selected, _, err := construction.OpenPostgresRuntime(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := selected.Close(); err != nil {
				t.Error(err)
			}
		})
		storetest.BootstrapPostgresRuntimeStore(t, selected)
		runtimeID := uuid.NewString()
		process, err := selected.AcquireProcessCapability(ctx, startupownership.AcquireRequest{
			OwnerID: "conversation-native-fence-fixture", BootID: uuid.NewString(), RuntimeInstanceID: runtimeID,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := process.Release(context.Background()); err != nil {
				t.Error(err)
			}
		})
		coordinate := agenttopology.SourceCoordinate{BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64)}
		plan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{coordinate}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := process.InstallCompleteSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}); err != nil {
			t.Fatal(err)
		}
		grant, err := process.IssueGenerationGrant(ctx, startupownership.GrantRequest{
			BundleHash: coordinate.BundleHash, RuntimeInstanceID: runtimeID, RuntimeGeneration: 1, SourceSetRevision: plan.Revision,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := grant.MarkProbesSettled(ctx, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := grant.AdmitExecution(ctx); err != nil {
			t.Fatal(err)
		}
		ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(runtimeID, coordinate.BundleHash))
		ctx, err = startupownership.WithRuntimeMutationAuthority(ctx, grant)
		if err != nil {
			t.Fatal(err)
		}
		return ctx
	})
}
