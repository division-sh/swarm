package conformance

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/notifyallchildren"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

func deploymentResourceSourceWithAgent(t *testing.T, includeAgent bool) semanticview.Source {
	t.Helper()
	root := notifyallchildren.WriteVariant(t, notifyallchildren.Options{FiniteLifecycle: true})
	if !includeAgent {
		if err := os.Remove(filepath.Join(root, notifyallchildren.ChildFlowID, "agents.yaml")); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(root, notifyallchildren.OwnerFlowID, "events.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	const old = "account.registered:\n  key: account_id\n  account_id: text\n"
	if strings.Count(string(raw), old) != 1 {
		t.Fatalf("account.registered fixture changed: %s", raw)
	}
	updated := strings.Replace(string(raw), old, old+"  document: json?\n", 1)
	if err := os.WriteFile(path, []byte(updated), 0o600); err != nil {
		t.Fatal(err)
	}
	repo := conformanceRepoRoot(t)
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatalf("load deployment resource bundle: %v", err)
	}
	return semanticview.Wrap(bundle)
}

type deploymentResourceFixture struct {
	selected     notifyAllChildrenStore
	db           *sql.DB
	postgresDSN  string
	source       semanticview.Source
	runtime      notifyAllChildrenRuntime
	topology     *notifyAllChildrenProcessTopology
	ctx          context.Context
	liveContexts *runtimepkg.RuntimeContextManager
	config       *config.Config
}

type deploymentFanOutDiagnostic struct {
	startupownership.FanOutExecutor
	t *testing.T
}

func (d deploymentFanOutDiagnostic) ReportFanOutServingError(ctx context.Context, err error) {
	d.t.Logf("deployment fan-out serving error: %v", err)
	d.FanOutExecutor.ReportFanOutServingError(ctx, err)
}

func newDeploymentResourceFixture(t *testing.T, backend string) *deploymentResourceFixture {
	return newDeploymentResourceFixtureWithAgent(t, backend, true)
}

func newDeploymentResourceFixtureWithAgent(t *testing.T, backend string, includeAgent bool) *deploymentResourceFixture {
	t.Helper()
	return newDeploymentResourceFixtureWithSource(t, backend, deploymentResourceSourceWithAgent(t, includeAgent))
}

func newDeploymentResourceFixtureWithSource(t *testing.T, backend string, source semanticview.Source, configs ...*config.Config) *deploymentResourceFixture {
	t.Helper()
	f := &deploymentResourceFixture{source: source}
	if len(configs) > 1 {
		t.Fatal("deployment fixture accepts one ordinary runtime configuration")
	}
	if len(configs) == 1 {
		f.config = configs[0]
	}
	switch backend {
	case "sqlite":
		selected := storetest.StartSQLiteRuntimeStore(t)
		f.selected, f.db = selected, storetest.DatabaseForTest(selected)
	case "postgres":
		dsn, db, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		f.postgresDSN = dsn
		f.selected, f.db = storetest.AdmitPostgresRuntimeStore(t, db), db
	default:
		t.Fatalf("unknown selected backend %q", backend)
	}
	fact := conformanceSourceArtifactFact(t, f.source)
	f.ctx = testAuthorActivityContextForBundle(context.Background(), fact)
	f.ctx = originalConformanceFixtureContext(t, f.ctx, f.selected)
	f.topology = newNotifyAllChildrenProcessTopology(t, f.ctx, f.selected, f.source)
	f.boot(t)
	return f
}

func (f *deploymentResourceFixture) boot(t *testing.T) {
	t.Helper()
	f.liveContexts = nil
	f.runtime = newNotifyAllChildrenRuntime(t, f.selected, f.db, f.source, time.Now, notifyAllChildrenRuntimeOptions{
		ordinaryContext: true,
		ordinaryConfig:  f.config,
		processTopology: f.topology,
		fanOutExecutor: func(coordinator *pipeline.PipelineCoordinator) startupownership.FanOutExecutor {
			return deploymentFanOutDiagnostic{FanOutExecutor: coordinator, t: t}
		},
	})

	if err := runNotifyAllChildrenManager(managedConformanceExecutionContextForBundle(t, f.ctx, fmt.Sprintf("deployment-resource-%d", time.Now().UnixNano()), f.runtime.sourceArtifactFact), f.runtime.bus, f.runtime.manager); err != nil {
		t.Fatalf("normal deployment resource boot: %v", err)
	}

}
