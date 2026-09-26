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

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/notifyallchildren"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

// Keep the real parent-to-template connect route. The imported document is an
// optional event field so ordinary authored account.registered producers remain
// valid, while deployment feeds can carry the full nested source document.
func deploymentResourceSource(t *testing.T) semanticview.Source {
	return deploymentResourceSourceWithAgent(t, true)
}

func deploymentResourceSourceWithAgent(t *testing.T, includeAgent bool) semanticview.Source {
	t.Helper()
	root := notifyallchildren.WriteVariant(t, notifyallchildren.Options{})
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
	selected notifyAllChildrenStore
	db       *sql.DB
	source   semanticview.Source
	runtime  notifyAllChildrenRuntime
	topology *notifyAllChildrenProcessTopology
	ctx      context.Context
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

func newDeploymentResourceFixtureWithSource(t *testing.T, backend string, source semanticview.Source) *deploymentResourceFixture {
	t.Helper()
	f := &deploymentResourceFixture{source: source}
	switch backend {
	case "sqlite":
		selected := storetest.StartSQLiteRuntimeStore(t)
		f.selected, f.db = selected, storetest.DatabaseForTest(selected)
	case "postgres":
		_, db, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		f.selected, f.db = storetest.AdmitPostgresRuntimeStore(t, db), db
	default:
		t.Fatalf("unknown selected backend %q", backend)
	}
	fact := conformanceSourceArtifactFact(t, f.source)
	f.ctx = testAuthorActivityContextForBundle(context.Background(), fact)
	f.topology = newNotifyAllChildrenProcessTopology(t, f.ctx, f.selected, f.source)
	f.boot(t)
	return f
}

func (f *deploymentResourceFixture) boot(t *testing.T) {
	t.Helper()
	f.runtime = newNotifyAllChildrenRuntime(t, f.selected, f.db, f.source, time.Now, notifyAllChildrenRuntimeOptions{
		processTopology: f.topology,
		fanOutExecutor: func(coordinator *pipeline.PipelineCoordinator) startupownership.FanOutExecutor {
			return deploymentFanOutDiagnostic{FanOutExecutor: coordinator, t: t}
		},
	})
	if err := f.runtime.manager.Run(managedConformanceExecutionContextForBundle(t, f.ctx, fmt.Sprintf("deployment-resource-%d", time.Now().UnixNano()), f.runtime.sourceArtifactFact)); err != nil {
		t.Fatalf("normal deployment resource boot: %v", err)
	}
}
