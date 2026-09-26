package conformance

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestServedParityHarnessRunStartDeploymentFeedLifecycle(t *testing.T) {
	scenario := servedparity.MustScenario(servedparity.ScenarioRunStartDeploymentFeedLifecycle)
	servedparity.Run(t, scenario, func(t *testing.T, backend servedparity.Backend) {
		storeBackend := "sqlite"
		if backend == servedparity.BackendExplicitPostgres {
			storeBackend = "postgres"
		}
		fixture := selectedDeploymentResourceFixture(t, storeBackend, "root")
		input := []byte("{\"account_id\":\"first\",\"document\":{\"slug\":\"first\"}}\n")
		compiled := selectedDeploymentVersion(t, fixture, input)
		file := filepath.Join(t.TempDir(), "accounts.jsonl")
		if err := os.WriteFile(file, input, 0o600); err != nil {
			t.Fatal(err)
		}
		server := fixture.operatorServer(t)
		firstRun := startDeploymentResourceRun(t, fixture, server, "--data", selectedDeploymentEvent+"="+file)
		assertSelectedDeploymentRows(t, fixture, server, firstRun, "root", string(compiled.VersionID), input)
		secondRun := startDeploymentResourceRun(t, fixture, server, "--pin", selectedDeploymentEvent+"@head")
		if secondRun == firstRun {
			t.Fatal("a new run.start trigger reused the first feed intent")
		}
		assertSelectedDeploymentRows(t, fixture, server, secondRun, "root", string(compiled.VersionID), input)

		reader, ok := fixture.selected.(interface {
			LoadRunTestQuiescence(context.Context, string, time.Time) (operatorread.RunTestQuiescence, error)
		})
		if !ok {
			t.Fatalf("selected store %T lacks canonical quiescence readback", fixture.selected)
		}
		for _, runID := range []string{firstRun, secondRun} {
			deadline := time.Now().Add(30 * time.Second)
			for {
				quiescence, err := reader.LoadRunTestQuiescence(fixture.ctx, runID, time.Now().UTC())
				if err != nil {
					t.Fatalf("read run %s quiescence: %v", runID, err)
				}
				counts := servedparity.SettlementCounts{
					NonTerminalDeliveries: quiescence.ActiveDeliveries,
					PendingPipelineEvents: quiescence.UnsettledPipelineEvents,
					UnfiredDueTimers:      quiescence.DueTimers,
				}
				failures := servedparity.SettlementPostconditionFailures(scenario, counts)
				if len(failures) == 0 {
					break
				}
				if time.Now().After(deadline) {
					rows, queryErr := fixture.db.QueryContext(fixture.ctx, `SELECT e.event_id,e.event_name,COALESCE(r.outcome,'missing')
						FROM events e LEFT JOIN event_receipts r ON r.event_id=e.event_id
						AND r.subscriber_type='platform' AND r.subscriber_id='pipeline'
						WHERE e.run_id=$1 ORDER BY e.created_at`, runID)
					if queryErr == nil {
						defer rows.Close()
						for rows.Next() {
							var eventID, eventName, outcome string
							if scanErr := rows.Scan(&eventID, &eventName, &outcome); scanErr == nil {
								t.Logf("event %s %s pipeline_receipt=%s", eventID, eventName, outcome)
							}
						}
					}
					t.Fatalf("run %s did not settle: %v", runID, failures)
				}
				time.Sleep(50 * time.Millisecond)
			}
		}
	})
}
