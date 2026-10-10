package cataloge2e

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

// Only the producer's causal context is controlled. Preparation, selected grant
// issuance, lifecycle commit, projection and activation all use their real owners.
type causalSelectedProcess struct {
	startupownership.ProcessCapability
	h           *runtimeHarness
	sourceEvent string
	kind        string
	mu          sync.Mutex
	items       []diaglog.LifecycleDiagnostic
	refusals    int
}

func (p *causalSelectedProcess) IssueSelectedForkGenerationGrant(ctx context.Context, req startupownership.SelectedForkGrantRequest) (startupownership.GenerationGrant, error) {
	grant, err := p.ProcessCapability.IssueSelectedForkGenerationGrant(ctx, req)
	if err != nil {
		return nil, err
	}
	return causalSelectedGrant{GenerationGrant: grant, probe: p}, nil
}

type causalSelectedGrant struct {
	startupownership.GenerationGrant
	probe *causalSelectedProcess
}

func (g causalSelectedGrant) CommitAgentLifecycleTransition(ctx context.Context, req manager.AgentLifecycleTransition) (manager.AgentLifecycleTransitionResult, error) {
	if req.OperationKind != "self_release" || req.Trigger != "self_release" || req.TargetPhase != manager.AgentLifecycleRegistered {
		return g.GenerationGrant.CommitAgentLifecycleTransition(ctx, req)
	}
	evidence, err := g.Evidence()
	if err != nil {
		return manager.AgentLifecycleTransitionResult{}, err
	}
	if evidence.SelectedFork == nil || evidence.SelectedFork.ForkRunID != req.Identity.RunID {
		return manager.AgentLifecycleTransitionResult{}, fmt.Errorf("causal proof requires the actual selected actor grant")
	}
	parent := activityidentity.ForkLineageEventID(req.Identity.RunID, g.probe.sourceEvent)
	reader, err := g.probe.h.catalogOperatorEventLister()
	if err != nil {
		return manager.AgentLifecycleTransitionResult{}, err
	}
	publishedParent, found, err := storetest.ReadCanonicalEventRecord(ctx, reader, parent)
	if err != nil || !found || publishedParent.RunID() != req.Identity.RunID {
		return manager.AgentLifecycleTransitionResult{}, fmt.Errorf("selected parent was not actually published: found=%t run=%s err=%v", found, publishedParent.RunID(), err)
	}
	lineage := correlation.RuntimeLineage{
		Owner: runfork.RunForkSelectedContractForkLocalRuntimeTypedLineageOwner,
		RunID: req.Identity.RunID, SelectedForkContext: true,
		Classification: correlation.RuntimeLineageClassificationForkLocal,
	}
	if g.probe.kind == "subject" {
		lineage.SubjectEventID = parent
	} else {
		lineage.ParentEventID = parent
	}
	if g.probe.kind == "missing" {
		lineage.ParentEventID = uuid.NewString()
	}
	result, err := g.GenerationGrant.CommitAgentLifecycleTransition(correlation.WithRuntimeLineage(ctx, lineage), req)
	if err != nil {
		return result, err
	}
	var diagnostics manager.AgentLifecycleDiagnosticPersistence = g.probe.h.pg
	if g.probe.h.sqlite != nil {
		diagnostics = g.probe.h.sqlite
	}
	pending, err := diagnostics.ListPendingAgentLifecycleDiagnostics(ctx, 1000)
	if err != nil {
		return result, err
	}
	for _, item := range pending {
		if item.OperationID == req.OperationID {
			if g.probe.kind == "missing" {
				var persistence runtimepkg.RuntimeLogPersistence = g.probe.h.pg
				if g.probe.h.sqlite != nil {
					persistence = g.probe.h.sqlite
				}
				logger := runtimepkg.NewRuntimeLogger(persistence, executionposture.Live, nil)
				consumer := correlation.WithRuntimeLineage(ctx, correlation.RuntimeLineage{RunID: uuid.NewString(), ParentEventID: uuid.NewString()})
				for attempt := 0; attempt < 2; attempt++ {
					if err := logger.ProjectLifecycleDiagnostic(consumer, item); err == nil || !strings.Contains(err.Error(), "does not exist") {
						return result, fmt.Errorf("missing causal parent refusal: %v", err)
					}
					observed, err := storetest.ReadSelectedCausalDiagnosticConservation(ctx, diagnostics, item.OutboxID)
					count := observed.Events
					if err != nil || count != 0 {
						return result, fmt.Errorf("missing parent emitted diagnostic: count=%d err=%v", count, err)
					}
					pendingCount := observed.Pending
					if pendingCount != 1 {
						return result, fmt.Errorf("missing parent acknowledged diagnostic: pending=%d err=%v", pendingCount, err)
					}
					g.probe.mu.Lock()
					g.probe.refusals++
					g.probe.mu.Unlock()
				}
			}
			g.probe.mu.Lock()
			g.probe.items = append(g.probe.items, item)
			g.probe.mu.Unlock()
			return result, nil
		}
	}
	return result, fmt.Errorf("selected transition did not retain its causal diagnostic")
}

func TestSelectedContractActivationAllowsCausalForkLocalRuntimeLogDiagnostic(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		for _, kind := range []string{"explicit", "subject", "missing"} {
			t.Run(string(backend)+"/"+kind, func(t *testing.T) {
				root := localReadinessFixture(t, 1, "agent")
				h := newRuntimeHarnessForBackend(t, root, backend, true)
				h.db = nil // Every physical witness must consume its original native owner.
				ctx := worklifetime.WithOccurrence(catalogRunContext(h, catalogRuntimeRunID), h.rt.WorkOccurrence())
				frontierID := activateLocalReadinessFrontier(t, ctx, h, "worker.ready")
				var sourceStore interface {
					storetest.DurableDataCatalogStore
					runforkexecution.SourceArtifactSelectedContractSourceStore
				} = h.pg
				var logStore runtimepkg.RuntimeLogPersistence = h.pg
				if h.sqlite != nil {
					sourceStore, logStore = h.sqlite, h.sqlite
				}
				loader, selection, loaded := selectedContractForkFixtureSelection(t, ctx, repoRootFromCatalogE2E(t), root, sourceStore)
				installCatalogSelectedSourceTopology(t, ctx, h, loaded)
				cfg := testRuntimeConfig()
				cfg.LLM.Backend = "anthropic"
				options := selectedContractAgentRuntimeOptionsForCatalogHarness(h, cfg)
				probe := &causalSelectedProcess{ProcessCapability: options.ProcessCapability, h: h, sourceEvent: frontierID, kind: kind}
				options.ProcessCapability = probe
				result, err := runforkexecution.ExecuteSelectedContractRunFork(ctx, runforkexecution.SelectedContractExecutionRequest{
					SourceRunID: catalogRuntimeRunID, At: frontierID, AllowSourceFreeze: true,
					Owner: selectedContractExecutionOwnerForCatalogHarness(t, h), SourceLoader: loader, ContractSelection: selection, AgentRuntime: options,
				})
				if err != nil || result.ExecutedEventCount != 1 || !result.Activation.Activated {
					t.Fatalf("real selected execution/activation: %+v %v", result, err)
				}
				probe.mu.Lock()
				items := append([]diaglog.LifecycleDiagnostic(nil), probe.items...)
				refusals := probe.refusals
				probe.mu.Unlock()
				if len(items) == 0 {
					t.Fatal("actual selected actor produced no owned self-release causal diagnostic")
				}
				if kind == "missing" {
					// Invalid diagnostics remain unprojected/unacknowledged. They are
					// not an ordinary-final retirement prerequisite for business ACK.
					if refusals != 2*len(items) {
						t.Fatalf("missing-parent self-release lost exact projection refusals: count=%d refusals=%d activated=%t err=%v", result.ExecutedEventCount, refusals, result.Activation.Activated, err)
					}
					return
				}
				logger := runtimepkg.NewRuntimeLogger(logStore, executionposture.Live, nil)
				consumer := correlation.WithRuntimeLineage(ctx, correlation.RuntimeLineage{RunID: uuid.NewString(), ParentEventID: uuid.NewString()})
				for _, item := range items {
					for attempt := 0; attempt < 2; attempt++ {
						if err := logger.ProjectLifecycleDiagnostic(consumer, item); err != nil {
							t.Fatal(err)
						}
						assertSelectedCausalDiagnosticReadback(t, h, item, kind, activityidentity.ForkLineageEventID(result.Materialization.ForkRunID, frontierID))
					}
				}
			})
		}
	}
}

func assertSelectedCausalDiagnosticReadback(t *testing.T, h *runtimeHarness, item diaglog.LifecycleDiagnostic, kind, parent string) {
	t.Helper()
	var selected any = h.sqlite
	if h.pg != nil {
		selected = h.pg
	}
	observed, err := storetest.ReadSelectedCausalDiagnosticStorage(context.Background(), selected, item.OutboxID)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, row := range observed.Events {
		run, cause := row.RunID, row.SourceEventID
		if run != item.Identity.RunID || cause != parent {
			t.Fatalf("selected event lineage: %s/%s", run, cause)
		}
		count++
	}
	raw := observed.Projection
	var receipt struct {
		RunID              string `json:"run_id"`
		ParentEventID      string `json:"parent_event_id"`
		LineageDisposition string `json:"lineage_disposition"`
	}
	if err := json.Unmarshal(raw, &receipt); err != nil {
		t.Fatal(err)
	}
	if count != 1 || receipt.RunID != item.Identity.RunID || receipt.ParentEventID != parent || receipt.LineageDisposition != "causal_"+kind {
		t.Fatalf("selected diagnostic receipt/count: %s/%d", raw, count)
	}
}
