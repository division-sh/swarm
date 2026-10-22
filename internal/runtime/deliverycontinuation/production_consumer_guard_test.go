package deliverycontinuation

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
)

func TestExecutableDeliveryContinuationHasClosedProductionConsumers(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	forbiddenCalls := map[string]struct{}{
		"ClaimAgentBacklog":     {},
		"ClaimNodeBacklog":      {},
		"RecoverNodeDeliveries": {},
		"ReplayAgentBacklog":    {},
	}
	forbiddenText := []string{
		"agent.replay_backlog",
		"swarm agent replay-backlog",
		"retry_eligible",
		"delivery_retry_eligible",
		"scanFailureRetryDelay",
		"dispatchRetryDelay",
		"isTopologyBlocked",
		"coordinatorOwns",
	}
	scanConsumers := map[string]int{}
	unqueuedCompletionConsumers := map[string]int{}
	pipelineSettlementConsumers := map[string]int{}
	err := checkoutsource.WalkDir(repoRoot, repoRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			switch entry.Name() {
			case "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		rel, err := filepath.Rel(repoRoot, path)
		if err != nil {
			return err
		}
		if strings.HasSuffix(path, "_test.go") {
			return nil
		}
		switch filepath.Ext(path) {
		case ".go":
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			for _, forbidden := range forbiddenText {
				if containsRetiredDeliveryDialect(filepath.ToSlash(rel), string(raw), forbidden) {
					t.Errorf("%s retains forbidden executable-delivery dialect %q", rel, forbidden)
				}
			}
			if filepath.ToSlash(rel) == "internal/runtime/deliverycontinuation/coordinator.go" {
				for _, forbidden := range []string{"time.Until(", ".NextEligibleAt", ".ClaimExpiresAt"} {
					if strings.Contains(string(raw), forbidden) {
						t.Errorf("%s reconstructs selected-store eligibility through %q", rel, forbidden)
					}
				}
			}
			if filepath.ToSlash(rel) == "internal/runtime/core/worklifetime/delivery_carrier_guard.go" {
				for _, forbidden := range []string{"time.Sleep(", "time.NewTimer(", "time.NewTicker(", "for {"} {
					if strings.Contains(string(raw), forbidden) {
						t.Errorf("%s retains forbidden carrier-resolution polling through %q", rel, forbidden)
					}
				}
			}
			switch filepath.ToSlash(rel) {
			case "internal/runtime/manager/runtime.go":
				if count := strings.Count(string(raw), "NewEventDeliveryCarrierGuard("); count != 1 {
					t.Errorf("%s guarded EventDelivery completion scopes = %d, want 1", rel, count)
				}
				for _, forbidden := range []string{"delivery.Complete()", "delivery.ConsumeContinuation()"} {
					if strings.Contains(string(raw), forbidden) {
						t.Errorf("%s bypasses the scoped delivery carrier guard through %q", rel, forbidden)
					}
				}
			case "internal/runtime/pipeline/coordinator.go":
				if count := strings.Count(string(raw), "NewDeliveryContinuationGuard("); count != 1 {
					t.Errorf("%s guarded direct continuation scopes = %d, want 1", rel, count)
				}
				for _, forbidden := range []string{"continuation.Return(", "continuation.Consume("} {
					if strings.Contains(string(raw), forbidden) {
						t.Errorf("%s bypasses the scoped delivery continuation guard through %q", rel, forbidden)
					}
				}
			}
			if strings.HasPrefix(filepath.ToSlash(rel), "internal/runtime/bus/") {
				if count := strings.Count(string(raw), "pipelineObligations.Settle("); count > 0 {
					pipelineSettlementConsumers[filepath.ToSlash(rel)] = count
				}
			}
			parsed, err := parser.ParseFile(token.NewFileSet(), path, raw, 0)
			if err != nil {
				return err
			}
			ast.Inspect(parsed, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				name := calledFunctionName(call.Fun)
				if _, forbidden := forbiddenCalls[name]; forbidden {
					t.Errorf("%s retains forbidden executable-delivery consumer %s", rel, name)
				}
				if name == "ScanDeliveryContinuations" {
					scanConsumers[filepath.ToSlash(rel)]++
				}
				if name == "CompleteUnqueued" {
					unqueuedCompletionConsumers[filepath.ToSlash(rel)]++
				}
				return true
			})
		case ".yaml", ".json":
			if rel != "platform-spec.yaml" && rel != "openrpc.json" {
				return nil
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			for _, forbidden := range forbiddenText {
				if containsRetiredDeliveryDialect(filepath.ToSlash(rel), string(raw), forbidden) {
					t.Errorf("%s retains forbidden executable-delivery dialect %q", rel, forbidden)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(unqueuedCompletionConsumers) != 1 || unqueuedCompletionConsumers["internal/runtime/bus/eventbus_routing.go"] != 10 {
		t.Fatalf("unqueued completion must remain in the two pre-send owners: %#v", unqueuedCompletionConsumers)
	}
	want := map[string]int{
		"internal/runtime/deliverycontinuation/coordinator.go":                      1,
		"internal/store/internal/runtimepersistence/facade_forwarders_generated.go": 2,
		"internal/runtime/testfixtures/startuphandoff/proof.go":                     1, // Test-only handoff witness delegates the exact native scan; no eligibility or execution owner.
	}
	if len(scanConsumers) != len(want) {
		t.Fatalf("delivery continuation scan consumers = %#v, want %#v", scanConsumers, want)
	}
	for path, count := range want {
		if scanConsumers[path] != count {
			t.Fatalf("ScanDeliveryContinuations calls in %s = %d, want %d", path, scanConsumers[path], count)
		}
	}
	wantPipelineSettlements := map[string]int{
		"internal/runtime/bus/pipeline_publication_claim.go": 1,
	}
	if len(pipelineSettlementConsumers) != len(wantPipelineSettlements) {
		t.Fatalf("pipeline settlement consumers = %#v, want %#v", pipelineSettlementConsumers, wantPipelineSettlements)
	}
	for path, count := range wantPipelineSettlements {
		if pipelineSettlementConsumers[path] != count {
			t.Fatalf("pipeline settlement calls in %s = %d, want %d", path, pipelineSettlementConsumers[path], count)
		}
	}
}

func containsRetiredDeliveryDialect(path, source, forbidden string) bool {
	if forbidden == "retry_eligible" {
		switch path {
		case "internal/store/internal/backend/delivery/adapter.go", "internal/store/internal/backend/runforkrevision/projection.go", "platform-spec.yaml":
			// #2592/6192759467 admits this committed-outcome column, not
			// the retired dispatch event or another eligibility interpreter.
			source = regexp.MustCompile(`\bretry_eligible_at\b`).ReplaceAllString(source, "")
		}
	}
	return strings.Contains(source, forbidden)
}

func TestRetiredDeliveryDialectStillRejectsRetryDispatchRestoration(t *testing.T) {
	for _, path := range []string{"internal/store/internal/backend/delivery/adapter.go", "internal/store/internal/backend/runforkrevision/projection.go", "platform-spec.yaml"} {
		for _, cell := range []struct {
			source    string
			forbidden string
			want      bool
		}{
			{"retry_eligible_at", "retry_eligible", false},
			{`"retry_eligible_at"`, "retry_eligible", false},
			{"retry_eligible", "retry_eligible", true},
			{"retry_eligible_at retry_eligible", "retry_eligible", true},
			{"retry_eligible_at_extra", "retry_eligible", true},
			{"other_retry_eligible_at", "retry_eligible", true},
			{"delivery_retry_eligible_at", "delivery_retry_eligible", true},
			{"retry_eligible_at dispatchRetryDelay", "dispatchRetryDelay", true},
		} {
			if got := containsRetiredDeliveryDialect(path, cell.source, cell.forbidden); got != cell.want {
				t.Fatalf("path=%s text=%q forbidden=%q got=%t want=%t", path, cell.source, cell.forbidden, got, cell.want)
			}
		}
	}
	for _, path := range []string{"internal/runtime/deliverycontinuation/coordinator.go", "internal/runtime/manager/runtime.go", "internal/runtime/pipeline/coordinator.go", "other/adapter.go", "openrpc.json"} {
		if !containsRetiredDeliveryDialect(path, "retry_eligible_at", "retry_eligible") {
			t.Fatalf("column spelling granted another retry owner: %s", path)
		}
	}
}

func calledFunctionName(expr ast.Expr) string {
	switch current := expr.(type) {
	case *ast.Ident:
		return current.Name
	case *ast.SelectorExpr:
		return current.Sel.Name
	default:
		return ""
	}
}
