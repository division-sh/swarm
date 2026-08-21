package deliverycontinuation

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
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
	}
	scanConsumers := map[string]int{}
	pipelineSettlementConsumers := map[string]int{}
	err := filepath.WalkDir(repoRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "vendor":
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
				if strings.Contains(string(raw), forbidden) {
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
				if strings.Contains(string(raw), strconv.Quote(forbidden)) ||
					strings.Contains(string(raw), forbidden) {
					t.Errorf("%s retains forbidden executable-delivery dialect %q", rel, forbidden)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int{
		"internal/runtime/deliverycontinuation/coordinator.go":                      1,
		"internal/store/internal/runtimepersistence/facade_forwarders_generated.go": 2,
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
