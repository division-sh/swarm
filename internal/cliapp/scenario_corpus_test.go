package cliapp

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/scenarioderivation"
	"github.com/division-sh/swarm/internal/runtime/scenariodocument"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

// These are the 96 accepted public resource witnesses from #2487, not private
// conformance fixture or runtime execution credit.
func TestScenarioAcceptedCorpusSharesAdmissionAndDerivedProjection(t *testing.T) {
	paths := []string{
		"examples/integrations/telegram-agent/tests/smoke.yaml",
		"examples/routing/parent-connect/tests/full-path.yaml",
		"examples/routing/root-ingress/tests/visible-smoke.yaml",
		"examples/routing/template-create-minted-key/tests/full-path.yaml",
		"examples/routing/template-reply/tests/full-path.yaml",
		"examples/routing/template-select-existing/tests/full-path.yaml",
		"examples/routing/template-select-or-create/tests/full-path.yaml",
		"internal/cliapp/archetypes/zero-agent-automation/tests/smoke.yaml",
		"tests/conformance/entity-progressive-presence/tests/visible-smoke.yaml",
		"tests/tier1-primitives/test-advances-to-terminal/tests/visible-smoke.yaml",
		"tests/tier1-primitives/test-advances-to/tests/visible-smoke.yaml",
		"tests/tier1-primitives/test-clear-gates/tests/visible-smoke.yaml",
		"tests/tier1-primitives/test-compute-standalone/tests/visible-smoke.yaml",
		"tests/tier1-primitives/test-data-accumulation-direct/tests/visible-smoke.yaml",
		"tests/tier1-primitives/test-data-accumulation-literal/tests/visible-smoke.yaml",
		"tests/tier1-primitives/test-data-accumulation-mapped/tests/visible-smoke.yaml",
		"tests/tier1-primitives/test-emits-multiple/tests/visible-smoke.yaml",
		"tests/tier1-primitives/test-emits-payload-transform/tests/visible-smoke.yaml",
		"tests/tier1-primitives/test-from-filter/tests/visible-smoke.yaml",
		"tests/tier1-primitives/test-guard-compound-condition/tests/visible-smoke.yaml",
		"tests/tier1-primitives/test-guard-discard/tests/visible-smoke.yaml",
		"tests/tier1-primitives/test-guard-entity-ref/tests/visible-smoke.yaml",
		"tests/tier1-primitives/test-guard-escalate/tests/visible-smoke.yaml",
		"tests/tier1-primitives/test-guard-kill/tests/visible-smoke.yaml",
		"tests/tier1-primitives/test-guard-multi-fail/tests/visible-smoke.yaml",
		"tests/tier1-primitives/test-guard-multi/tests/visible-smoke.yaml",
		"tests/tier1-primitives/test-guard-pass/tests/visible-smoke.yaml",
		"tests/tier1-primitives/test-guard-policy-ref/tests/visible-smoke.yaml",
		"tests/tier1-primitives/test-guard-reject/tests/visible-smoke.yaml",
		"tests/tier1-primitives/test-on-complete-first-match/tests/visible-smoke.yaml",
		"tests/tier1-primitives/test-on-complete-second-match/tests/visible-smoke.yaml",
		"tests/tier1-primitives/test-on-complete-with-state/tests/visible-smoke.yaml",
		"tests/tier1-primitives/test-payload-transform-multi-source/tests/visible-smoke.yaml",
		"tests/tier1-primitives/test-record-evidence/tests/visible-smoke.yaml",
		"tests/tier1-primitives/test-rules-advances-to/tests/visible-smoke.yaml",
		"tests/tier1-primitives/test-rules-data-accumulation/tests/visible-smoke.yaml",
		"tests/tier1-primitives/test-rules-else/tests/visible-smoke.yaml",
		"tests/tier1-primitives/test-rules-match/tests/visible-smoke.yaml",
		"tests/tier1-primitives/test-rules-no-match/tests/visible-smoke.yaml",
		"tests/tier1-primitives/test-sets-gate/tests/visible-smoke.yaml",
		"tests/tier10-policy-patterns/test-policy-capacity-query/tests/visible-smoke.yaml",
		"tests/tier10-policy-patterns/test-policy-counter-escalate/tests/visible-smoke.yaml",
		"tests/tier10-policy-patterns/test-policy-hard-gate-override/tests/visible-smoke.yaml",
		"tests/tier10-policy-patterns/test-policy-multi-guard-partial/tests/visible-smoke.yaml",
		"tests/tier10-policy-patterns/test-policy-threshold-three-way/tests/visible-smoke.yaml",
		"tests/tier10-policy-patterns/test-policy-timeout-elapsed/tests/visible-smoke.yaml",
		"tests/tier11-flow-composition/test-child-flow-absolute-path/tests/visible-smoke.yaml",
		"tests/tier11-flow-composition/test-child-flow-pin-wiring/tests/visible-smoke.yaml",
		"tests/tier11-flow-composition/test-child-flow-policy-inherit/tests/visible-smoke.yaml",
		"tests/tier11-flow-composition/test-data-pin-wiring/tests/visible-smoke.yaml",
		"tests/tier11-flow-composition/test-multi-level-policy-inherit/tests/visible-smoke.yaml",
		"tests/tier11-flow-composition/test-wildcard-deep-subscription/tests/visible-smoke.yaml",
		"tests/tier12-runtime-fork/test-non-agent-replay-fail-closed/tests/visible-smoke.yaml",
		"tests/tier3-list-processing/test-fan-out-basic/tests/visible-smoke.yaml",
		"tests/tier3-list-processing/test-fan-out-count/tests/visible-smoke.yaml",
		"tests/tier3-list-processing/test-fan-out-emit-mapping/tests/visible-smoke.yaml",
		"tests/tier3-list-processing/test-fan-out-empty/tests/visible-smoke.yaml",
		"tests/tier3-list-processing/test-filter-basic/tests/visible-smoke.yaml",
		"tests/tier3-list-processing/test-filter-empty/tests/visible-smoke.yaml",
		"tests/tier3-list-processing/test-group-by-standalone/tests/visible-smoke.yaml",
		"tests/tier3-list-processing/test-reduce-count/tests/visible-smoke.yaml",
		"tests/tier3-list-processing/test-reduce-max/tests/visible-smoke.yaml",
		"tests/tier3-list-processing/test-reduce-min/tests/visible-smoke.yaml",
		"tests/tier3-list-processing/test-reduce-operation-count/tests/visible-smoke.yaml",
		"tests/tier4-cross-entity/test-clear-multiple-targets/tests/visible-smoke.yaml",
		"tests/tier4-cross-entity/test-clear-state/tests/visible-smoke.yaml",
		"tests/tier4-cross-entity/test-create-entity/tests/visible-smoke.yaml",
		"tests/tier4-cross-entity/test-query-filter/tests/visible-smoke.yaml",
		"tests/tier4-cross-entity/test-query-group-by/tests/visible-smoke.yaml",
		"tests/tier5-flow-lifecycle/test-auto-emit-on-create/tests/visible-smoke.yaml",
		"tests/tier5-flow-lifecycle/test-terminal-state-preserves/tests/visible-smoke.yaml",
		"tests/tier5-flow-lifecycle/test-terminal-state-rejects/tests/visible-smoke.yaml",
		"tests/tier5-flow-lifecycle/test-timer-cancel/tests/visible-smoke.yaml",
		"tests/tier5-flow-lifecycle/test-timer-start-on/tests/visible-smoke.yaml",
		"tests/tier5-flow-lifecycle/test-wildcard-subscription/tests/visible-smoke.yaml",
		"tests/tier6-event-loop/test-atomicity-guard-rollback/tests/visible-smoke.yaml",
		"tests/tier6-event-loop/test-atomicity-rollback/tests/visible-smoke.yaml",
		"tests/tier6-event-loop/test-dead-letter/tests/visible-smoke.yaml",
		"tests/tier6-event-loop/test-guards-pre-handler-state/tests/visible-smoke.yaml",
		"tests/tier6-event-loop/test-on-complete-atomicity-chain/tests/visible-smoke.yaml",
		"tests/tier7-composition/test-agent-emits-to-node/tests/visible-smoke.yaml",
		"tests/tier7-composition/test-dual-delivery/tests/visible-smoke.yaml",
		"tests/tier7-composition/test-full-lifecycle/tests/visible-smoke.yaml",
		"tests/tier7-composition/test-multi-gate-pipeline/tests/visible-smoke.yaml",
		"tests/tier7-composition/test-two-node-chain/tests/visible-smoke.yaml",
		"tests/tier7-composition/test-wildcard-cross-flow/tests/visible-smoke.yaml",
		"tests/tier9-composition-patterns/test-compose-accumulate-compute-branch/tests/visible-smoke.yaml",
		"tests/tier9-composition-patterns/test-compose-clear-gates-reenter/tests/visible-smoke.yaml",
		"tests/tier9-composition-patterns/test-compose-gate-chain-three/tests/visible-smoke.yaml",
		"tests/tier9-composition-patterns/test-compose-gate-data-advance-emit/tests/visible-smoke.yaml",
		"tests/tier9-composition-patterns/test-compose-guard-counter-escalate/tests/visible-smoke.yaml",
		"tests/tier9-composition-patterns/test-compose-guard-multi-source/tests/visible-smoke.yaml",
		"tests/tier9-composition-patterns/test-compose-guard-query-capacity/tests/visible-smoke.yaml",
		"tests/tier9-composition-patterns/test-compose-lifecycle-seven-states/tests/visible-smoke.yaml",
		"tests/tier9-composition-patterns/test-compose-rules-fanout-data/tests/visible-smoke.yaml",
		"tests/tier9-composition-patterns/test-compose-rules-per-rule-data/tests/visible-smoke.yaml",
	}
	for _, candidate := range paths {
		t.Run(candidate, func(t *testing.T) {
			split := strings.LastIndex(candidate, "/tests/")
			if split < 0 {
				t.Fatal("missing tests resource coordinate")
			}
			root := filepath.Join(RepoRoot(), filepath.FromSlash(candidate[:split]))
			label := candidate[split+1:]
			artifact, err := sourceartifact.AdmitDirectory(root)
			if err != nil {
				t.Fatal(err)
			}
			files, err := scenarioTestFilesByLabel(artifact)
			if err != nil {
				t.Fatal(err)
			}
			file, found := files[label]
			if !found {
				t.Fatalf("missing admitted resource %s", label)
			}
			admitted, found, err := scenariodocument.Discover(file.Raw, label)
			if err != nil || !found {
				t.Fatalf("discovery: found=%v err=%v", found, err)
			}
			cli, err := admitted.Projection()
			if err != nil {
				t.Fatal(err)
			}
			declaration, found, err := scenarioderivation.ParseDeclaration(file.Raw, label)
			if err != nil {
				t.Fatal(err)
			}
			if found != (cli.Derive != nil) {
				t.Fatalf("CLI/derived branch disagreement: %#v %#v", cli.Derive, declaration)
			}
			if found && (declaration.Name != cli.Name || declaration.FlowID != cli.Derive.FlowID || declaration.Input != cli.Derive.Input) {
				t.Fatalf("projection identity disagreement: %#v %#v", cli.Derive, declaration)
			}
		})
	}
}
