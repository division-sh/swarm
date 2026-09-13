package bootverify

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	c "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestEntityProgressivePresenceSourceLoadedFullVerify(t *testing.T) {
	canonicalrouting.Prove(t, canonicalrouting.ArtifactID("tests/conformance/entity-progressive-presence"))
	for _, variant := range []string{"ruled", "missing assessment write", "read before write", "retired stage spelling", "naive optional without decision", "naive optional fallback"} {
		t.Run(variant, func(t *testing.T) {
			repo := repoRootForBootverifyTest(t)
			root := t.TempDir()
			for _, name := range []string{"manifest.yaml", "schema.yaml", "entities.yaml", "events.yaml", "nodes.yaml"} {
				data, err := os.ReadFile(filepath.Join(repo, "tests", "conformance", "entity-progressive-presence", name))
				if err != nil {
					t.Fatal(err)
				}
				contents := string(data)
				if name == "entities.yaml" && strings.HasPrefix(variant, "naive optional") {
					contents = strings.Replace(contents, "business_brief: text", "business_brief: text?", 1)
				}
				if name == "nodes.yaml" {
					switch variant {
					case "missing assessment write":
						contents = strings.Replace(contents, "          - business_brief\n", "", 1)
					case "read before write":
						contents = strings.Replace(contents, `check: "_entity.current_state == 'assess'"`, `check: "_entity.current_state == 'assess' && entity.business_brief != ''"`, 1)
					case "retired stage spelling":
						contents = strings.ReplaceAll(contents, "_entity.current_state", "_entity.stage")
					case "naive optional without decision", "naive optional fallback":
						contents = strings.Replace(contents, "          - business_brief\n", "", 1)
						contents = strings.Replace(contents, "    work.assessed:\n", "    work.assessed:\n      rules:\n        - condition: payload.business_brief != ''\n          data_accumulation:\n            writes: [business_brief]\n          advances_to: consume\n          emit: {event: work.consume}\n        - condition: \"true\"\n          advances_to: consume\n          emit: {event: work.consume}\n", 1)
						contents = strings.Replace(contents, "      advances_to: consume\n      emit:\n        event: work.consume\n", "", 1)
						if variant == "naive optional fallback" {
							contents = strings.ReplaceAll(contents, "entity.business_brief", "entity.?business_brief.orValue('fallback')")
						}
					}
				}
				writeBootverifyFixtureFile(t, filepath.Join(root, name), contents)
			}
			bundle := loadFixtureBundleAt(t, repo, root, c.DefaultPlatformSpecFile(repo))
			report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
			if variant == "ruled" || variant == "naive optional fallback" {
				if len(report.Errors()) != 0 || len(report.Warnings()) != 0 {
					t.Fatalf("supported authoring rejected: errors=%#v warnings=%#v", report.Errors(), report.Warnings())
				}
			} else if variant == "naive optional without decision" {
				if !reportContains(report.Errors(), "emit_field_expression_validation", "presence decision") {
					t.Fatalf("optional declaration admitted without a decision: %#v", report.Errors())
				}
			} else if variant == "retired stage spelling" {
				if !reportContains(report.Errors(), "expression_field_reference_validation", "_entity.stage is not a supported") {
					t.Fatalf("unsupported stage alias admitted: %#v", report.Errors())
				}
			} else if !reportContains(report.Errors(), "expression_field_reference_validation", "not definitely assigned") {
				t.Fatalf("missing assignment rejection: %#v", report.Errors())
			}
		})
	}
}

func TestEntityDefiniteAssignmentLoops(t *testing.T) {
	for _, variant := range []string{"start assignment", "backedge only", "admitted callback assignment"} {
		t.Run(variant, func(t *testing.T) {
			root := canonicalrouting.CopyForkLoopGenerationNotice(t)
			writeBootverifyFixtureFile(t, filepath.Join(root, "review", "entities.yaml"), "work:\n  brief: text\n")
			path := filepath.Join(root, "review", "nodes.yaml")
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			nodes := strings.Replace(string(data), "summary: {literal: review processed}", "summary: {ref: entity.brief}", 1)
			writer := "    work.started:\n"
			switch variant {
			case "backedge only":
				writer = "    review.retry:\n"
			case "admitted callback assignment":
				writer = "    review.requested:\n"
			}
			nodes = strings.Replace(nodes, writer, writer+"      data_accumulation:\n        writes:\n          - {source_field: token, target_field: brief}\n", 1)
			writeBootverifyFixtureFile(t, path, nodes)
			repo := repoRootForBootverifyTest(t)
			bundle := loadFixtureBundleAt(t, repo, root, c.DefaultPlatformSpecFile(repo))
			source := semanticview.Wrap(bundle)
			checker := &checkerContext{ctx: context.Background(), source: source}
			findings := checker.expressionFieldReferences()
			missing := false
			for _, finding := range findings {
				missing = missing || strings.Contains(finding.Message, "entity.brief") && strings.Contains(finding.Message, "not definitely assigned")
			}
			if missing != (variant == "backedge only") {
				t.Fatalf("variant %s: findings=%#v", variant, findings)
			}
			analysis, err := engine.BuildEntityAssignmentAnalysis(source, "review")
			if err != nil {
				t.Fatal(err)
			}
			if got := analysis.StageFacts("working").Has("brief"); got != (variant == "start assignment") {
				t.Fatalf("first/repeated entry brief assigned=%v for %s", got, variant)
			}
			// Escape is a distinct outcome before a repeat's writes. It cannot
			// borrow the value that only the successful backedge would write.
			if got := analysis.StageFacts("exhausted").Has("brief"); got != (variant != "backedge only") {
				t.Fatalf("escape brief assigned=%v for %s", got, variant)
			}
		})
	}
}

func TestEntityDefiniteAssignmentProgramPoints(t *testing.T) {
	for _, tc := range []struct {
		name        string
		change      func(*c.SystemNodeEventHandler)
		wantMissing bool
	}{
		{"earlier write", func(h *c.SystemNodeEventHandler) {
			h.DataAccumulation.Writes = []c.WorkflowDataWrite{
				{TargetField: "base_score", Value: c.LiteralExpression(7)},
				{TargetField: "adjusted_score", Value: c.RefExpression("entity.base_score")},
			}
		}, false},
		{"guard before write", func(h *c.SystemNodeEventHandler) {
			h.Guard = &c.GuardSpec{Check: "entity.base_score > 0"}
			h.DataAccumulation.Writes = []c.WorkflowDataWrite{{TargetField: "base_score", Value: c.LiteralExpression(7)}}
		}, true},
		{"earlier guard proves later check", func(h *c.SystemNodeEventHandler) {
			h.Guard = &c.GuardSpec{Checks: []c.GuardCheck{{Check: "has(entity.base_score)"}, {Check: "entity.base_score > 0"}}}
		}, false},
		{"later guard cannot prove earlier check", func(h *c.SystemNodeEventHandler) {
			h.Guard = &c.GuardSpec{Checks: []c.GuardCheck{{Check: "entity.base_score > 0"}, {Check: "has(entity.base_score)"}}}
		}, true},
		{"guard OR is not assignment", func(h *c.SystemNodeEventHandler) {
			h.Guard = &c.GuardSpec{Checks: []c.GuardCheck{{Check: "has(entity.base_score) || payload.score > 0"}, {Check: "entity.base_score > 0"}}}
		}, true},
		{"RHS cannot prove itself", func(h *c.SystemNodeEventHandler) {
			h.DataAccumulation.Writes = []c.WorkflowDataWrite{{TargetField: "base_score", Value: c.CELExpression("entity.base_score + 1")}}
		}, true},
		{"branch is not merge proof", func(h *c.SystemNodeEventHandler) {
			h.Rules = []c.HandlerRuleEntry{{Condition: "payload.score > 0", DataAccumulation: c.WorkflowDataAccumulation{Writes: []c.WorkflowDataWrite{{TargetField: "base_score", Value: c.LiteralExpression(7)}}}}}
			h.DataAccumulation.Writes = []c.WorkflowDataWrite{{TargetField: "adjusted_score", Value: c.RefExpression("entity.base_score")}}
		}, true},
		{"selected branch read", func(h *c.SystemNodeEventHandler) {
			h.Rules = []c.HandlerRuleEntry{{Condition: "has(entity.base_score)", DataAccumulation: c.WorkflowDataAccumulation{Writes: []c.WorkflowDataWrite{{TargetField: "adjusted_score", Value: c.RefExpression("entity.base_score")}}}}}
		}, false},
		{"exhaustive branches both write", func(h *c.SystemNodeEventHandler) {
			h.Rules = []c.HandlerRuleEntry{
				{Condition: "payload.score > 0", DataAccumulation: c.WorkflowDataAccumulation{Writes: []c.WorkflowDataWrite{{TargetField: "base_score", Value: c.LiteralExpression(7)}}}},
				{Condition: "else", DataAccumulation: c.WorkflowDataAccumulation{Writes: []c.WorkflowDataWrite{{TargetField: "base_score", Value: c.LiteralExpression(0)}}}},
			}
			h.DataAccumulation.Writes = []c.WorkflowDataWrite{{TargetField: "adjusted_score", Value: c.RefExpression("entity.base_score")}}
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bundle := loadWave1ExpressionFixtureBundle(t)
			flow, node, event, handler := firstFlowHandlerInFlowView(t, bundle)
			tc.change(&handler)
			writeFlowHandler(t, bundle, flow, node, event, handler)
			checker := &checkerContext{ctx: context.Background(), source: semanticview.Wrap(bundle)}
			findings := checker.expressionFieldReferences()
			missing := false
			for _, finding := range findings {
				if strings.Contains(finding.Message, "not definitely assigned") {
					missing = true
				}
			}
			if missing != tc.wantMissing {
				t.Fatalf("missing=%t, want %t: %#v", missing, tc.wantMissing, findings)
			}
		})
	}
}

func TestEntityArtifactOutputAssignmentAtSourceLoadedStageRead(t *testing.T) {
	for _, handledFailure := range []bool{false, true} {
		for _, field := range []string{"repo_url", "current_ref", "status", "last_request_id", "last_source_event_id"} {
			name := "success_only/" + field
			failure := ""
			if handledFailure {
				name = "handled_failure/" + field
				failure = "          failure_event: artifact.failed\n"
			}
			t.Run(name, func(t *testing.T) {
				root := t.TempDir()
				writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: artifact-assignment\nstages:\n  ready: {initial: true}\n  done: {}\n")
				writeBootverifyFixtureFile(t, filepath.Join(root, "entities.yaml"), "work:\n  repo_url: text\n  current_ref: text\n  file_manifest: json\n  status: text\n  failure: json\n  last_request_id: uuid\n  last_source_event_id: uuid\n")
				writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), "work.requested: {}\nwork.observe: {}\nwork.observed: {}\nartifact.failed: {}\n")
				writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), `owner:
  execution_type: system_node
  subscribes_to: [work.requested, work.observe]
  event_handlers:
    work.requested:
      guard: {check: "_entity.current_state == 'ready'"}
      action:
        id: artifact_repo_commit
        artifact_repo:
          provider: local_git
          repo_id: {literal: "11111111-1111-1111-1111-111111111111"}
          request_id: {literal: "22222222-2222-2222-2222-222222222222"}
          allowed_paths: [note.txt]
          files:
            - path: {literal: note.txt}
              content: {literal: proof}
              content_type: text
          output:
            repo_url: repo_url
            current_ref: current_ref
            file_manifest: file_manifest
            status: status
            failure: failure
            last_request_id: last_request_id
            last_source_event_id: last_source_event_id
`+failure+`      advances_to: done
    work.observe:
      guard: {check: "_entity.current_state == 'done'"}
      emit:
        event: work.observed
        fields:
          observed: {expression: "string(entity.`+field+`)"}
`)
				bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, c.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))
				checker := &checkerContext{ctx: context.Background(), source: semanticview.Wrap(bundle)}
				findings := checker.expressionFieldReferences()
				missing := false
				for _, finding := range findings {
					missing = missing || strings.Contains(finding.Message, "not definitely assigned")
				}
				wantMissing := handledFailure && (field == "repo_url" || field == "current_ref" || field == "file_manifest")
				if missing != wantMissing || !wantMissing && len(findings) != 0 {
					t.Fatalf("post-action emit missing=%v want=%v: %#v", missing, wantMissing, findings)
				}
			})
		}
	}
}

func TestEntityDefiniteAssignmentStages(t *testing.T) {
	for _, variant := range []string{"all paths write", "guard stage then value", "guard short circuit", "bypass", "same destination outcomes", "on_complete outcomes", "same event different node", "zero trip", "backedge cannot prove first entry"} {
		t.Run(variant, func(t *testing.T) {
			root := t.TempDir()
			writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: assignment-proof\n")
			writeBootverifyFixtureFile(t, filepath.Join(root, "child", "schema.yaml"), `name: child
stages:
  idle: {initial: true}
  assess: {}
  consume: {}
  done: {terminal: true}
`)
			writeBootverifyFixtureFile(t, filepath.Join(root, "child", "entities.yaml"), "work:\n  score: {type: integer}\n")
			writeBootverifyFixtureFile(t, filepath.Join(root, "child", "events.yaml"), `work.opened: {}
work.scored:
  score: integer
work.consume: {}
work.bypass: {}
work.result:
  score: integer
`)
			nodes := `owner:
  execution_type: system_node
  subscribes_to: [work.opened, work.scored, work.consume, work.bypass]
  produces: [work.result]
  event_handlers:
    work.opened:
      create_entity: true
      advances_to: assess
    work.scored:
      guard: {check: "_entity.current_state == 'assess'"}
      data_accumulation:
        writes: [score]
      advances_to: consume
    work.consume:
      guard: {check: "_entity.current_state == 'consume'"}
      emit:
        event: work.result
        fields:
          score: {ref: entity.score}
      advances_to: done
`
			if variant == "bypass" {
				nodes += "    work.bypass:\n      guard: {check: \"_entity.current_state == 'assess'\"}\n      advances_to: consume\n"
			}
			if variant == "guard stage then value" {
				nodes = strings.Replace(nodes, `guard: {check: "_entity.current_state == 'consume'"}`, `guard:
        checks:
          - check: "_entity.current_state == 'consume'"
          - check: "entity.score > 0"`, 1)
			}
			if variant == "guard short circuit" {
				nodes = strings.Replace(nodes, `guard: {check: "_entity.current_state == 'consume'"}`, `guard: {check: "_entity.current_state == 'consume' && entity.score > 0"}`, 1)
			}
			if variant == "same destination outcomes" || variant == "on_complete outcomes" {
				nodes = strings.Replace(nodes, "      data_accumulation:\n        writes: [score]\n      advances_to: consume", `      rules:
        - condition: payload.score >= 0
          data_accumulation:
            writes: [score]
          advances_to: consume
        - condition: else
          advances_to: consume`, 1)
				if variant == "on_complete outcomes" {
					nodes = strings.Replace(nodes, "      rules:\n", "      on_complete:\n", 1)
				}
			}
			if variant == "same event different node" {
				nodes += `other-owner:
  execution_type: system_node
  subscribes_to: [work.scored]
  event_handlers:
    work.scored:
      guard: {check: "_entity.current_state == 'assess'"}
      advances_to: consume
`
			}
			if variant == "zero trip" {
				nodes = strings.Replace(nodes, "      advances_to: assess", "      advances_to: consume", 1)
			}
			if variant == "backedge cannot prove first entry" {
				nodes = strings.Replace(nodes, "      advances_to: assess", "      advances_to: consume", 1)
				nodes += "    work.bypass:\n      guard: {check: \"_entity.current_state == 'consume'\"}\n      advances_to: assess\n"
			}
			writeBootverifyFixtureFile(t, filepath.Join(root, "child", "nodes.yaml"), nodes)
			if variant == "same event different node" {
				_, err := c.LoadWorkflowContractBundleWithOverrides(repoRootForBootverifyTest(t), root, c.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))
				if err == nil || !strings.Contains(err.Error(), "multiple authoritative system node owners") {
					t.Fatalf("duplicate owner was not rejected at the earlier ownership gate: %v", err)
				}
				return
			}
			bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, c.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))
			checker := &checkerContext{ctx: context.Background(), source: semanticview.Wrap(bundle)}
			findings := checker.expressionFieldReferences()
			missing := false
			for _, finding := range findings {
				if strings.Contains(finding.Message, "not definitely assigned") {
					missing = true
				}
			}
			wantMissing := variant == "bypass" || variant == "same destination outcomes" || variant == "on_complete outcomes" || variant == "same event different node" || variant == "zero trip" || variant == "backedge cannot prove first entry"
			if missing != wantMissing {
				t.Fatalf("missing=%t, want %t: %#v", missing, wantMissing, findings)
			}
		})
	}
}

func TestEntityDefiniteAssignmentStructuralMutations(t *testing.T) {
	for _, tc := range []struct {
		name, writes string
		missing      bool
	}{
		{"missing named parent", `        - target_field: profile.id
          value: replacement
`, true},
		{"earlier complete parent", `        - target_field: profile
          value: {id: original}
        - target_field: profile.id
          value: replacement
`, false},
		{"literal optional member", `        - target_field: profile
          value: {id: original, note: supplied}
        - target_field: observed
          expression: entity.profile.note
`, false},
		{"replacement forgets optional member", `        - target_field: profile
          value: {id: original, note: supplied}
        - target_field: profile
          value: {id: replacement}
        - target_field: observed
          expression: entity.profile.note
`, true},
		{"constructive root append", `        - op: append
          target: entity.notes
          value: supplied
        - target_field: observed
          expression: "string(entity.notes.size())"
`, false},
		{"merge cannot construct", `        - op: merge
          target: entity.by_id
          key: one
          value: {id: supplied}
`, true},
		{"clear absent parent is noop", `        - op: clear
          target: entity.profile.note
`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: mutation-assignment\n")
			writeBootverifyFixtureFile(t, filepath.Join(root, "types.yaml"), "types:\n  Profile:\n    id: text\n    note: text?\n")
			writeBootverifyFixtureFile(t, filepath.Join(root, "entities.yaml"), "work:\n  profile: Profile\n  observed: text\n  notes: '[text]'\n  by_id: map[text]Profile\n")
			writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), "work.requested: {}\n")
			writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), `owner:
  execution_type: system_node
  subscribes_to: [work.requested]
  event_handlers:
    work.requested:
      data_accumulation:
        writes:
`+tc.writes)
			bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, c.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))
			checker := &checkerContext{ctx: context.Background(), source: semanticview.Wrap(bundle)}
			findings := checker.expressionFieldReferences()
			missing := false
			for _, finding := range findings {
				missing = missing || strings.Contains(finding.Message, "not definitely assigned")
			}
			if missing != tc.missing {
				t.Fatalf("missing=%t, want %t: %#v", missing, tc.missing, findings)
			}
		})
	}
}
