package contracts

import (
	"reflect"
	"testing"
)

func TestHandlerEmitEventsIncludesArtifactRepoCommitResults(t *testing.T) {
	handler := SystemNodeEventHandler{
		Emit: EmitSpec{Event: "handler.emitted"},
		Action: ActionSpec{
			ID: "artifact_repo_commit",
			ArtifactRepo: &ArtifactRepoSpec{
				SuccessEvent: "artifact_repo.commit_completed",
				FailureEvent: "artifact_repo.commit_failed",
			},
		},
		Rules: []HandlerRuleEntry{{
			Emit: EmitSpec{Event: "rule.emitted"},
			Action: ActionSpec{
				ID: "artifact_repo_commit",
				ArtifactRepo: &ArtifactRepoSpec{
					SuccessEvent: "artifact_repo.rule_completed",
					FailureEvent: "artifact_repo.rule_failed",
				},
			},
		}},
		Branch: []BranchSpec{{
			Then: &HandlerRuleEntry{Emit: EmitSpec{Event: "branch.then.emitted"}},
			Else: &HandlerRuleEntry{
				Emit: EmitSpec{Event: "branch.else.emitted"},
				Action: ActionSpec{
					ID: "artifact_repo_commit",
					ArtifactRepo: &ArtifactRepoSpec{
						SuccessEvent: "artifact_repo.branch_completed",
						FailureEvent: "artifact_repo.branch_failed",
					},
				},
			},
		}},
	}

	got := HandlerEmitEvents(handler)
	want := []string{
		"handler.emitted",
		"artifact_repo.commit_completed",
		"artifact_repo.commit_failed",
		"rule.emitted",
		"artifact_repo.rule_completed",
		"artifact_repo.rule_failed",
		"branch.then.emitted",
		"branch.else.emitted",
		"artifact_repo.branch_completed",
		"artifact_repo.branch_failed",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("HandlerEmitEvents() = %#v, want %#v", got, want)
	}
}

func TestHandlerHasNestedEmitSitesIncludesBranch(t *testing.T) {
	handler := SystemNodeEventHandler{
		Branch: []BranchSpec{{
			Then: &HandlerRuleEntry{Emit: EmitSpec{Event: "branch.then.emitted"}},
		}},
	}
	if !HandlerHasNestedEmitSites(handler) {
		t.Fatal("HandlerHasNestedEmitSites() = false, want true for branch emit sites")
	}
}

func TestHandlerEmitEventsIncludesOnSuccessAfterRules(t *testing.T) {
	handler := SystemNodeEventHandler{
		OnSuccess: HandlerOnSuccessSpec{Emit: EmitSpec{Event: "handler.succeeded"}},
		Rules: []HandlerRuleEntry{{
			Emit: EmitSpec{Event: "rule.emitted"},
		}},
	}

	got := HandlerEmitEvents(handler)
	want := []string{"rule.emitted", "handler.succeeded"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("HandlerEmitEvents() = %#v, want %#v", got, want)
	}
}

func TestHandlerRuleEmitTemplateSitesMergeHandlerTemplateWithRuleFields(t *testing.T) {
	handler := SystemNodeEventHandler{
		Emit: EmitSpec{
			Event: "account.bucketed",
			Fields: map[string]ExpressionValue{
				"account_id": CELExpression("payload.account_id"),
			},
		},
		Rules: []HandlerRuleEntry{
			{
				ID:        "high",
				Condition: "payload.score >= 80",
				Emit: EmitSpec{Fields: map[string]ExpressionValue{
					"bucket": CELExpression(`"high"`),
				}},
			},
			{
				ID:        "low",
				Condition: "else",
				Emit: EmitSpec{Fields: map[string]ExpressionValue{
					"bucket": CELExpression(`"low"`),
				}},
			},
		},
	}

	sites := HandlerRuleEmitTemplateSites(handler)
	if got := len(sites); got != 2 {
		t.Fatalf("HandlerRuleEmitTemplateSites len = %d, want 2", got)
	}
	if got := HandlerEmitEvents(handler); !reflect.DeepEqual(got, []string{"account.bucketed"}) {
		t.Fatalf("HandlerEmitEvents = %#v, want one effective account.bucketed", got)
	}
	if got := sites[0].Spec.EventType(); got != "account.bucketed" {
		t.Fatalf("merged event = %q, want account.bucketed", got)
	}
	if _, ok := sites[0].Spec.Fields["account_id"]; !ok {
		t.Fatalf("merged site missing handler field: %#v", sites[0].Spec.Fields)
	}
	if expr := sites[0].Spec.Fields["bucket"]; expr.Kind != ExpressionKindCEL || expr.CEL != `"high"` {
		t.Fatalf("merged bucket expr = %#v, want CEL \"high\"", expr)
	}
}
