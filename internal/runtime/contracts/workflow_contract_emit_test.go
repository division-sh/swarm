package contracts

import (
	"reflect"
	"strings"
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
	}

	got := HandlerEmitEvents(handler)
	want := []string{
		"handler.emitted",
		"artifact_repo.commit_completed",
		"artifact_repo.commit_failed",
		"rule.emitted",
		"artifact_repo.rule_completed",
		"artifact_repo.rule_failed",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("HandlerEmitEvents() = %#v, want %#v", got, want)
	}
}

func TestHandlerEmitEventsExcludesUnsupportedCompletionActionResults(t *testing.T) {
	completionAction := ActionSpec{
		ID: "artifact_repo_commit",
		ArtifactRepo: &ArtifactRepoSpec{
			SuccessEvent: "artifact_repo.completion_succeeded",
			FailureEvent: "artifact_repo.completion_failed",
		},
	}
	handler := SystemNodeEventHandler{
		OnComplete: []HandlerRuleEntry{{
			Action: completionAction,
			Emit:   EmitSpec{Event: "handler.completed"},
		}},
		Join: &JoinSpec{
			OnComplete: HandlerRuleEntry{Action: completionAction, Emit: EmitSpec{Event: "join.completed"}},
			Timeout:    JoinTimeoutSpec{Outcome: HandlerRuleEntry{Action: completionAction, Emit: EmitSpec{Event: "join.timed_out"}}},
		},
	}

	want := []string{"handler.completed", "join.completed", "join.timed_out"}
	if got := HandlerEmitEvents(handler); !reflect.DeepEqual(got, want) {
		t.Fatalf("HandlerEmitEvents() = %#v, want %#v", got, want)
	}
	for _, site := range HandlerDeclarativeEmitSites(handler) {
		if site.Source == "handler.on_complete.action.success" || site.Source == "handler.on_complete.action.failure" ||
			strings.Contains(site.Source, "handler.join.on_complete.action") || strings.Contains(site.Source, "handler.join.timeout.action") {
			t.Fatalf("unsupported completion action created declarative emit site: %#v", site)
		}
	}
}

func TestHandlerHasNestedEmitSitesIncludesRules(t *testing.T) {
	handler := SystemNodeEventHandler{
		Rules: []HandlerRuleEntry{{
			Emit: EmitSpec{Event: "rules.then.emitted"},
		}},
	}
	if !HandlerHasNestedEmitSites(handler) {
		t.Fatal("HandlerHasNestedEmitSites() = false, want true for rules emit sites")
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
