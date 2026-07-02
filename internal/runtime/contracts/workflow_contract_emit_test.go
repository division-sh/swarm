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
