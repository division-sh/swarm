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
