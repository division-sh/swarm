package canonicalrouting

import (
	"strings"
	"testing"
)

// CopyArtifactActionResultDelivery retains historical fixture coordinates for
// callback and recovery consumers. Results now come from ordinary authored
// rules, not artifact_repo_commit: the two fixed document strings select
// business branches and do not prove YAML validation or any Git operation.
func CopyArtifactActionResultDelivery(t testing.TB, mode string, childRequest bool) string {
	t.Helper()
	files := artifactActionResultDeliveryFixtureFiles()
	switch mode {
	case "template":
		if childRequest {
			t.Fatal("child request requires the static artifact callback fixture")
		}
	case "static":
		files = artifactActionResultStaticDeliveryFixtureFiles()
		if childRequest {
			addArtifactActionResultChildRequest(files)
		}
	default:
		t.Fatalf("unsupported artifact callback mode %q", mode)
	}
	root := t.TempDir()
	for name, body := range files {
		writeClosedVariantFile(t, root, name, body)
	}
	return root
}

func artifactActionResultDeliveryFixtureFiles() map[string]string {
	return map[string]string{
		"schema.yaml": "name: artifact-action-result-delivery\n",
		"repo-scaffold/schema.yaml": `name: repo-scaffold
instance: request_id
stages:
  ready: {initial: true}
  done: {terminal: true}
`,
		"repo-scaffold/entities.yaml": "test_entity:\n  request_id: {type: text, _unused_reason: receiver instance identity}\n",
		"repo-scaffold/events.yaml": `repo_scaffold.repo_commit_requested:
  request_id: string
  mvp_yaml: string
repo_scaffold.repo_commit_succeeded:
  request_id: string
  result_kind: string
repo_scaffold.repo_commit_failed:
  request_id: string
  result_kind: string
  request_copy: string
`,
		"repo-scaffold/nodes.yaml": `repo-scaffold-node:
  execution_type: system_node
  subscribes_to:
    - repo_scaffold.repo_commit_requested
    - repo_scaffold.repo_commit_succeeded
    - repo_scaffold.repo_commit_failed
  produces:
    - repo_scaffold.repo_commit_succeeded
    - repo_scaffold.repo_commit_failed
  event_handlers:
    repo_scaffold.repo_commit_requested:
      rules:
        ready:
          when: 'payload.mvp_yaml == "name: Demo\n"'
          emit:
            event: repo_scaffold.repo_commit_succeeded
            fields:
              request_id: ${payload.request_id}
              result_kind: {literal: ready}
        rejected:
          else: true
          emit:
            event: repo_scaffold.repo_commit_failed
            fields:
              request_id: ${payload.request_id}
              request_copy: ${payload.request_id}
              result_kind: {literal: failed}
    repo_scaffold.repo_commit_succeeded:
      sets_gate: result_callback_observed
    repo_scaffold.repo_commit_failed:
      sets_gate: result_callback_observed
`,
	}
}

func artifactActionResultStaticDeliveryFixtureFiles() map[string]string {
	files := artifactActionResultDeliveryFixtureFiles()
	files["repo-scaffold/schema.yaml"] = strings.Replace(files["repo-scaffold/schema.yaml"], "instance: request_id\n", "", 1)
	return files
}

func addArtifactActionResultChildRequest(files map[string]string) {
	files["schema.yaml"] += "pins:\n  inputs:\n    events: [start.requested]\n  outputs:\n    events: [start.requested]\nconnect:\n  - {event: start.requested, from: ., to: repo-scaffold/child-1}\n"
	files["events.yaml"] = "start.requested:\n  request_id: text\n  mvp_yaml: text\n"
	files["repo-scaffold/events.yaml"] = strings.TrimPrefix(files["repo-scaffold/events.yaml"], "repo_scaffold.repo_commit_requested:\n  request_id: string\n  mvp_yaml: string\n")
	files["repo-scaffold/schema.yaml"] += `pins:
  inputs:
    events: [repo_scaffold.repo_commit_requested]
connect:
  - {event: repo_scaffold.repo_commit_requested, from: child-1, to: .}
`
	files["repo-scaffold/child-1/schema.yaml"] = `name: child-requester
pins:
  inputs:
    events:
      - start.requested
  outputs:
    events: [repo_scaffold.repo_commit_requested]
`
	files["repo-scaffold/child-1/events.yaml"] = `repo_scaffold.repo_commit_requested:
  request_id: text
  mvp_yaml: text
`
	files["repo-scaffold/child-1/nodes.yaml"] = `requester:
  execution_type: system_node
  subscribes_to: [start.requested]
  event_handlers:
    start.requested:
      emit:
        event: repo_scaffold.repo_commit_requested
        fields: {request_id: "${payload.request_id}", mvp_yaml: "${payload.mvp_yaml}"}
`
}
