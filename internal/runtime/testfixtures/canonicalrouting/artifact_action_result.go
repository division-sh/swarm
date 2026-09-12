package canonicalrouting

import (
	"strings"
	"testing"
)

// CopyArtifactActionResultDelivery preserves the callback and recovery fixture
// with an explicit static child-to-parent publication variant.
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
mode: template
instance: request_id
initial_state: ready
terminal_states: [done]
states: [ready, done]
`,
		"repo-scaffold/entities.yaml": `test_entity:
  repo_url: text
  current_ref: text
  file_manifest: ArtifactManifest
  status: text
  failure: json
  last_request_id: text
  last_source_event_id: text
`,
		"repo-scaffold/types.yaml": `types:
  ArtifactProvenance:
    artifact_type: text
    source_record_id: text
  ArtifactManifestFile:
    path: text
    content_type: text
    sha256: text
    size_bytes: integer
  ArtifactManifest:
    provider: text
    repo_id: text
    namespace: text
    partition_key: text
    display_slug: text
    request_id: text
    source_event_id: text
    repo_url: text
    ref: text
    tree_hash: text
    files: [ArtifactManifestFile]
    provenance: ArtifactProvenance
`,
		"repo-scaffold/events.yaml": `repo_scaffold.repo_commit_requested:
  request_id: string
  mvp_yaml: string
repo_scaffold.repo_commit_succeeded:
  repo_id: string
  namespace: string
  partition_key: string?
  display_slug: string?
  request_id: string
  source_event_id: string
  repo_url: string
  current_ref: string
  file_manifest: ArtifactManifest
  provenance: ArtifactProvenance
  result_kind: string
repo_scaffold.repo_commit_failed:
  repo_id: string
  namespace: string
  partition_key: string?
  display_slug: string?
  request_id: string
  source_event_id: string
  failure: platform.failure/v1 envelope
  provenance: ArtifactProvenance
  result_kind: string
  request_copy: string?
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
      action:
        id: artifact_repo_commit
        artifact_repo:
          provider: local_git
          repo_id:
            ref: entity.repo_id
          namespace:
            ref: entity.namespace
          partition_key:
            ref: entity.partition_key
          display_slug:
            ref: entity.display_slug
          request_id:
            ref: payload.request_id
          author:
            literal: artifact-writer
          provenance:
            artifact_type:
              literal: fixture
            source_record_id:
              ref: entity.source_record_id
          allowed_paths:
            - specs/mvp.yaml
          files:
            - path:
                literal: specs/mvp.yaml
              content:
                ref: payload.mvp_yaml
              content_type: yaml
              schema:
                type: object
                required_fields:
                  - name
              max_bytes: 4096
          output:
            repo_url: repo_url
            current_ref: current_ref
            file_manifest: file_manifest
            status: status
            failure: failure
            last_request_id: last_request_id
            last_source_event_id: last_source_event_id
          limits:
            max_yaml_bytes: 4096
            max_repo_bytes: 1048576
          success_event: repo_scaffold.repo_commit_succeeded
          success_payload:
            result_kind:
              literal: ready
          failure_event: repo_scaffold.repo_commit_failed
          failure_payload:
            result_kind:
              literal: failed
            request_copy:
              ref: payload.request_id
    repo_scaffold.repo_commit_succeeded:
      sets_gate: result_callback_observed
    repo_scaffold.repo_commit_failed:
      sets_gate: result_callback_observed
`,
	}
}

func artifactActionResultStaticDeliveryFixtureFiles() map[string]string {
	files := artifactActionResultDeliveryFixtureFiles()
	files["repo-scaffold/schema.yaml"] = strings.Replace(files["repo-scaffold/schema.yaml"], "mode: template", "mode: static", 1)
	return files
}

func addArtifactActionResultChildRequest(files map[string]string) {
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
      - {event: start.requested, source: external}
  outputs:
    events: [repo_scaffold.repo_commit_requested]
`
	files["repo-scaffold/child-1/events.yaml"] = `start.requested:
  request_id: text
  mvp_yaml: text
repo_scaffold.repo_commit_requested:
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
        fields: {request_id: payload.request_id, mvp_yaml: payload.mvp_yaml}
`
}
