package canonicalrouting

import (
	"fmt"
	"testing"
)

// CopyPublicationArtifact executes the real artifact action and observes both
// result arms locally and across an explicit connection.
func CopyPublicationArtifact(t testing.TB, mode string) string {
	t.Helper()
	root, prefix, flow := t.TempDir(), "", "."
	if mode == "static" {
		prefix, flow = "source/", "source"
	} else if mode != "root" {
		t.Fatalf("unsupported artifact publication topology %q", mode)
	}
	connect := fmt.Sprintf("connect:\n  - {event: commit.ok, from: %s, to: sink}\n  - {event: commit.failed, from: %s, to: sink}\n", flow, flow)
	schema := "name: artifact-publication\npins:\n  inputs:\n    events:\n      - {event: artifact.requested, source: external}\n  outputs:\n    events: [commit.ok, commit.failed]\n"
	if flow == "." {
		schema += connect
	} else {
		writeClosedVariantFile(t, root, "schema.yaml", "name: artifact-root\n"+connect)
	}
	writeClosedVariantFile(t, root, prefix+"schema.yaml", schema)
	writeClosedVariantFile(t, root, prefix+"types.yaml", `types:
  Provenance:
    artifact_type: text
  ManifestFile:
    path: text
    content_type: text
    sha256: text
    size_bytes: integer
  Manifest:
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
    files: [ManifestFile]
    provenance: Provenance
`)
	writeClosedVariantFile(t, root, prefix+"entities.yaml", `artifact:
  repo_url: text
  current_ref: text
  file_manifest: Manifest
  status: text
  failure: json
  last_request_id: text
  last_source_event_id: text
`)
	writeClosedVariantFile(t, root, prefix+"events.yaml", `artifact.requested:
  repo_id: text
  request_id: text
  document: text
commit.ok:
  repo_id: text
  namespace: text
  partition_key: text?
  display_slug: text?
  request_id: text
  source_event_id: text
  repo_url: text
  current_ref: text
  file_manifest: Manifest
  provenance: Provenance
  result_kind: text
commit.failed:
  repo_id: text
  namespace: text
  partition_key: text?
  display_slug: text?
  request_id: text
  source_event_id: text
  failure: platform.failure/v1 envelope
  provenance: Provenance
  result_kind: text
`)
	local := `local:
  execution_type: system_node
  subscribes_to: [commit.ok, commit.failed]
  event_handlers:
    commit.ok:
      guard: {id: success, check: "payload.result_kind == 'ready'"}
    commit.failed:
      guard: {id: failure, check: "payload.result_kind == 'failed'"}
`
	writeClosedVariantFile(t, root, prefix+"nodes.yaml", `writer:
  execution_type: system_node
  subscribes_to: [artifact.requested]
  event_handlers:
    artifact.requested:
      action:
        id: artifact_repo_commit
        artifact_repo:
          provider: local_git
          repo_id: {ref: payload.repo_id}
          namespace: {literal: publication-proof}
          request_id: {ref: payload.request_id}
          provenance:
            artifact_type: {literal: proof}
          allowed_paths: [document.yaml]
          files:
            - path: {literal: document.yaml}
              content: {ref: payload.document}
              content_type: yaml
              schema: {type: object, required_fields: [name]}
              max_bytes: 4096
          output:
            repo_url: repo_url
            current_ref: current_ref
            file_manifest: file_manifest
            status: status
            failure: failure
            last_request_id: last_request_id
            last_source_event_id: last_source_event_id
          success_event: commit.ok
          success_payload: {result_kind: {literal: ready}}
          failure_event: commit.failed
          failure_payload: {result_kind: {literal: failed}}
`+local)
	writeClosedVariantFile(t, root, "sink/schema.yaml", "name: sink\npins:\n  inputs:\n    events: [commit.ok, commit.failed]\n")
	writeClosedVariantFile(t, root, "sink/nodes.yaml", local)
	return root
}
