package canonicalrouting

import (
	"fmt"
	"path/filepath"
	"testing"
)

// Source execution uses a real connector and reply publication. A child at the
// request cut must consume its recorded result without invoking the connector.
func CopyRecordedActivityReply(t testing.TB, providerURL string) string {
	t.Helper()
	root := CopyRootReplyBoundary(t, true, true)
	applyClosedReplacement(t, filepath.Join(root, "nodes.yaml"), "    provider.replied:\n      emit:\n", "    provider.replied:\n      advances_to: done\n      emit:\n")
	writeClosedVariantFile(t, root, "provider/schema.yaml", `name: provider
stages:
  waiting: {}
  working: {}
  done: {final: true}
pins:
  inputs: [provider.requested]
  outputs: [provider.replied]
`)
	writeClosedVariantFile(t, root, "provider/entities.yaml", "work: {}\n")
	writeClosedVariantFile(t, root, "provider/nodes.yaml", `provider-node:
  execution_type: system_node
  subscribes_to: [provider.requested, compute.succeeded]
  event_handlers:
    provider.requested:
      advances_to: working
      activity:
        id: compute
        tool: compute
        fork_policy: reuse_recorded_result
        input:
          account_id: payload.account_id
          request_id: payload.request_id
    compute.succeeded:
      advances_to: done
      emit:
        event: provider.replied
        fields:
          account_id: payload.result.account_id
          request_id: payload.result.request_id
          result: payload.result.result
`)
	writeClosedVariantFile(t, root, "provider/tools.yaml", fmt.Sprintf(`compute:
  description: Return the exact original request's recorded result
  handler_type: http
  effect_class: non_idempotent_write
  input_schema:
    type: object
    properties: {account_id: {type: string}, request_id: {type: string}}
    required: [account_id, request_id]
  output_schema:
    type: object
    properties: {account_id: {type: string}, request_id: {type: string}, result: {type: string}}
    required: [account_id, request_id, result]
  response_success: {kind: http_status_2xx}
  http:
    method: POST
    url: %q
    body: {account_id: "{{input.account_id}}", request_id: "{{input.request_id}}"}
`, providerURL))
	return root
}
