package canonicalrouting

import "testing"

// CopyReceiverConfigHistory declares the recorded business values used by the
// fixed-revision config proof, including names that overlap runtime controls.
func CopyReceiverConfigHistory(t testing.TB) string {
	t.Helper()
	root := CopyTemplateInstanceRoute(t, TemplateInstanceRouteOptions{Consumer: TemplateInstanceAgentConsumer})
	writeClosedVariantFile(t, root, "consumer/schema.yaml", `name: consumer
mode: template
instance: vertical_id
instance_variables:
  variables:
    nested: json
    status: boolean
    flow_path: json
pins:
  inputs:
    events:
      - event: deploy.done
        resolution:
          mode: select
`)
	return root
}
