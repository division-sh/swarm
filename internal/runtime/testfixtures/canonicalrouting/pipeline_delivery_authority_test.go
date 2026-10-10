package canonicalrouting

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPipelineDeliveryAuthoritySourcesRetainAuthoredBytes(t *testing.T) {
	for _, tc := range []struct {
		name  string
		copy  func(testing.TB) string
		files map[string]string
	}{
		{"authority", CopyPipelineDeliveryAuthority, map[string]string{
			"schema.yaml":   "name: delivery-authority\nstages:\n  queued: {}\n  done: {final: true}\n",
			"entities.yaml": "test_entity: {}\n",
			"events.yaml":   "source.evt:\n",
			"nodes.yaml":    "node-a:\n  execution_type: system_node\n  subscribes_to: [source.evt]\n  event_handlers:\n    source.evt:\n      advances_to: done\n",
		}},
		{"connected_collision", CopyPipelineConnectedDeliveryCollision, map[string]string{
			"schema.yaml":          "name: connected-collision\nconnect:\n  - {event: deploy.done, from: producer, to: receiver, rename: deploy.accepted}\n  - {event: deploy.done, from: producer, to: receiver, rename: deploy.audited}\n",
			"entities.yaml":        "test_entity: {}\n",
			"producer/schema.yaml": "name: producer\npins:\n  outputs: [deploy.done]\n",
			"producer/events.yaml": "deploy.done:\n",
			"receiver/schema.yaml": "name: receiver\npins:\n  inputs: [deploy.accepted, deploy.audited]\n",
			"receiver/nodes.yaml":  "receiver-node:\n  execution_type: system_node\n  subscribes_to: [deploy.accepted, deploy.audited]\n  event_handlers:\n    deploy.accepted: {}\n    deploy.audited: {}\n",
		}},
		{"retry", CopyPipelineDeliveryRetry, map[string]string{
			"schema.yaml":   "name: delivery-retry\nstages:\n  queued: {}\n  done: {final: true}\n",
			"entities.yaml": "test_entity: {}\n",
			"events.yaml":   "source.evt:\nnode.completed:\n",
			"policy.yaml":   "handler_retry_base_seconds: 30\n",
			"nodes.yaml":    "node-a:\n  execution_type: system_node\n  subscribes_to: [source.evt]\n  event_handlers:\n    source.evt:\n      emit: node.completed\n",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := tc.copy(t)
			for name, want := range tc.files {
				actual, err := os.ReadFile(filepath.Join(root, name))
				if err != nil || string(actual) != want {
					t.Fatalf("%s authored bytes changed: %v", name, err)
				}
			}
		})
	}
}
