package contracts

import (
	"strings"
	"testing"
)

func TestSystemNodeContractRefusesRetiredFieldsOnPresence(t *testing.T) {
	for _, field := range []string{"execution_type", "subscribes_to", "produces", "event_handlers"} {
		for _, value := range []string{"null", "{}", "[]", "system_node", "[work.completed]"} {
			t.Run(field+"/"+value, func(t *testing.T) {
				var node SystemNodeContract
				err := decodeNodeTestYAML([]byte(field+": "+value+"\nwork.requested: {}\n"), &node)
				if err == nil || !strings.Contains(err.Error(), field) || !strings.Contains(err.Error(), "directly under the node") {
					t.Fatalf("retired node field accepted or missing teaching: %v", err)
				}
			})
		}
	}
}
