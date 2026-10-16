package packfixture

import "fmt"

// ScopedActivityMembers contains real authored declarations with deliberately
// ambiguous global tool aliases. It grants no execution or provider authority.
func ScopedActivityMembers() map[string][]byte {
	members := map[string][]byte{}
	for _, flow := range []string{".", "child", "sibling", "inherited"} {
		prefix := ""
		if flow != "." {
			prefix = flow + "/"
		}
		members[prefix+"schema.yaml"] = []byte("stages: []\n")
		members[prefix+"events.yaml"] = []byte("send.requested:\n")
		members[prefix+"nodes.yaml"] = []byte(`sender:
  execution_type: system_node
  subscribes_to: [send.requested]
  event_handlers:
    send.requested:
      activity: {id: send, tool: shared, input: {}}
`)
		if flow == "inherited" {
			continue
		}
		origin := flow
		if flow == "." {
			origin = "root"
		}
		members[prefix+"tools.yaml"] = []byte(fmt.Sprintf(`shared:
  description: Read the %s scoped fixture endpoint.
  handler_type: http
  effect_class: read_only
  input_schema: {type: object, additionalProperties: false}
  output_schema:
    type: object
    additionalProperties: false
    required: [origin]
    properties:
      origin: {type: string, enum: [%s]}
  http: {method: GET, url: 'https://%s.example.test/read'}
`, origin, origin, origin))
	}
	return members
}
