package contracts

import "testing"

func TestToolSchemaEntryRejectsUnadmittedExecutionSemantics(t *testing.T) {
	object := MustToolInputSchema(ToolSchemaObject)
	testCases := []struct {
		name   string
		option ToolSchemaEntryOption
	}{
		{name: "unknown category", option: WithToolCategory("connector-ish")},
		{name: "malformed permission", option: WithToolPermission("bad permission")},
		{name: "invalid rate policy", option: WithToolRateLimit("many/soon", "later")},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewToolSchemaEntry(
				tc.option,
				WithToolHandler(ToolHandlerPlatformBuiltin),
				WithToolSchemas(object, object),
			); err == nil {
				t.Fatal("NewToolSchemaEntry error = nil, want admission rejection")
			}
		})
	}
}
