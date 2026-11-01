package operatorsurface

import (
	"strings"
	"testing"
)

func TestRecordedProviderRenderingDoesNotReprojectOrExposeAssistantHistory(t *testing.T) {
	for _, test := range []struct{ name, request, want string }{
		{"chat", `{"messages":[{"role":"system","content":"author intent"},{"role":"user","name":"speaker","content":"old projection"},{"role":"assistant","content":"prior reply","reasoning_content":"private reasoning"},{"role":"tool","tool_call_id":"call-1","content":"ok"},{"role":"user","name":"speaker","content":"current projection"}],"tools":[{"type":"function"}]}`, `current projection`},
		{"responses", `{"input":[{"type":"function_call","call_id":"call-1","arguments":"{}"},{"type":"function_call_output","call_id":"call-1","output":"exact output"}]}`, `exact output`},
		{"cli", `{"args":["--mcp-config","private configuration"],"provider_input":"[swarm:environment]\nexact stdin"}`, `exact stdin`},
		{"old_cli", `{"message":{"role":"user","content":"old recorded rendering"}}`, `old recorded rendering`},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := projectProviderRendering([]byte(test.request))
			if err != nil || got.Status != "recorded" || got.Source != "recorded_provider_request" || !strings.Contains(string(got.Input), test.want) {
				t.Fatalf("projection=%#v err=%v", got, err)
			}
			for _, private := range []string{"private reasoning", "reasoning_content", "private configuration", "author intent"} {
				if strings.Contains(string(got.Input), private) {
					t.Fatalf("private value exposed: %s", private)
				}
			}
		})
	}
	got, err := projectProviderRendering([]byte(`{}`))
	if err != nil || got.Status != "unresolved" || len(got.Input) != 0 {
		t.Fatal("missing historical evidence was invented")
	}
	if _, err := projectProviderRendering([]byte(`broken`)); err == nil {
		t.Fatal("malformed recorded evidence accepted")
	}
}
