package contracts

import (
	"strings"
	"testing"
)

func TestS1CredentialTemplatesResolveExactQualifiedStoreKeys(t *testing.T) {
	for _, key := range []string{"team-bot", "team_bot"} {
		t.Run(key, func(t *testing.T) {
			plan, err := AdmitToolHTTPExecution(HTTPToolSpec{
				Method: "POST", URL: "https://provider.example/{{credentials." + key + ".bot_token}}/{{input.customer.id}}",
				Headers: map[string]string{"X-Token": "{{credentials." + key + ".bot_token}}"},
				Body:    map[string]any{"token": "{{credentials." + key + ".bot_token}}", "text": "literal credentials.bot_token", "id": "{{input.customer.id}}"},
			})
			if err != nil {
				t.Fatal(err)
			}
			request, err := plan.Prepare(map[string]any{"customer": map[string]any{"id": "17"}}, map[string]any{key + ".bot_token": "fixture/token"})
			if err != nil || request.URL() != "https://provider.example/fixture%2Ftoken/17" || request.Headers().Get("X-Token") != "fixture/token" || !strings.Contains(string(request.Body()), `"token":"fixture/token"`) || !strings.Contains(string(request.Body()), "literal credentials.bot_token") {
				t.Fatalf("qualified flat key did not use canonical template resolution: request=%+v err=%v", request, err)
			}
			for _, credentials := range []map[string]any{
				{"bot_token": "unqualified"},
				{"foreign.bot_token": "foreign"},
				{key: map[string]any{"bot_token": "nested-not-a-store-key"}},
			} {
				if request, err := plan.Prepare(map[string]any{"customer": map[string]any{"id": "17"}}, credentials); err == nil {
					t.Errorf("missing exact key gained request authority: %+v", request)
				}
			}
		})
	}
	plan, err := AdmitToolHTTPExecution(HTTPToolSpec{Method: "POST", URL: "https://provider.example", Headers: map[string]string{"X-Token": "{{credentials.token}}"}})
	if err != nil {
		t.Fatal(err)
	}
	if request, err := plan.Prepare(nil, map[string]any{"token": "ordinary"}); err != nil || request.Headers().Get("X-Token") != "ordinary" {
		t.Fatalf("ordinary exact key changed: %+v err=%v", request, err)
	}
}
