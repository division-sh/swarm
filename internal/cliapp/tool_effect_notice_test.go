package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/authoringview"
)

func TestDescribeAgentToolEffectNoticeTextAndJSON(t *testing.T) {
	root := t.TempDir()
	writeDescribeTestFile(t, filepath.Join(root, "schema.yaml"), "name: notice\n")
	writeDescribeTestFile(t, filepath.Join(root, "tools.yaml"), "probe:\n  handler_type: http\n  effect_class: read_only\n  http: {method: GET, url: 'https://example.invalid'}\n")
	writeDescribeTestFile(t, filepath.Join(root, "agents.yaml"), "first:\n  intent: {inline: Observe one result.}\n  model: regular\n  tools: [probe]\nsecond:\n  intent: {inline: Observe another result.}\n  model: regular\n  tools: [probe]\n")
	const wording = "not applied to agent calls; an interrupted call is outcome-uncertain and not redispatched"
	for _, jsonOutput := range []bool{false, true} {
		args := []string{"describe", root}
		if jsonOutput {
			args = append(args, "--json")
		}
		var out, diagnostic bytes.Buffer
		code := executeRootCommandWithOptions(context.Background(), RepoRoot(), args, &out, &diagnostic, defaultRootCommandOptions())
		if code != 0 || strings.Count(out.String(), wording) != 1 {
			t.Fatalf("describe notice is not exactly once: json=%t code=%d stdout=%s stderr=%s", jsonOutput, code, out.String(), diagnostic.String())
		}
		if jsonOutput {
			var view authoringview.View
			if err := json.Unmarshal(out.Bytes(), &view); err != nil {
				t.Fatal(err)
			}
			count := 0
			for _, finding := range view.Diagnostics {
				if finding.CheckID == "agent_tool_effect_class_not_applied" {
					count++
					if finding.AuthoredLocation != "tools.yaml" || finding.Severity != "info" {
						t.Fatalf("notice lost its exact authored scope: %+v", finding)
					}
				}
			}
			if count != 1 {
				t.Fatalf("JSON notices=%d", count)
			}
		}
	}
}
