package cliapp

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestDescribeRefusesRetiredToolEffectClassTextAndJSON(t *testing.T) {
	root := t.TempDir()
	writeDescribeTestFile(t, filepath.Join(root, "schema.yaml"), "name: notice\n")
	writeDescribeTestFile(t, filepath.Join(root, "tools.yaml"), "probe:\n  handler_type: http\n  effect_class: read_only\n  http: {method: GET, url: 'https://example.invalid'}\n")
	writeDescribeTestFile(t, filepath.Join(root, "agents.yaml"), "first:\n  intent: {inline: Observe one result.}\n  model: regular\n  tools: [probe]\nsecond:\n  intent: {inline: Observe another result.}\n  model: regular\n  tools: [probe]\n")
	const wording = "effect_class is not a tool key; the runtime attempts each HTTP call once, and an interrupted call is outcome-uncertain"
	for _, jsonOutput := range []bool{false, true} {
		args := []string{"describe", root}
		if jsonOutput {
			args = append(args, "--json")
		}
		var out, diagnostic bytes.Buffer
		code := executeRootCommandWithOptions(context.Background(), RepoRoot(), args, &out, &diagnostic, defaultRootCommandOptions())
		message := out.String() + diagnostic.String()
		if code == 0 || !strings.Contains(message, wording) || !strings.Contains(message, "tools.yaml:3:") {
			t.Fatalf("retired tool declaration did not refuse with teaching evidence: json=%t code=%d stdout=%s stderr=%s", jsonOutput, code, out.String(), diagnostic.String())
		}
	}
}
