package canonicalrouting

import (
	"os"
	"path/filepath"
	"testing"
)

// CopyPublicationConnector uses the real imported Telegram contract in the
// existing four-arm local/connected activity topology, not a schema restatement.
func CopyPublicationConnector(t testing.TB, mode string) string {
	t.Helper()
	root := CopyPublicationActivity(t, mode, "http://127.0.0.1", true)
	path := root
	if mode == "nested_template" {
		path = filepath.Join(root, "outer", "source")
	} else if mode != "root" {
		path = filepath.Join(root, "source")
	}
	if err := os.Remove(filepath.Join(path, "tools.yaml")); err != nil {
		t.Fatal(err)
	}
	applyClosedReplacement(t, filepath.Join(path, "schema.yaml"), "name: publication-activity\n", "name: publication-activity\nimports:\n  connector_packs:\n    - {provider: telegram, tool: telegram.send_message}\n")
	applyClosedReplacement(t, filepath.Join(path, "nodes.yaml"), "tool: send\n", "tool: telegram.send_message\n")
	applyClosedReplacement(t, filepath.Join(path, "nodes.yaml"), "input: {message: {ref: payload.message}}", "input: {chat_id: {literal: 42}, text: {ref: payload.message}}")
	for _, nodes := range []string{filepath.Join(path, "nodes.yaml"), filepath.Join(root, "sink", "nodes.yaml")} {
		applyClosedReplacement(t, nodes, "payload.result.delivered == true", "payload.result.message_id > 0")
	}
	return root
}
