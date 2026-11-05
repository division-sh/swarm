package releasee2e

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDescribePackPublicReadOnlyJourney(t *testing.T) {
	root := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, root)
	env := goldenProcessEnv(t, root, "", 0)
	command := func(args ...string) string {
		t.Helper()
		result := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary, args...)
		if result.err != nil {
			t.Fatalf("public command %v: %v\n%s", args, result.err, result.output)
		}
		return result.output
	}
	listing := command("describe", "pack", "--json")
	if !strings.Contains(listing, `"id":"telegram"`) {
		t.Fatalf("public listing lost selected provider: %s", listing)
	}
	var description struct {
		ID         string `json:"id"`
		Components []struct {
			Source string `json:"source"`
			Tools  []struct {
				ID string `json:"id"`
			} `json:"tools"`
		} `json:"components"`
		Project struct {
			Evaluated bool `json:"evaluated"`
		} `json:"project"`
		Fragments []struct {
			File    string `json:"file"`
			Purpose string `json:"purpose"`
			YAML    string `json:"yaml"`
		} `json:"fragments"`
	}
	raw := command("describe", "pack", "telegram", "--json")
	if err := json.Unmarshal([]byte(raw), &description); err != nil || description.ID != "telegram" || description.Project.Evaluated {
		t.Fatalf("public outside-deployment description: %+v err=%v", description, err)
	}
	if len(description.Components) != 1 || description.Components[0].Source != "embedded" {
		t.Fatal("public installed provenance missing")
	}
	detail := command("describe", "pack", "telegram", "--tool", "telegram.send_message")
	if !strings.Contains(detail, "chat_id") || !strings.Contains(detail, "EXECUTION") {
		t.Fatalf("public exact tool detail missing: %s", detail)
	}
	source := filepath.Join(root, "consumer")
	writeReleaseFile(t, filepath.Join(source, "manifest.yaml"), "version: 1.0.0\nplatform_version: '*'\n")
	command("import", "telegram", source)
	for _, fragment := range description.Fragments {
		if fragment.File == "provider setup" || strings.HasSuffix(fragment.File, "/nodes.yaml") && fragment.Purpose != "tool example: telegram.send_message" {
			continue
		}
		body := strings.ReplaceAll(fragment.YAML, "REPLACE_CHAT_ID", "12345")
		body = strings.ReplaceAll(body, "REPLACE_TEXT", "synthetic reply")
		body = strings.ReplaceAll(body, "REPLACE_OPERATOR_ACCOUNT", "420079")
		writeReleaseFile(t, filepath.Join(source, fragment.File), body)
	}
	verified := command("verify", source, "--portable", "--json")
	if !strings.Contains(verified, `"ok":true`) {
		t.Fatalf("public concrete snippet verification failed: %s", verified)
	}
	if _, err := os.Stat(filepath.Join(root, ".swarm")); !os.IsNotExist(err) {
		t.Fatalf("read-only command unexpectedly created runtime state: %v", err)
	}
}
