package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestVerifyMultipleEventOwnersTeachingError(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	root := t.TempDir()
	for name, body := range map[string]string{
		"schema.yaml": "name: owner-diagnostic\nstages: {waiting: {}, done: {final: true}}\n",
		"events.yaml": "work.requested:\n",
		"nodes.yaml":  "alpha:\n  work.requested:\n    advances_to: done\nbeta:\n  work.requested:\n    advances_to: done\n",
	} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	config := writeTestVerifyRuntimeConfig(t)
	for _, mode := range []string{"text", "json", "quiet"} {
		t.Run(mode, func(t *testing.T) {
			args := []string{"verify", root, "--config", config}
			if mode != "text" {
				args = append(args, "--"+mode)
			}
			var out, diagnostic bytes.Buffer
			if code := executeRootCommand(context.Background(), RepoRoot(), args, &out, &diagnostic); code != CLIExitValidation {
				t.Fatalf("exit=%d stdout=%s stderr=%s", code, &out, &diagnostic)
			}
			message := out.String()
			if mode == "json" {
				var result verifyCommandResult
				if err := json.Unmarshal(out.Bytes(), &result); err != nil {
					t.Fatal(err)
				}
				if result.OK || result.AdmissionComplete || result.LiveReadiness != "not_evaluated" {
					t.Fatalf("invalid ownership fabricated admission: %+v", result)
				}
				message = ""
				for _, observation := range result.Observations {
					message += observation.Reason + "\n"
				}
			}
			for _, want := range []string{
				"multiple authoritative system node owners",
				`node "alpha" (scope "."): handler "work.requested" at nodes.yaml:4:7`,
				`node "beta" (scope "."): handler "work.requested" at nodes.yaml:8:7`,
				"one router node", "ordered rules:",
			} {
				if !strings.Contains(message, want) {
					t.Errorf("public %s omitted %q: stdout=%s stderr=%s", mode, want, &out, &diagnostic)
				}
			}
		})
	}
}
