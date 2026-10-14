package cliapp

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSingleDeploymentConfigPublicHelp(t *testing.T) {
	for _, args := range [][]string{{"--help"}, {"serve", "--help"}, {"run", "start", "--help"}} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			var out, errOut bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), args, &out, &errOut, defaultRootCommandOptions())
			if code != 0 {
				t.Fatalf("help failed: code=%d stderr=%s", code, errOut.String())
			}
			for _, want := range []string{"Deployment config file", "whole file", ".swarm/swarm.yaml", "--config wins over SWARM_CONFIG"} {
				if !strings.Contains(out.String(), want) {
					t.Errorf("help missing %q:\n%s", want, out.String())
				}
			}
		})
	}
}

func TestSingleDeploymentConfigPublicSetupDocs(t *testing.T) {
	for _, name := range []string{"README.md", "CONTRIBUTING.md", ".env.example"} {
		t.Run(name, func(t *testing.T) {
			body, err := os.ReadFile(filepath.Join(RepoRoot(), name))
			if err != nil {
				t.Fatal(err)
			}
			for _, want := range []string{"one file", ".swarm/swarm.yaml", "--config", "SWARM_CONFIG"} {
				if !strings.Contains(string(body), want) {
					t.Errorf("setup guidance missing %q", want)
				}
			}
		})
	}
}
