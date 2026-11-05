package releasee2e

import (
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestOperatorChannelRC1ProofServeRefusalBothStores(t *testing.T) {
	root := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, root)
	raw, err := os.ReadFile(filepath.Join(releaseE2ERepoRoot(t), "internal", "operatorchannel", "testdata", "rc1-credential-proof-v2.json"))
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprintf("%x", sha256.Sum256(raw)); got != "c48eec9b6c441ea55608bde1f4d109bfe81b7ce4cb8c0fecd56e8d67054a5747" {
		t.Fatalf("rc.1 artifact changed: %s", got)
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			base := filepath.Join(root, backend)
			store := goldenSQLiteStore(base)
			if backend == "postgres" {
				store = goldenPostgresStore(t, os.Getenv(goldenPostgresEnv))
			}
			source := filepath.Join(base, "source")
			copyReleaseTree(t, filepath.Join(releaseE2ERepoRoot(t), "tests", "tier8-boot-verification", "test-boot-success"), source)
			config := filepath.Join(base, "swarm.yaml")
			writeReleaseFile(t, config, goldenRuntimeConfig(store))
			env := goldenProcessEnv(t, base, store.passwordEnv, 1)
			proofPath := filepath.Join(base, "home", ".swarm", "operator-channel-proofs.json")
			writeReleaseFile(t, proofPath, string(raw))
			if err := os.Chmod(proofPath, 0o600); err != nil {
				t.Fatal(err)
			}
			result := runReleaseCommand(t, goldenStartupTimeout, base, env, "", binary,
				"--config", config, "serve", source, "--no-color")
			exitErr, ok := result.err.(*exec.ExitError)
			if !ok || exitErr.ExitCode() == 0 {
				t.Fatalf("serve must refuse before READY with a nonzero exit: %v\n%s", result.err, result.output)
			}
			for _, want := range []string{`json: unknown field "external_account_reference"`, "If this is an rc.1 proof store, remove it and re-pair the channel before starting v0.7."} {
				if !strings.Contains(result.output, want) {
					t.Errorf("public refusal lacks %q:\n%s", want, result.output)
				}
			}
			if strings.Contains(result.output, "READY") {
				t.Fatalf("refused proof store reached READY:\n%s", result.output)
			}
			after, err := os.ReadFile(proofPath)
			if err != nil || string(after) != string(raw) {
				t.Fatalf("public refusal rewrote rc.1 proof: %v", err)
			}
		})
	}
}
