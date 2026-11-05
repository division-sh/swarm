package releasee2e

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestOperatorChannelRC1ProofUpgradeBothStores(t *testing.T) {
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
			token := filepath.Join(base, "api-token")
			writeReleaseFile(t, token, goldenAPIToken+"\n")
			env := goldenProcessEnv(t, base, store.passwordEnv, 1)
			proofPath := filepath.Join(base, "home", ".swarm", "operator-channel-proofs.json")
			writeReleaseFile(t, proofPath, string(raw))
			if err := os.Chmod(proofPath, 0o600); err != nil {
				t.Fatal(err)
			}
			process := startReleaseServe(t, releaseProcessSpec{BinaryPath: binary, WorkingDir: base, ConfigPath: config, Source: source, Store: backend, TokenFile: token, Token: goldenAPIToken, Env: env})
			ctx, cancel := context.WithTimeout(t.Context(), goldenStartupTimeout)
			defer cancel()
			if err := process.waitReady(ctx); err != nil {
				t.Fatalf("public serve with untouched rc.1 proof: %v", err)
			}
			result := runReleaseCommand(t, goldenStartupTimeout, base, env, "", binary, "--config", config,
				"channel", "list", "--api-server", process.apiBase, "--api-token-file", token, "--json")
			if result.err != nil {
				t.Fatalf("public retained-proof read: %v\n%s", result.err, result.output)
			}
			var listing struct {
				Channels []json.RawMessage `json:"channels"`
			}
			if err := json.Unmarshal([]byte(strings.TrimSpace(result.output)), &listing); err != nil || len(listing.Channels) != 0 {
				t.Fatalf("unrelated historical proof acquired channel admission: channels=%d err=%v", len(listing.Channels), err)
			}
			if err := process.stopAndWait(10 * time.Second); err != nil {
				t.Fatal(err)
			}
			after, err := os.ReadFile(proofPath)
			if err != nil || string(after) != string(raw) {
				t.Fatalf("public load rewrote rc.1 proof: err=%v", err)
			}
		})
	}
}
