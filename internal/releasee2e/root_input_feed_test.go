package releasee2e

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestRootInputFeedLocalAndConnectedCLIExecuteBothStores(t *testing.T) {
	base := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, base)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			root := filepath.Join(base, backend)
			config, env := clockReleaseConfig(t, root, backend)
			source := filepath.Join(root, "contracts")
			copyReleaseTree(t, filepath.Join(releaseE2ERepoRoot(t), "internal/serveapp/testdata/verify-run-jobflow-registry"), source)
			writeReleaseFile(t, filepath.Join(root, "api-token"), goldenAPIToken+"\n")
			rawConfig, err := os.ReadFile(config)
			if err != nil {
				t.Fatal(err)
			}
			writeReleaseFile(t, config, string(rawConfig)+fmt.Sprintf("serve:\n  api_token_file: %q\nconnection:\n  api_token_file: %q\n", filepath.Join(root, "api-token"), filepath.Join(root, "api-token")))
			writeReleaseFile(t, filepath.Join(root, "rows.jsonl"), "{\"slug\":\"one\",\"name\":\"One\",\"eng_roles\":8}\n{\"slug\":\"two\",\"name\":\"Two\",\"eng_roles\":2}\n")
			assertGoldenProcessHasNoExternalExecutables(t, env)
			verify := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary, "verify", source, "--config", config, "--portable", "--json")
			assertFullLifecycleVerifySuccess(t, verify)
			localID := uuid.NewString()
			local := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary,
				"run", "start", source, "--config", config, "--data", "company.registered=rows.jsonl", "--run-id", localID, "--mcp-port", fmt.Sprint(freeReleaseTCPPort(t)))
			if local.err != nil || !strings.Contains(local.output, "status=completed") {
				t.Fatalf("input-only local feed run: %v\n%s", local.err, local.output)
			}
			process := startReleaseServe(t, releaseProcessSpec{BinaryPath: binary, WorkingDir: root, Source: source,
				ConfigPath: config, Store: backend, TokenFile: "api-token", Token: goldenAPIToken, Env: env})
			ctx, cancel := context.WithTimeout(t.Context(), goldenStartupTimeout)
			defer cancel()
			if err := process.waitReady(ctx); err != nil {
				t.Fatalf("input-only feed serve: %v\n%s", err, process.output.String())
			}
			for _, selector := range []string{"--data", "--pin"} {
				id := uuid.NewString()
				value := "company.registered=rows.jsonl"
				if selector == "--pin" {
					value = "company.registered@head"
				}
				connected := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary,
					"run", "start", "--connect", process.apiBase, selector, value, "--run-id", id, "--no-follow")
				if connected.err != nil {
					t.Fatalf("connected input-only feed %s: %v\n%s", selector, connected.err, connected.output)
				}
				waitForFullLifecycleRunStatus(t, process.rpc, id, "completed")
				assertRootInputFeedBinaryRows(t, process, id, binary, root, config, env)
			}
			waitForFullLifecycleRunStatus(t, process.rpc, localID, "completed")
			assertRootInputFeedBinaryRows(t, process, localID, binary, root, config, env)
			if err := process.stopAndWait(goldenShutdownGrace); err != nil {
				t.Fatalf("input-only feed shutdown: %v\n%s", err, process.output.String())
			}
		})
	}
}

func assertRootInputFeedBinaryRows(t *testing.T, process *releaseServeProcess, runID, binary, root, config string, env []string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), goldenStartupTimeout)
	defer cancel()
	rows, err := listGoldenEvents(ctx, process.rpc, runID)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, row := range rows {
		if row.EventName != "company.registered" {
			continue
		}
		slug, ok := row.Payload["slug"].(string)
		if !ok || (slug != "one" && slug != "two") || seen[slug] || row.Source != "deployment-feed" || row.ProducerType != "external" || len(row.DeadLetters) != 0 || len(row.Deliveries) != 1 {
			t.Fatalf("binary input-only feed changed business rows or declaration producer: %+v", row)
		}
		seen[slug] = true
		delivery := row.Deliveries[0]
		if delivery.Target.FlowID != "registry" || delivery.Target.FlowInstance == "" || delivery.Target.EntityID == "" || delivery.Status != "delivered" || !delivery.Terminal {
			t.Fatalf("binary input-only feed lost execution/settlement: %+v", delivery)
		}
		assertFullEventPayloadReadback(t, process, row, binary, root, config, "api-token", env)
	}
	if len(seen) != 2 {
		t.Fatalf("binary feed published %d exact rows, want 2", len(seen))
	}
}
