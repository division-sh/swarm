package releasee2e

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCommandScaffoldDevStartup(t *testing.T) {
	root := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, root)
	home := filepath.Join(root, "home")
	env := []string{
		"HOME=" + home,
		"XDG_CONFIG_HOME=" + filepath.Join(home, ".config"),
		"XDG_CACHE_HOME=" + filepath.Join(home, ".cache"),
		"XDG_DATA_HOME=" + filepath.Join(home, ".local", "share"),
		"XDG_STATE_HOME=" + filepath.Join(home, ".local", "state"),
		"PATH=" + root,
		"NO_COLOR=1",
	}
	scaffold := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary, "new", "zero-agent-automation")
	if scaffold.err != nil {
		t.Fatalf("scaffold: %v\n%s", scaffold.err, scaffold.output)
	}
	project := filepath.Join(root, "zero-agent-automation")
	for _, args := range [][]string{{"verify", "--portable"}, {"test", ".", "tests/smoke.yaml"}} {
		result := runReleaseCommand(t, goldenStartupTimeout, project, env, "", binary, args...)
		if result.err != nil {
			t.Fatalf("%v: %v\n%s", args, result.err, result.output)
		}
	}
	// This fixture proves credential admission and startup, not bot-token validity or delivery.
	const syntheticToken = "scaffold-startup-not-a-real-telegram-token"
	credential := runReleaseCommand(t, goldenStartupTimeout, project, env, syntheticToken+"\n", binary,
		"secrets", "set", "--stdin", "--", "telegram.telegram_bot_token")
	if credential.err != nil || strings.Contains(credential.output, syntheticToken) {
		t.Fatalf("synthetic credential setup failed or exposed its value: %v", credential.err)
	}
	apiToken, err := os.ReadFile(filepath.Join(project, ".swarm", "api-token"))
	if err != nil {
		t.Fatal(err)
	}
	process := startReleaseServe(t, releaseProcessSpec{
		BinaryPath: binary, WorkingDir: project, Env: env,
		Dev: true, DefaultExecutionSelection: true, Token: strings.TrimSpace(string(apiToken)),
	})
	ctx, cancel := context.WithTimeout(t.Context(), goldenStartupTimeout)
	defer cancel()
	if err := process.waitReady(ctx); err != nil {
		t.Fatal(err)
	}
	requireLifecycleHealthPosture(t, process.rpc, "live")
	if err := process.stopAndWait(10 * time.Second); err != nil {
		t.Fatalf("join dev serve: %v\n%s", err, process.output.String())
	}
	if _, err := os.Stat(filepath.Join(project, ".swarm", "stores", "dev-scratch.db")); err != nil {
		t.Fatalf("dev serve did not use its scratch store: %v", err)
	}
	wantNext := "Next:\n  cd zero-agent-automation\n  swarm verify --portable\n  swarm test tests/smoke.yaml\n  swarm secrets set -- 'telegram.telegram_bot_token'\n  swarm serve --dev\n"
	if !strings.HasSuffix(scaffold.output, wantNext) {
		t.Fatalf("scaffold next steps differ:\n%s", scaffold.output)
	}
}
