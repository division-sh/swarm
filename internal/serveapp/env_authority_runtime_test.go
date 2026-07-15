package serveapp

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestForkHarnessIgnoresMalformedRepoDotEnv(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := writeEnvAuthorityRepoWithMalformedDotEnv(t)

	var stdout bytes.Buffer
	code := runForkRuntimeOwnerHarness(context.Background(), repo, []string{"--dry-run"}, &stdout)
	if code == 0 {
		t.Fatalf("fork unexpectedly succeeded; expected non-env runtime/store failure to keep proof meaningful")
	}
	assertNoDotEnvLoadFailure(t, stdout.String())
}

func writeEnvAuthorityRepoWithMalformedDotEnv(t *testing.T) string {
	t.Helper()
	repo := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(repo, "go.mod"), "module dotenvignored\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(repo, ".env"), "SWARM_API_TOKEN=repo-token\nBROKEN\n")
	return repo
}

func assertNoDotEnvLoadFailure(t testing.TB, output string) {
	t.Helper()
	for _, forbidden := range []string{"load .env", "expected KEY=VALUE"} {
		if strings.Contains(output, forbidden) {
			t.Fatalf("output still shows repo .env parsing failure %q:\n%s", forbidden, output)
		}
	}
}
