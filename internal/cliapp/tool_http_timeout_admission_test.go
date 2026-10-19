package cliapp

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestVerifyToolHTTPTimeoutDurationParity(t *testing.T) {
	for _, row := range []struct {
		seconds string
		valid   bool
	}{
		{"", true}, {"1", true}, {"10", true}, {"120", true}, {"9223372036", true},
		{"9223372037", false}, {"0", false}, {"null", false}, {"-1", false},
	} {
		t.Run("seconds_"+row.seconds, func(t *testing.T) {
			root := canonicalrouting.CopyExample(t, canonicalrouting.RootIngress)
			field := ""
			if row.seconds != "" {
				field = ", timeout_seconds: " + row.seconds
			}
			writeDescribeTestFile(t, filepath.Join(root, "tools.yaml"), "budget_probe:\n  description: Observe the admitted request budget.\n  handler_type: http\n  http: {method: GET, url: 'https://example.invalid'"+field+"}\n")
			var out, diagnostic bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{"verify", root, "--portable", "--config", writeTestVerifyRuntimeConfig(t), "--json"}, &out, &diagnostic, defaultRootCommandOptions())
			bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(RepoRoot(), root, contracts.DefaultPlatformSpecFile(RepoRoot()))
			if !row.valid {
				if code != CLIExitValidation || err == nil {
					t.Fatalf("verify/load disagree: code=%d load=%v stdout=%s stderr=%s", code, err, out.String(), diagnostic.String())
				}
				if !strings.Contains(out.String()+diagnostic.String(), "tools.yaml") || !strings.Contains(err.Error(), "tools.yaml:4:") {
					t.Fatalf("admission lost source: load=%v stdout=%s stderr=%s", err, out.String(), diagnostic.String())
				}
				return
			}
			if code != 0 || err != nil {
				t.Fatalf("valid timeout refused: code=%d load=%v stdout=%s stderr=%s", code, err, out.String(), diagnostic.String())
			}
			entry, ok := bundle.ToolEntryForFlow(".", "budget_probe")
			if !ok {
				t.Fatal("loaded tool missing")
			}
			execution, ok := entry.HTTPExecution()
			if !ok {
				t.Fatal("HTTP execution missing")
			}
			prepared, err := execution.Prepare(nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			want := 30 * time.Second
			if row.seconds != "" {
				want, err = time.ParseDuration(row.seconds + "s")
				if err != nil {
					t.Fatal(err)
				}
			}
			if prepared.Timeout() != want {
				t.Fatalf("verify-green timeout changed during preparation: got=%s want=%s err=%v", prepared.Timeout(), want, err)
			}
		})
	}
}
