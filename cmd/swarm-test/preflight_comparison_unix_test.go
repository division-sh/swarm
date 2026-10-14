//go:build aix || darwin || dragonfly || freebsd || hurd || illumos || linux || netbsd || openbsd || solaris

package main

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/testplanning"
)

func TestPreflightComparisonPinsRefsAndRejectsAmbientAuthority(t *testing.T) {
	for _, mode := range []string{"valid", "empty", "missing-base", "foreign-base", "moved-base", "event", "refresh", "publisher", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			repo, base := completionRepo(t)
			completionGit(t, repo, "update-ref", "refs/remotes/origin/master", base)
			t.Chdir(repo)
			t.Setenv("GITHUB_EVENT_PATH", "")
			t.Setenv("SWARM_REFRESH_PERSISTENCE_AUTHORITY_DEBT", "")
			t.Setenv("SWARM_DEBT_CACHE_PUBLISH", "")
			if mode != "empty" {
				if err := os.WriteFile("source_test.go", []byte("package proof\n// exact source change\n"), 0600); err != nil {
					t.Fatal(err)
				}
				completionGit(t, repo, "add", "source_test.go")
			}
			completionGit(t, repo, "commit", "--allow-empty", "--quiet", "-m", "test: frozen comparison")
			head := completionGit(t, repo, "rev-parse", "HEAD")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch mode {
			case "missing-base":
				completionGit(t, repo, "update-ref", "-d", "refs/remotes/origin/master")
			case "foreign-base":
				base = strings.Repeat("f", 40)
			case "moved-base":
				completionGit(t, repo, "update-ref", "refs/remotes/origin/master", head)
			case "event":
				t.Setenv("GITHUB_EVENT_PATH", "foreign-event.json")
			case "refresh":
				t.Setenv("SWARM_REFRESH_PERSISTENCE_AUTHORITY_DEBT", "downward")
			case "publisher":
				t.Setenv("SWARM_DEBT_CACHE_PUBLISH", "1")
			case "cancelled":
				cancel()
			}
			observedBase, comparison, err := observePreflightComparison(ctx, head, base)
			if mode != "valid" && mode != "empty" {
				if err == nil {
					t.Fatal("ambiguous/foreign comparator gained admission")
				}
				return
			}
			if err != nil || observedBase != base || comparison.OriginMasterSHA != base || comparison.DebtRequired != (mode == "valid") || len(comparison.DiffSHA256) != 64 {
				t.Fatalf("frozen comparison: %s %+v %v", observedBase, comparison, err)
			}
			completionGit(t, repo, "update-ref", "refs/remotes/origin/master", head)
			if _, _, err := observePreflightComparison(ctx, head, comparison.OriginMasterSHA); err == nil {
				t.Fatal("late ref movement adopted a different comparison")
			}
		})
	}
}

func TestPreflightDebtUsesNativeSourceAndOneTwentyMinutePhase(t *testing.T) {
	base := strings.Repeat("a", 40)
	for _, mode := range []string{"valid", "foreign", "missing", "duplicate", "other-root", "malformed"} {
		t.Run(mode, func(t *testing.T) {
			source, root := base, "TestPersistenceAuthorityDebtRatchet"
			if mode == "foreign" {
				source = strings.Repeat("b", 40)
			}
			if mode == "other-root" {
				root = "TestInvented"
			}
			raw, err := json.Marshal(map[string]string{"Action": "output", "Package": "github.com/division-sh/swarm/internal/store", "Test": root,
				"Output": "    persistence_authority_debt_test.go:849: debt source=" + source + " collector=" + strings.Repeat("c", 64) + " total-findings=1\n"})
			if err != nil {
				t.Fatal(err)
			}
			input := string(raw) + "\n"
			switch mode {
			case "missing":
				input = ""
			case "duplicate":
				input += input
			case "malformed":
				input = "broken\n"
			}
			if err := validatePreflightDebtSource(strings.NewReader(input), base); (err == nil) != (mode == "valid") {
				t.Fatalf("native comparator proof: %v", err)
			}
		})
	}
	plan := testplanning.PreflightPlan{BaseSHA: base, Comparison: &testplanning.PreflightComparison{DebtRequired: true}, Packages: []testplanning.PreflightPackage{{
		Package: "github.com/division-sh/swarm/internal/store", Run: "^(TestNativeFixtureFamiliesDoNotReceiveRawAuthority|TestPersistenceAuthorityDebtRatchet)$", Required: []testplanning.RequiredTest{
			{TestRoot: testplanning.TestRoot{Package: "github.com/division-sh/swarm/internal/store", Name: "TestNativeFixtureFamiliesDoNotReceiveRawAuthority"}},
			{TestRoot: testplanning.TestRoot{Package: "github.com/division-sh/swarm/internal/store", Name: "TestPersistenceAuthorityDebtRatchet"}},
		},
	}}}
	var phases int
	for _, phase := range preflightPhases(plan) {
		if phase.Package != nil {
			phases++
			if strings.Join(phase.Args, " ") != "go test -json -count=1 -timeout=20m -run "+plan.Packages[0].Run+" "+plan.Packages[0].Package || phase.DebtBase != base {
				t.Fatalf("debt roots split, lost native envelope or comparator: %+v", phase)
			}
		}
	}
	if phases != 1 {
		t.Fatalf("debt package execution count=%d", phases)
	}
}
