package store_test

import (
	"go/ast"

	"go/parser"
	"go/token"
	"go/types"

	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// Controls are outside the policy payload to avoid a self-referential digest.
const debtExcludedFixtureCollectorTo = "261b4a7ce9d04da1c3fbe831f21379f5e6f8f49563bacfe1ff010c184cd02916"
const debtTestProcessCollectorTo = "439bcc9c17492fb3097bdc8ef46f9c44a13890f85ed0e60a3f63a353dc9d9d5f"

func debtExcludedFixtureControlSource(t *testing.T, path, source string) *ast.File {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, source, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	return file
}

func TestPersistenceAuthorityDebtReviewedExcludedExactDelta(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	classified := 0
	for _, path := range []string{"internal/serveapp/issue642_future_provider_test.go", "internal/serveapp/lifecycle_template_race_enabled_test.go", "internal/testutil/testprocess/process_darwin.go", "tests/support/testutil/testprocess/process_darwin.go"} {
		current := path
		if path == "internal/testutil/testprocess/process_darwin.go" {
			current = "tests/support/testutil/testprocess/process_darwin.go"
		}
		data, err := os.ReadFile(filepath.Join(root, current))
		if err != nil {
			t.Fatal(err)
		}
		before := debtExcludedSourceUncertainty(t, path, debtExcludedFixtureControlSource(t, path, string(data)), &types.Info{})
		after := debtExcludedSourceFindings(t, path, debtExcludedFixtureControlSource(t, path, string(data)), &types.Info{})
		if len(before) != len(after) {
			t.Fatal("classification dropped findings")
		}
		for i, finding := range before {
			t.Logf("exact source pin %s %s %s %s", path, finding.Enclosing, finding.Member, finding.Resolved)
			if after[i].Kind != "reviewed-excluded-source" {
				t.Errorf("missing exact classified pin: %+v", after[i])
				continue
			}
			classified++
			raw := finding
			raw.RawSQL = true
			if got := debtClassifyReviewedExcludedFixtures(t, []authorityFinding{raw}); len(got) != 1 || got[0] != raw {
				t.Fatal("reviewed declaration suppressed raw authority")
			}
			after[i].Kind = finding.Kind
			if after[i] != finding {
				t.Fatal("classification changed an identity or its context")
			}
		}
		if got := authorityDebtSites(after); len(got) != len(before) {
			t.Fatal("restored historical findings no longer enter the ratchet")
		}
	}
	if classified != 9 {
		t.Fatalf("classified %d declarations, want three retained and both exact three-declaration Darwin homes", classified)
	}
}

func TestPersistenceAuthorityDebtReviewedExcludedHostileContexts(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	for _, path := range []string{"internal/serveapp/issue642_future_provider_test.go", "internal/serveapp/lifecycle_template_race_enabled_test.go", "internal/testutil/testprocess/process_darwin.go", "tests/support/testutil/testprocess/process_darwin.go"} {
		current := path
		if path == "internal/testutil/testprocess/process_darwin.go" {
			current = "tests/support/testutil/testprocess/process_darwin.go"
		}
		data, err := os.ReadFile(filepath.Join(root, current))
		if err != nil {
			t.Fatal(err)
		}
		for _, mutation := range []string{"exact", "old-body", "body", "helper-body", "binding", "import-alias", "tag", "foreign-path", "foreign-declaration", "extra-declaration", "duplicate", "injected-authority"} {
			t.Run(filepath.Base(path)+"/"+mutation, func(t *testing.T) {
				candidate, source := path, string(data)
				provider := strings.Contains(path, "future_provider")
				darwin := strings.HasSuffix(path, "process_darwin.go")
				want := 1
				if provider {
					want = 2
				}
				if darwin {
					want = 3
				}
				uncertain := 0
				switch mutation {
				case "old-body":
					if provider {
						source = strings.Replace(source, `"idempotency_key": "future-provider-start-cut"`, `"allow_source_freeze": true, "idempotency_key": "future-provider-start-cut"`, 1)
						want--
						uncertain = 1
					}
					if darwin {
						source = strings.Replace(source, "return startWatched(command, owner)", "return command.Start()", 1)
						want--
						uncertain = 1
					}
				case "body", "helper-body":
					if darwin {
						if mutation == "helper-body" {
							source = strings.Replace(source, "return nil }", "return errors.New(\"unreviewed\") }", 1)
						} else {
							source = strings.Replace(source, "info.Proc.P_pid != int32(pid)", "info.Proc.P_pid == int32(pid)", 1)
						}
					} else if provider {
						if mutation == "helper-body" {
							source = strings.Replace(source, "25 * time.Millisecond", "26 * time.Millisecond", 1)
						} else {
							source = strings.Replace(source, "30*time.Second", "31*time.Second", 1)
						}
					} else {
						source = strings.Replace(source, "= true", "= false", 1)
					}
					want--
					uncertain = 1
				case "binding", "import-alias":
					if darwin {
						binding := `"example.invalid/unix"`
						if mutation == "import-alias" {
							binding = `foreign "golang.org/x/sys/unix"`
						}
						source = strings.Replace(source, `"golang.org/x/sys/unix"`, binding, 1)
					} else if provider {
						binding := `"example.invalid/storetest"`
						if mutation == "import-alias" {
							binding = `foreign "github.com/division-sh/swarm/internal/store/storetest"`
						}
						source = strings.Replace(source, `"github.com/division-sh/swarm/internal/store/storetest"`, binding, 1)
					} else {
						source = strings.Replace(source, "const lifecycleTemplateRaceBuild", "import _ \"database/sql\"\nconst lifecycleTemplateRaceBuild", 1)
					}
					uncertain, want = want, 0
				case "tag":
					if darwin {
						source = "//go:build darwin && foreign\n\n" + source
					} else {
						source = strings.Replace(source, "//go:build ", "//go:build foreign && ", 1)
					}
					uncertain, want = want, 0
				case "foreign-path":
					candidate += ".foreign"
					uncertain, want = want, 0
				case "foreign-declaration":
					name := "lifecycleTemplateRaceBuild"
					if provider {
						name = "TestIssue642FutureProviderConstructsAfterEmptyCatalogBothStores"
					}
					if darwin {
						name = "startNative"
					}
					source = strings.Replace(source, name, name+"Foreign", 1)
					want--
					uncertain = 1
				case "extra-declaration", "injected-authority":
					source += "\nfunc unreviewed(db interface { Exec(string) }) { db.Exec(\"DELETE FROM events\") }\n"
					uncertain = 1
				}
				file := debtExcludedFixtureControlSource(t, candidate, source)
				if mutation == "duplicate" {
					for _, decl := range file.Decls {
						if gen, ok := decl.(*ast.GenDecl); ok && gen.Tok == token.IMPORT {
							continue
						}
						file.Decls = append(file.Decls, decl)
						want--
						uncertain = 2
						break
					}
				}
				before := debtExcludedSourceUncertainty(t, candidate, file, &types.Info{})
				findings := debtExcludedSourceFindings(t, candidate, file, &types.Info{})
				if len(findings) != len(before) {
					t.Fatal("hostile source lost independently extracted findings")
				}
				classified, remaining := 0, 0
				for _, finding := range findings {
					switch finding.Kind {
					case "reviewed-excluded-source":
						classified++
					case "unresolved-excluded-source":
						remaining++
					}
				}
				if classified != want || remaining != uncertain {
					t.Fatalf("classified=%d want=%d unresolved=%d want=%d findings=%+v", classified, want, remaining, uncertain, findings)
				}
				for i, finding := range findings {
					if finding.Kind == "reviewed-excluded-source" {
						finding.Kind = "unresolved-excluded-source"
					}
					if finding != before[i] {
						t.Fatal("classification changed another finding or its authority coordinates")
					}
				}
			})
		}
	}
	t.Run("portable-context-body-pairs", func(t *testing.T) {
		const oldContext = "bf98ca60306a0fa727206f73aecb6d27e6a9a366183114a0fe7cd8bbfd62c02e"
		const newContext = "6632b5256bfc7f4f796dee46748546cad0f83930df4a23794fcd179e971e8f37"
		const oldBody = "declaration:931af522b4f0b9379a9842b13e9a28e01e907202547ed55a48a510783d324121"
		const body = "declaration:a93689c088d0eac109b63733b72f50d2c90bacd48405c1eff84a8b6105cf1d5b"
		const helperBody = "declaration:69bf4b7d588cc2455f6ce5c20aea5e8968aba3e962c8d66a45d2190e8e18f9bf"
		const entry = "TestIssue642FutureProviderConstructsAfterEmptyCatalogBothStores"
		const helper = "issue642WaitFutureProviderCompletion"
		for _, test := range []struct {
			declaration, body, context string
			accept                     bool
		}{
			{entry, oldBody, oldContext, true},
			{entry, body, oldContext, true},
			{entry, body, newContext, true},
			{entry, oldBody, newContext, false},
			{entry, body, strings.Repeat("a", 64), false},
			{entry, helperBody, newContext, false},
			{helper, helperBody, oldContext, true},
			{helper, helperBody, newContext, true},
			{helper, body, newContext, false},
			{helper, helperBody, strings.Repeat("a", 64), false},
		} {
			finding := authorityFinding{Kind: "unresolved-excluded-source", File: "internal/serveapp/issue642_future_provider_test.go", Enclosing: test.declaration, Member: test.body, Resolved: "unresolved|source-context:" + test.context}
			classified := debtClassifyReviewedExcludedFixtures(t, []authorityFinding{finding})
			if (classified[0].Kind == "reviewed-excluded-source") != test.accept {
				t.Fatalf("body/context pair accept=%v: %+v", test.accept, classified)
			}
			if got := debtClassifyReviewedExcludedFixtures(t, []authorityFinding{finding, finding}); got[0] != finding || got[1] != finding {
				t.Fatal("duplicate body/context pair admitted")
			}
			finding.RawSQL = true
			if got := debtClassifyReviewedExcludedFixtures(t, []authorityFinding{finding}); got[0] != finding {
				t.Fatal("body/context pair suppressed raw authority")
			}
		}
	})
	row := authorityFinding{Kind: "raw-operation", File: "internal/serveapp/lifecycle_template_race_enabled_test.go", Enclosing: "package", Member: "call:Exec"}
	if got := debtClassifyReviewedExcludedFixtures(t, []authorityFinding{row}); !slices.Equal(got, []authorityFinding{row}) {
		t.Fatal("independent raw authority finding was suppressed")
	}
}

func TestPersistenceAuthorityDebtReviewedExcludedPolicyIsFingerprintBound(t *testing.T) {
	root, copy := persistenceAuthorityRepoRoot(t), t.TempDir()
	for _, path := range []string{
		"internal/store/persistence_authority_debt_census_test.go", "internal/store/persistence_authority_debt_test.go",
		"internal/store/persistence_authority_debt_cache_test.go", "internal/store/persistence_authority_debt_sidecar_test.go",
		"internal/store/persistence_authority_debt_process_lease_test.go", "internal/store/persistence_authority_debt_native_fixture_test.go",
		"internal/store/persistence_authority_debt_excluded_fixture_test.go", "internal/checkoutsource/fixture_role.go",
	} {
		data, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, copy, path, string(data))
	}
	digest, err := debtCollectorDigest(copy)
	if err != nil {
		t.Fatalf("copied reviewed policy=%s %v", digest, err)
	}
	const policy = "internal/store/persistence_authority_debt_excluded_fixture_test.go"
	data, err := os.ReadFile(filepath.Join(copy, policy))
	if err != nil {
		t.Fatal(err)
	}
	debtWriteModuleSource(t, copy, policy, strings.Replace(string(data), "!= 1", "< 1", 1))
	if changed, err := debtCollectorDigest(copy); err != nil || changed == digest {
		t.Fatalf("increased occurrence permission was not fingerprinted: %s %v", changed, err)
	}
	if err := os.Remove(filepath.Join(copy, policy)); err != nil {
		t.Fatal(err)
	}
	if _, err := debtCollectorDigest(copy); err == nil {
		t.Fatal("missing current policy admitted through historical fallback")
	}
	debtWriteModuleSource(t, copy, policy, "package store_test; func broken(")
	if _, err := debtCollectorDigest(copy); err == nil {
		t.Fatal("corrupt current policy admitted")
	}
}
