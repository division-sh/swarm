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
	for _, path := range []string{"internal/serveapp/issue642_future_provider_test.go", "internal/serveapp/lifecycle_template_race_enabled_test.go"} {
		data, err := os.ReadFile(filepath.Join(root, path))
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
			after[i].Kind = finding.Kind
			if after[i] != finding {
				t.Fatal("classification changed an identity or its context")
			}
		}
		if got := authorityDebtSites(after); len(got) != len(before) {
			t.Fatal("restored historical findings no longer enter the ratchet")
		}
	}
	if classified != 3 {
		t.Fatalf("classified %d declarations, want exactly three", classified)
	}
}

func TestPersistenceAuthorityDebtReviewedExcludedHostileContexts(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	for _, path := range []string{"internal/serveapp/issue642_future_provider_test.go", "internal/serveapp/lifecycle_template_race_enabled_test.go"} {
		data, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		for _, mutation := range []string{"exact", "old-body", "body", "helper-body", "binding", "import-alias", "tag", "foreign-path", "foreign-declaration", "extra-declaration", "duplicate", "injected-authority"} {
			t.Run(filepath.Base(path)+"/"+mutation, func(t *testing.T) {
				candidate, source := path, string(data)
				provider := strings.Contains(path, "future_provider")
				want := 1
				if provider {
					want = 2
				}
				uncertain := 0
				switch mutation {
				case "old-body":
					if provider {
						source = strings.Replace(source, `"idempotency_key": "future-provider-start-cut"`, `"allow_source_freeze": true, "idempotency_key": "future-provider-start-cut"`, 1)
					}
				case "body", "helper-body":
					if provider {
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
					if provider {
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
					source = strings.Replace(source, "//go:build ", "//go:build foreign && ", 1)
					uncertain, want = want, 0
				case "foreign-path":
					candidate += ".foreign"
					uncertain, want = want, 0
				case "foreign-declaration":
					name := "lifecycleTemplateRaceBuild"
					if provider {
						name = "TestIssue642FutureProviderConstructsAfterEmptyCatalogBothStores"
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
