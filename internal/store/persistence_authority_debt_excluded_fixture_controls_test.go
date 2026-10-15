package store_test

import (
	"bytes"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"go/types"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// Controls are outside the policy payload to avoid a self-referential digest.
const debtExcludedFixtureCollectorTo = "261b4a7ce9d04da1c3fbe831f21379f5e6f8f49563bacfe1ff010c184cd02916"

// Same seven collector payloads as backed-up 823510bfd3; retained in ancestry.
// Reviewer-a #642/6202927132 preserves both exact policy digests.
const debtExcludedFixturePredecessor = "c3c3b3fa4671966ba6bf8c7f3baecbfebb746dbd"

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
		for _, mutation := range []string{"exact", "body", "helper-body", "binding", "import-alias", "tag", "foreign-path", "foreign-declaration", "extra-declaration", "duplicate", "injected-authority"} {
			t.Run(filepath.Base(path)+"/"+mutation, func(t *testing.T) {
				candidate, source := path, string(data)
				provider := strings.Contains(path, "future_provider")
				want := 1
				if provider {
					want = 2
				}
				uncertain := 0
				switch mutation {
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
	if digest, err := debtCollectorDigest(copy); err != nil || digest != debtExcludedFixtureCollectorTo {
		t.Fatalf("copied reviewed policy=%s %v", digest, err)
	}
	const policy = "internal/store/persistence_authority_debt_excluded_fixture_test.go"
	data, err := os.ReadFile(filepath.Join(copy, policy))
	if err != nil {
		t.Fatal(err)
	}
	debtWriteModuleSource(t, copy, policy, strings.Replace(string(data), "!= 1", "< 1", 1))
	if digest, err := debtCollectorDigest(copy); err != nil || digest == debtExcludedFixtureCollectorTo {
		t.Fatalf("increased occurrence permission was not fingerprinted: %s %v", digest, err)
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

func TestPersistenceAuthorityDebtReviewedExcludedCollectorTransition(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	predecessor := t.TempDir()
	paths := []string{
		"internal/store/persistence_authority_debt_census_test.go", "internal/store/persistence_authority_debt_test.go",
		"internal/store/persistence_authority_debt_cache_test.go", "internal/store/persistence_authority_debt_sidecar_test.go",
		"internal/store/persistence_authority_debt_process_lease_test.go", "internal/store/persistence_authority_debt_native_fixture_test.go",
		"internal/checkoutsource/fixture_role.go",
	}
	for _, path := range paths {
		data, err := debtGit(root, "show", debtExcludedFixturePredecessor+":"+path)
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, predecessor, path, string(data))
	}
	if digest, err := debtCollectorDigest(predecessor); err != nil || digest != debtNativeFixtureCollectorTo {
		t.Fatalf("immutable predecessor collector=%s error=%v", digest, err)
	}
	if digest, err := debtCollectorDigest(root); err != nil || digest != debtExcludedFixtureCollectorTo {
		t.Fatalf("exact successor collector=%s error=%v", digest, err)
	}
	// The uncertainty extraction itself is unchanged, not replaced by a trusting scanner.
	canonical := func(path, name string) []byte {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		file := debtExcludedFixtureControlSource(t, path, string(data))
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == name {
				fn.Name.Name = "uncertainty"
				var out bytes.Buffer
				if err := format.Node(&out, token.NewFileSet(), fn); err != nil {
					t.Fatal(err)
				}
				return out.Bytes()
			}
		}
		t.Fatal("missing uncertainty owner")
		return nil
	}
	const census = "internal/store/persistence_authority_debt_census_test.go"
	if !bytes.Equal(canonical(filepath.Join(predecessor, census), "debtExcludedSourceFindings"), canonical(filepath.Join(root, census), "debtExcludedSourceUncertainty")) {
		t.Fatal("historical uncertainty scanner changed")
	}
	for _, mutation := range []string{"exact", "landed", "foreign-base", "foreign-head", "unrotated", "reverse", "rebootstrap", "addition", "removal", "increase", "decrease"} {
		t.Run(mutation, func(t *testing.T) {
			row := debtControlSite("call:QueryRow", 2)
			trusted := authorityDebtBaseline{BootstrapSource: debtBootstrapSource, Collector: debtNativeFixtureCollectorTo, Sites: debtControlSet(row)}
			head := trusted
			head.Collector, head.Sites = debtExcludedFixtureCollectorTo, maps.Clone(trusted.Sites)
			current := head.Collector
			switch mutation {
			case "landed":
				trusted.Collector = current
			case "foreign-base":
				trusted.Collector = strings.Repeat("a", 64)
			case "foreign-head":
				head.Collector, current = strings.Repeat("b", 64), strings.Repeat("b", 64)
			case "unrotated":
				head.Collector = trusted.Collector
			case "reverse":
				trusted, head, current = head, trusted, trusted.Collector
			case "rebootstrap":
				head.BootstrapSource = strings.Repeat("c", 40)
			case "addition":
				added := debtControlSite("call:Exec", 1)
				head.Sites[added.identity()] = added
			case "removal":
				delete(head.Sites, row.identity())
			case "increase", "decrease":
				delete(head.Sites, row.identity())
				if mutation == "increase" {
					row.Multiplicity++
				} else {
					row.Multiplicity--
				}
				head.Sites[row.identity()] = row
			}
			if err := debtValidateCollectorIdentity(trusted.Collector, current, trusted, head, ""); (err == nil) != (mutation == "exact" || mutation == "landed") {
				t.Fatalf("transition error=%v", err)
			}
		})
	}
}
