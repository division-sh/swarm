package store_test

import (
	"bytes"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Excluded from the collector payload to avoid a self-referential fingerprint.
const debtProcessLeaseCollectorFrom = "1a353f6c8e5cfe2e2fce2163231e4ac4832df0776daa8dbea177c6edb274bd90"
const debtProcessLeaseCollectorTo = "6847bd84b8ef61a1b21aa8d063e0a8abe77dff964642060c7bb4d3b819d3ff74"

func TestPersistenceAuthorityDebtProcessLeaseCustodySuccessorPreservesOnlyPredecessorEvidence(t *testing.T) {
	root, predecessor := persistenceAuthorityRepoRoot(t), t.TempDir()
	const source = "98feeaa8de97bd15e3f93c7d110a3dda6af4a783"
	const dir = "internal/store/internal/startupownership"
	paths, err := debtGit(root, "ls-tree", "-r", "--name-only", source, "--", dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range strings.Fields(string(paths)) {
		if !strings.HasSuffix(path, ".go") {
			continue
		}
		data, err := debtGit(root, "show", source+":"+path)
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, predecessor, path, string(data))
	}
	// #642/6191476293: the old digest is evidence, never a second accepted pin.
	const prior = "6065613a9040e4fc3f779d8e3f09afd0ea055421ea0965512e1de09a52e84ff3"
	if digest, err := debtProcessLeaseCustodyDigest(predecessor); err != nil || digest != prior {
		t.Fatalf("custody predecessor=%s error=%v", digest, err)
	}
	const typ = "github.com/division-sh/swarm/internal/store/internal/startupownership.StartupPostgresOwner"
	findings := []authorityFinding{
		{Kind: "raw-authority-export", File: dir + "/owner.go", Enclosing: "StartupPostgresOwner", Member: "exported-type", Resolved: typ, RawSQL: true},
		{Kind: "raw-authority-export", File: dir + "/owner.go", Enclosing: "NewPostgres", Member: "result:#1", Resolved: "*" + typ, RawSQL: true},
	}
	debtClassifyProcessLeaseCustody(predecessor, findings)
	for _, finding := range findings {
		if finding.Disposition != "" {
			t.Fatal("historical custody digest remains permissive:", finding)
		}
	}
}

func TestPersistenceAuthorityDebtProcessLeaseRelocationsAreOneWay(t *testing.T) {
	for _, pair := range debtProcessLeaseRelocations() {
		old, current := pair[0], pair[1]
		t.Run(current.Declaration, func(t *testing.T) {
			before, after := debtControlSet(old), debtControlSet(current)
			for _, head := range []map[string]authorityDebtSite{before, after} {
				if failures := authorityDebtRatchet(after, head, before, before); len(failures) != 0 {
					t.Fatal(failures)
				}
			}
			if failures := authorityDebtRatchet(after, after, after, after); len(failures) != 0 {
				t.Fatal("unchanged landed successor rejected:", failures)
			}
			if failures := authorityDebtRatchet(debtControlSet(), after, after, after); len(failures) != 0 {
				t.Fatal("genuine later deletion rejected:", failures)
			}
			_, normalized, source, failures := debtProcessLeaseRelocationComparisons(after, before, before, before)
			if len(failures) != 0 || authorityDebtCount(normalized) != old.Multiplicity || authorityDebtCount(source) != old.Multiplicity {
				t.Fatal("relocation earned deletion credit")
			}
			for _, mutation := range []string{"missing-donor", "changed-donor", "source-drift", "reverse", "increase", "coexist-actual", "coexist-head", "coexist-base", "coexist-source", "foreign-file", "foreign-type", "foreign-owner", "foreign-operation", "foreign-declaration", "unrelated-addition"} {
				t.Run(mutation, func(t *testing.T) {
					actual, head, trusted, origin := maps.Clone(after), maps.Clone(after), maps.Clone(before), maps.Clone(before)
					changed := current
					delete(actual, current.identity())
					switch mutation {
					case "missing-donor":
						trusted = debtControlSet()
					case "changed-donor":
						donor := old
						donor.Multiplicity++
						trusted = debtControlSet(donor)
					case "source-drift":
						origin = debtControlSet()
					case "reverse":
						actual, head, trusted, origin = before, before, after, after
					case "increase":
						changed.Multiplicity++
					case "coexist-actual":
						actual[old.identity()] = old
					case "coexist-head":
						head[old.identity()] = old
					case "coexist-base":
						trusted[current.identity()] = current
					case "coexist-source":
						origin[current.identity()] = current
					case "foreign-file":
						changed.File = "internal/runtime/foreign.go"
					case "foreign-type":
						changed.Resolved += "foreign"
					case "foreign-owner":
						changed.Replacement += "foreign"
					case "foreign-operation":
						changed.Operation += "foreign"
					case "foreign-declaration":
						changed.Declaration += "foreign"
					case "unrelated-addition":
						added := debtControlSite("call:Exec", 1)
						actual[added.identity()] = added
					}
					if mutation != "reverse" {
						actual[changed.identity()] = changed
					}
					if failures := authorityDebtRatchet(actual, head, trusted, origin); len(failures) == 0 {
						t.Fatal("hostile relocation admitted")
					}
				})
			}
		})
	}
}

func TestPersistenceAuthorityDebtProcessLeaseCollectorTransitionIsBounded(t *testing.T) {
	for _, donor := range []string{debtForkProofCollectorTo, debtProcessLeaseCollectorFrom} {
		t.Run(donor[:8], func(t *testing.T) {
			before, after := debtControlSet(), debtControlSet()
			for _, pair := range debtProcessLeaseRelocations() {
				before[pair[0].identity()], after[pair[1].identity()] = pair[0], pair[1]
			}
			inherited := debtControlSite("call:QueryRow", 2)
			before[inherited.identity()], after[inherited.identity()] = inherited, inherited
			if donor == debtProcessLeaseCollectorFrom {
				before = maps.Clone(after)
			}
			trusted := authorityDebtBaseline{BootstrapSource: debtBootstrapSource, Collector: donor, Sites: before}
			head := authorityDebtBaseline{BootstrapSource: debtBootstrapSource, Collector: debtProcessLeaseCollectorTo, Sites: after}
			for _, mutation := range []string{"exact", "downward", "landed-successor", "reverse", "unlanded-role-predecessor", "foreign-base", "foreign-destination", "unrotated", "rebootstrap", "missing-donor", "addition", "increase"} {
				t.Run(mutation, func(t *testing.T) {
					base, candidate := trusted, head
					base.Sites, candidate.Sites = maps.Clone(trusted.Sites), maps.Clone(head.Sites)
					current := debtProcessLeaseCollectorTo
					switch mutation {
					case "downward":
						delete(candidate.Sites, inherited.identity())
					case "landed-successor":
						base = candidate
					case "reverse":
						base, candidate, current = candidate, base, base.Collector
					case "unlanded-role-predecessor":
						base.Collector = debtFixtureRoleCollectorTo
					case "foreign-base":
						base.Collector = strings.Repeat("a", 64)
					case "foreign-destination":
						candidate.Collector, current = strings.Repeat("b", 64), strings.Repeat("b", 64)
					case "unrotated":
						candidate.Collector = base.Collector
					case "rebootstrap":
						candidate.BootstrapSource = strings.Repeat("c", 40)
					case "missing-donor":
						pair := debtProcessLeaseRelocations()[0]
						delete(base.Sites, pair[0].identity())
						delete(base.Sites, pair[1].identity())
					case "addition":
						added := debtControlSite("call:Exec", 1)
						candidate.Sites[added.identity()] = added
					case "increase":
						inherited.Multiplicity++
						candidate.Sites[inherited.identity()] = inherited
					}
					accept := mutation == "exact" || mutation == "landed-successor" ||
						(mutation == "downward" && donor == debtForkProofCollectorTo)
					if err := debtValidateCollectorIdentity(base.Collector, current, base, candidate, ""); (err == nil) != accept {
						t.Fatalf("accept=%v error=%v", accept, err)
					}
				})
			}
		})
	}
}

func TestPersistenceAuthorityDebtProcessLeaseRelocationRejectsSourceDrift(t *testing.T) {
	root, base, changed := persistenceAuthorityRepoRoot(t), t.TempDir(), t.TempDir()
	debtRequireProcessLeasePredecessor(t, root)
	before, after := debtControlSet(), debtControlSet()
	for _, pair := range debtProcessLeaseRelocations() {
		before[pair[0].identity()], after[pair[1].identity()] = pair[0], pair[1]
	}
	for _, path := range []string{"internal/store/construction/open.go", "internal/store/internal/runtimepersistence/runtime_startup_ownership_test.go"} {
		old, err := debtGit(root, "show", "f8ffc42420a93b6dfb3575c4a3dddeee9d858693:"+path)
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, base, path, string(old))
		current, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, changed, path, string(current))
	}
	if err := debtValidateProcessLeaseRelocationSource(root, base, after, before); err != nil {
		t.Fatal(err)
	}
	const path = "internal/store/construction/open.go"
	data, err := os.ReadFile(filepath.Join(changed, path))
	if err != nil {
		t.Fatal(err)
	}
	debtWriteModuleSource(t, changed, path, strings.Replace(string(data), "private.ComposeSQLiteRuntimeStoreWithBackendIdentity(schema, backend, identity)", "private.ComposeSQLiteRuntimeStore(schema, backend)", 1))
	if err := debtValidateProcessLeaseRelocationSource(changed, base, after, before); err == nil {
		t.Fatal("changed native constructor provenance admitted")
	}
	if err := debtValidateProcessLeaseRelocationSource(root, changed, after, before); err == nil {
		t.Fatal("changed donor source admitted")
	}
}

func debtRequireProcessLeasePredecessor(t *testing.T, root string) {
	t.Helper()
	if _, err := debtGit(root, "cat-file", "-e", "f8ffc42420a93b6dfb3575c4a3dddeee9d858693^{commit}"); err != nil {
		// Use the existing authenticated lineage owner to deepen hosted shallow
		// checkouts. Never accept caller-selected source or a baseline file.
		if _, err := debtTrustedBase(root); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPersistenceAuthorityDebtUnsupportedProcessWaitsUseTypedRefusal(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	const path = "internal/store/internal/startupownership/sqlite_possession_unsupported.go"
	file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, path), nil, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	want := "{\n\t_, err := acquireSQLiteFilePossession(\"\")\n\treturn nil, err\n}"
	seen := map[string]bool{}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || (fn.Name.Name != "waitSQLiteFilePossession" && fn.Name.Name != "waitSQLiteConstructionPossession") {
			continue
		}
		var body bytes.Buffer
		if err := format.Node(&body, token.NewFileSet(), fn.Body); err != nil {
			t.Fatal(err)
		}
		if body.String() != want {
			t.Fatalf("%s differs from the exact existing typed-refusal forwarder: %s", fn.Name, &body)
		}
		seen[fn.Name.Name] = true
	}
	if len(seen) != 2 {
		t.Fatal("both unsupported process waits must retain the refusal")
	}
}
