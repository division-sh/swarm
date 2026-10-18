package store_test

import (
	"bytes"
	"crypto/sha256"
	"fmt"
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
const debtProcessLeaseCollectorTo = "1a353f6c8e5cfe2e2fce2163231e4ac4832df0776daa8dbea177c6edb274bd90"

const debtProcessFenceCustodyPin = "e91d3e1b04948d4e07c157a9a66cb8a1f93b4c3221ad040b3ddf97ab98b7006d"
const debtProcessFenceCollectorTo = "e841ab0fc56bbc524f8987f1e4a95a24c49bf8cf083398bf40f4e971b8d1de78"

func TestPersistenceAuthorityDebtProcessLeaseReviewedSourcePins(t *testing.T) {
	root, base := persistenceAuthorityRepoRoot(t), t.TempDir()
	debtRequireProcessLeasePredecessor(t, root)
	for _, path := range []string{"internal/store/construction/open.go", "internal/store/internal/runtimepersistence/runtime_startup_ownership_test.go"} {
		data, err := debtGit(root, "show", "f8ffc42420a93b6dfb3575c4a3dddeee9d858693:"+path)
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, base, path, string(data))
	}
	for i, pair := range debtProcessLeaseRelocations() {
		for j, tree := range []string{base, root} {
			digest, err := debtProcessLeaseDeclarationDigest(tree, pair[j].File, pair[j].Declaration)
			if err != nil || digest != debtProcessLeaseSourcePins[i][j] {
				t.Errorf("pin[%d][%d]=%q error=%v", i, j, digest, err)
			}
		}
	}
	if digest, err := debtProcessLeaseCustodyDigest(root); err != nil || digest != debtProcessFenceCustodyPin {
		t.Errorf("custody pin=%q error=%v", digest, err)
	}
	debtRequireOriginalConstructionCustodyContinuation(t, root)
	if digest, err := debtCollectorDigest(root); err != nil || digest != debtProcessFenceCollectorTo {
		t.Errorf("collector pin=%q error=%v", digest, err)
	}
}

// Authenticate the complete additions before building a historical comparison
// view. The live census still scans every source declaration unchanged.
func debtRequireOriginalConstructionCustodyContinuation(t *testing.T, root string) {
	t.Helper()
	const dir = "internal/store/internal/startupownership"
	digest, err := debtProcessLeaseCustodyDigest(root)
	if err != nil || digest != debtProcessFenceCustodyPin {
		t.Fatalf("original-construction custody source changed: %s/%v", digest, err)
	}
	projected := t.TempDir()
	entries, err := os.ReadDir(filepath.Join(root, dir))
	if err != nil {
		t.Fatal(err)
	}
	fragments := map[string]string{
		"AcquireProcessCapability":      "fb87cc63082df6bdb949ac9f144c437bca4036eab2dc3d1a79c6fb260b0de5f8",
		"CaptureOriginalProcessForTest": "ffd050e3dcf0d1d9b4fa31966d207e61cd74a911a32961b714c8c6a1228ced5d",
		"observeConstructedProcess":     "b629ef1d287ed729af1ebcc42d73c4f36172d51b91717f8cda07ec4e4c1f0e56",
	}
	removed := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}
		path := dir + "/" + entry.Name()
		file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, path), nil, parser.AllErrors)
		if err != nil {
			t.Fatal(err)
		}
		var declarations []ast.Decl
		for _, decl := range file.Decls {
			if group, ok := decl.(*ast.GenDecl); ok {
				for _, spec := range group.Specs {
					named, ok := spec.(*ast.TypeSpec)
					if !ok || named.Name.Name != "StartupPostgresOwner" {
						continue
					}
					shape, ok := named.Type.(*ast.StructType)
					if !ok {
						t.Fatal("native custody carrier is not a struct")
					}
					var fields []*ast.Field
					for _, field := range shape.Fields.List {
						if len(field.Names) == 1 && field.Names[0].Name == "originalObservation" {
							pointer, pointerOK := field.Type.(*ast.StarExpr)
							var named *ast.Ident
							if pointerOK {
								named, _ = pointer.X.(*ast.Ident)
							}
							if !pointerOK || named == nil || named.Name != "OriginalProcessCaptureForTest" || field.Tag != nil {
								t.Fatal("original observation field changed")
							}
							removed++
							continue
						}
						fields = append(fields, field)
					}
					shape.Fields.List = fields
				}
			}
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || len(fn.Recv.List) != 1 {
				declarations = append(declarations, decl)
				continue
			}
			pointer, ok := fn.Recv.List[0].Type.(*ast.StarExpr)
			if !ok {
				declarations = append(declarations, decl)
				continue
			}
			owner, ok := pointer.X.(*ast.Ident)
			pin := fragments[fn.Name.Name]
			if !ok || owner.Name != "StartupPostgresOwner" || pin == "" {
				declarations = append(declarations, decl)
				continue
			}
			var body bytes.Buffer
			if err := format.Node(&body, token.NewFileSet(), fn); err != nil || fmt.Sprintf("%x", sha256.Sum256(body.Bytes())) != pin {
				t.Fatalf("original construction fragment changed: %s", fn.Name.Name)
			}
			removed++
			if fn.Name.Name == "AcquireProcessCapability" {
				cut := len(fn.Body.List) - 3
				if cut < 0 {
					t.Fatal("original constructor lost its publication boundary")
				}
				var call bytes.Buffer
				if err := format.Node(&call, token.NewFileSet(), fn.Body.List[cut]); err != nil || call.String() != "s.observeConstructedProcess(session, capability)" {
					t.Fatal("original construction notification moved")
				}
				fn.Body.List = append(fn.Body.List[:cut], fn.Body.List[cut+1:]...)
				declarations = append(declarations, decl)
			}
		}
		file.Decls = declarations
		var b bytes.Buffer
		if err := format.Node(&b, token.NewFileSet(), file); err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, projected, path, b.String())
	}
	if removed != 4 {
		t.Fatalf("original custody continuation fragments=%d, want4", removed)
	}
	if prior, err := debtProcessLeaseCustodyDigest(projected); err != nil || prior != debtProcessLeaseCustodyPin {
		t.Fatalf("original construction changed its custody predecessor: %s/%v", prior, err)
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
	t.Run("original-construction-continuation", func(t *testing.T) {
		inherited := debtControlSite("call:QueryRow", 2)
		trusted := authorityDebtBaseline{BootstrapSource: debtBootstrapSource, Collector: debtProcessLeaseCollectorTo, Sites: debtControlSet(inherited)}
		head := trusted
		head.Collector = debtProcessFenceCollectorTo
		for _, mutation := range []string{"exact", "downward", "landed", "addition", "increase", "foreign", "reverse", "stale-destination", "rebootstrap"} {
			t.Run(mutation, func(t *testing.T) {
				base, candidate := trusted, head
				base.Sites, candidate.Sites = maps.Clone(trusted.Sites), maps.Clone(head.Sites)
				switch mutation {
				case "downward":
					delete(candidate.Sites, inherited.identity())
				case "landed":
					base = candidate
				case "addition":
					added := debtControlSite("call:Exec", 1)
					candidate.Sites[added.identity()] = added
				case "increase":
					increased := inherited
					increased.Multiplicity++
					candidate.Sites[inherited.identity()] = increased
				case "foreign":
					base.Collector = strings.Repeat("a", 64)
				case "reverse":
					base, candidate = candidate, base
				case "stale-destination":
					candidate.Collector = debtForkProofCollectorTo
				case "rebootstrap":
					candidate.BootstrapSource = strings.Repeat("b", 40)
				}
				accept := mutation == "exact" || mutation == "downward" || mutation == "landed"
				if err := debtValidateCollectorIdentity(base.Collector, candidate.Collector, base, candidate, ""); (err == nil) != accept {
					t.Fatalf("original construction collector accept=%v error=%v", accept, err)
				}
			})
		}
	})
	for _, donor := range []string{debtForkProofCollectorTo} {
		t.Run(donor[:8], func(t *testing.T) {
			before, after := debtControlSet(), debtControlSet()
			for _, pair := range debtProcessLeaseRelocations() {
				before[pair[0].identity()], after[pair[1].identity()] = pair[0], pair[1]
			}
			inherited := debtControlSite("call:QueryRow", 2)
			before[inherited.identity()], after[inherited.identity()] = inherited, inherited
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
						delete(base.Sites, debtProcessLeaseRelocations()[0][0].identity())
					case "addition":
						added := debtControlSite("call:Exec", 1)
						candidate.Sites[added.identity()] = added
					case "increase":
						inherited.Multiplicity++
						candidate.Sites[inherited.identity()] = inherited
					}
					accept := mutation == "exact" || mutation == "downward" || mutation == "landed-successor"
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

func TestPersistenceAuthorityDebtProcessLeaseCustodyIsSourceBound(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	const dir = "internal/store/internal/startupownership"
	entries, err := os.ReadDir(filepath.Join(root, dir))
	if err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []string{"actual", "raw-field", "raw-getter", "sql-callback", "forwarder", "custody-change", "construction-wiring", "observation-field", "observation-constructor-cut", "observation-installer", "observation-binding", "fifth-change"} {
		t.Run(mutation, func(t *testing.T) {
			copy := t.TempDir()
			for _, entry := range entries {
				if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
					continue
				}
				data, err := os.ReadFile(filepath.Join(root, dir, entry.Name()))
				if err != nil {
					t.Fatal(err)
				}
				text := string(data)
				if entry.Name() == "owner.go" && mutation == "raw-field" {
					text = strings.ReplaceAll(text, "constructionLease", "ConstructionLease")
				}
				if entry.Name() == "owner.go" && mutation == "observation-field" {
					text = strings.Replace(text, "originalObservation", "foreignObservation", 1)
				}
				if entry.Name() == "owner.go" && mutation == "observation-constructor-cut" {
					text = strings.Replace(text, "s.observeConstructedProcess(session, capability)", "s.observeConstructedProcess(session, nil)", 1)
				}
				if entry.Name() == "test_original_session_interruption.go" && mutation == "observation-installer" {
					text = strings.Replace(text, "s.originalObservation = p", "s.originalObservation = nil", 1)
				}
				if entry.Name() == "test_original_session_interruption.go" && mutation == "observation-binding" {
					text = strings.Replace(text, "p.session, p.capability = session, capability", "p.session, p.capability = nil, capability", 1)
				}
				if entry.Name() == "postgres_construction.go" && mutation == "custody-change" {
					text = strings.Replace(text, "return lease, capacity, nil", "return s.acquireConstructionPossession(ctx)", 1)
				}
				if entry.Name() == "postgres_construction.go" && mutation == "construction-wiring" {
					text = strings.Replace(text, "schemastore.BindPostgresProcessLease(schema, lease)", "schemastore.BindPostgresProcessLease(schema, nil)", 1)
				}
				debtWriteModuleSource(t, copy, dir+"/"+entry.Name(), text)
			}
			additions := map[string]string{
				"raw-getter":   "func(s *StartupPostgresOwner)Raw()any{return s.constructionLease}",
				"sql-callback": "func(s *StartupPostgresOwner)Call(f func(any)){f(s.constructionLease)}",
				"forwarder":    "func(s *StartupPostgresOwner)Forward()any{return s.Raw()}",
				"fifth-change": "func(s *StartupPostgresOwner)Fifth(){panic(\"unreviewed\")}",
			}
			if extra := additions[mutation]; extra != "" {
				debtWriteModuleSource(t, copy, dir+"/escape.go", "package startupownership\n"+extra)
			}
			const typ = "github.com/division-sh/swarm/internal/store/internal/startupownership.StartupPostgresOwner"
			findings := []authorityFinding{
				{Kind: "raw-authority-export", File: dir + "/owner.go", Enclosing: "StartupPostgresOwner", Member: "exported-type", Resolved: typ, RawSQL: true},
				{Kind: "raw-authority-export", File: dir + "/owner.go", Enclosing: "NewPostgres", Member: "result:#1", Resolved: "*" + typ, RawSQL: true},
			}
			debtClassifyProcessLeaseCustody(copy, findings)
			for _, finding := range findings {
				if allowed := debtRawSQLDispositionAllowed(finding, "fixture-2151"); allowed != (mutation == "actual") {
					t.Fatalf("mutation=%s owned=%v finding=%+v", mutation, allowed, finding)
				}
				if mutation == "actual" {
					for _, field := range []string{"kind", "file", "declaration", "member", "resolved", "raw"} {
						foreign := finding
						mutate := map[string]*string{"kind": &foreign.Kind, "file": &foreign.File, "declaration": &foreign.Enclosing, "member": &foreign.Member, "resolved": &foreign.Resolved}
						if field == "raw" {
							foreign.RawSQL = false
							if debtProcessLeaseCustodyFinding(foreign) {
								t.Fatal("nonraw identity misclassified")
							}
							continue
						}
						*mutate[field] += "foreign"
						if debtProcessLeaseCustodyFinding(foreign) || (field != "kind" && debtRawSQLDispositionAllowed(foreign, "fixture-2151")) {
							t.Fatalf("foreign %s received native custody class", field)
						}
					}
				}
			}
		})
	}
}

func TestPersistenceAuthorityDebtProcessLeaseCustodyUsesCollectorMember(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	const path = "internal/store/internal/startupownership/owner.go"
	file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, path), nil, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	const typ = "github.com/division-sh/swarm/internal/store/internal/startupownership.StartupPostgresOwner"
	var findings []authorityFinding
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv != nil || fn.Name.Name != "NewPostgres" {
			continue
		}
		// Use the collector's actual authored-field naming, not a shortened TSV
		// operation (which discards the occurrence index).
		findings = append(findings, authorityFinding{Kind: "raw-authority-export", File: path, Enclosing: fn.Name.Name,
			Member: debtAuthorityFieldName("result", 0, fn.Type.Results.List[0]), Resolved: "*" + typ, RawSQL: true})
	}
	findings = append(findings, authorityFinding{Kind: "raw-authority-export", File: path, Enclosing: "StartupPostgresOwner",
		Member: "exported-type", Resolved: typ, RawSQL: true})
	debtClassifyProcessLeaseCustody(root, findings)
	if len(findings) != 2 || len(authorityDebtSites(findings)) != 0 {
		t.Fatalf("exact source-owned findings must remain visible but not become consumer debt: %+v", findings)
	}
	for _, finding := range findings {
		if !finding.RawSQL || finding.Kind != "raw-authority-export" || !debtProcessLeaseCustodyFinding(finding) {
			t.Fatal("classification hid or altered the all-owner evidence:", finding)
		}
	}
}
