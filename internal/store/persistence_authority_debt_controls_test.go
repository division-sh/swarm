package store_test

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/testplanning"
)

func debtControlSite(operation string, count int) authorityDebtSite {
	return authorityDebtSite{Kind: "raw-operation", File: "internal/runtime/legacy_test.go", Declaration: "legacy", Operation: operation, Resolved: "func(context.Context,string)*sql.Row", Family: "observation", Replacement: "domain observation owner", Multiplicity: count}
}
func debtControlSet(sites ...authorityDebtSite) map[string]authorityDebtSite {
	out := map[string]authorityDebtSite{}
	for _, site := range sites {
		out[site.identity()] = site
	}
	return out
}

func TestPersistenceAuthorityDebtRatchetRejectsPaymentDuplicationReseedAndResurrection(t *testing.T) {
	one, two := debtControlSite("call:QueryRow", 1), debtControlSite("call:Exec", 1)
	base := debtControlSet(one, two)
	for _, test := range []struct {
		name                        string
		actual, head, trustedSource map[string]authorityDebtSite
		reject                      bool
	}{
		{"unchanged-legacy-with-unrelated-edit", base, base, base, false},
		{"genuine-downward-deletion", debtControlSet(one), debtControlSet(one), base, false},
		{"unrefreshed-removal", debtControlSet(one), base, base, false},
		{"new-site", debtControlSet(one, two, debtControlSite("call:Begin", 1)), base, base, true},
		{"deletion-cannot-pay-equal-count", debtControlSet(one, debtControlSite("call:Begin", 1)), base, base, true},
		{"deletion-cannot-pay-lower-count", debtControlSet(debtControlSite("call:Begin", 1)), base, base, true},
		{"duplicate-operation", debtControlSet(debtControlSite(one.Operation, 2)), base, base, true},
		{"raised-baseline", debtControlSet(one), debtControlSet(debtControlSite(one.Operation, 2), two), base, true},
		{"reseeded-baseline", debtControlSet(one), debtControlSet(one, debtControlSite("call:Begin", 1)), base, true},
		{"obsolete-row-cannot-authorize-resurrection", base, base, debtControlSet(one), true},
	} {
		t.Run(test.name, func(t *testing.T) {
			failures := authorityDebtRatchet(test.actual, test.head, base, test.trustedSource)
			if (len(failures) > 0) != test.reject {
				t.Fatalf("ratchet=%v expected rejection=%v", failures, test.reject)
			}
			if test.reject && !strings.Contains(strings.Join(failures, "\n"), "owner=") {
				t.Fatalf("missing owner routing: %v", failures)
			}
		})
	}
}

func TestPersistenceAuthorityDebtBaselineRejectsCorruptionAndDuplicateRows(t *testing.T) {
	baseline := authorityDebtBaseline{BootstrapSource: strings.Repeat("a", 40), Collector: strings.Repeat("b", 64), Sites: debtControlSet(debtControlSite("call:QueryRow", 1))}
	valid := marshalAuthorityDebtBaseline(baseline)
	if parsed, err := parseAuthorityDebtBaseline(valid); err != nil || !debtSitesEqual(parsed.Sites, baseline.Sites) {
		t.Fatalf("round trip: %+v %v", parsed, err)
	}
	row := strings.Split(string(valid), "\n")[4]
	for _, test := range []struct {
		name string
		data []byte
	}{
		{"empty", nil}, {"truncated", valid[:30]},
		{"duplicate", append(bytes.Clone(valid), []byte(row+"\n")...)},
		{"zero-multiplicity", []byte(strings.Replace(string(valid), "\t1\n", "\t0\n", 1))},
		{"negative-multiplicity", []byte(strings.Replace(string(valid), "\t1\n", "\t-1\n", 1))},
		{"unknown-metadata", append(bytes.Clone(valid), []byte("# allowed=true\n")...)},
		{"missing-field", []byte(strings.Replace(string(valid), "\tlegacy\t", "\t\t", 1))},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := parseAuthorityDebtBaseline(test.data); err == nil {
				t.Fatal("corrupt baseline accepted")
			}
		})
	}
}

func TestPersistenceAuthorityDebtCensusRetainsAliasesCallbacksAndOpaqueConstruction(t *testing.T) {
	for name, source := range map[string]string{
		"alias":              `package probe;import "database/sql";type Alias=sql.DB;func legacy(db *Alias){db.QueryRow("one")}`,
		"embedding":          `package probe;import "database/sql";type Carrier struct{*sql.DB};func legacy(db Carrier){db.QueryRow("one")}`,
		"named-callback":     `package probe;import("context";"database/sql");type Callback func(context.Context,*sql.Tx)error;func legacy(cb Callback){_ = cb}`,
		"context-protocol":   `package probe;import("context";"database/sql");func legacy(ctx context.Context,tx *sql.Tx){_ = context.WithValue(ctx,"tx",tx)}`,
		"opaque-constructor": `package probe;type SQLiteRuntimeStore struct{};func replacement()*SQLiteRuntimeStore{return nil};func RequireRun(){factory:=replacement;_ = factory()}`,
	} {
		t.Run(name, func(t *testing.T) {
			path := "internal/runtime/legacy_test.go"
			if name == "opaque-constructor" {
				path = "internal/store/storetest/run_lifecycle.go"
			}
			findings := debtAuthorityFindingsFromSource(t, path, source)
			if len(authorityDebtSites(findings)) == 0 {
				t.Fatalf("same-concept interpreter disappeared: %+v", findings)
			}
		})
	}
}

func TestPersistenceAuthorityDebtCensusRetainsNonSQLConstructionAcrossOrdinaryPackages(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.com/authorityprobe\n\ngo 1.25.0\n"), 0644); err != nil {
		t.Fatal(err)
	}
	for _, directory := range []string{"internal/apiv1", "internal/cliapp", "internal/testutil/ordinaryfixture"} {
		path := filepath.Join(root, directory)
		if err := os.MkdirAll(path, 0755); err != nil {
			t.Fatal(err)
		}
		source := "package probe;type SQLiteRuntimeStore struct{};func replacement()*SQLiteRuntimeStore{return nil};func legacy(){factory:=replacement;_ = factory()}\n"
		if err := os.WriteFile(filepath.Join(path, "legacy_test.go"), []byte(source), 0644); err != nil {
			t.Fatal(err)
		}
	}
	findings := debtLoadPersistenceAuthorityFindings(t, root)
	sites := authorityDebtSites(findings)
	for _, directory := range []string{"internal/apiv1", "internal/cliapp", "internal/testutil/ordinaryfixture"} {
		found := false
		for _, site := range sites {
			found = found || site.File == directory+"/legacy_test.go" && site.Kind == "selected-store-construction"
		}
		if !found {
			t.Errorf("ordinary-package non-SQL constructor escaped: %s; findings=%+v", directory, findings)
		}
	}
}

func TestPersistenceAuthorityDebtIdentityIgnoresLinesAndEarlierOrdinalRemoval(t *testing.T) {
	path := "internal/runtime/legacy_test.go"
	before := `package probe;import "database/sql";func legacy(db *sql.DB){db.QueryRow("one");db.QueryRow("two")}`
	after := `package probe

import "database/sql"
// An unrelated comment and harmless source movement.
func legacy(db *sql.DB){
db.QueryRow("two")
}`
	base := authorityDebtSites(debtAuthorityFindingsFromSource(t, path, before))
	head := authorityDebtSites(debtAuthorityFindingsFromSource(t, path, after))
	if failures := authorityDebtSubset(head, base, "line movement / earlier deletion"); len(failures) > 0 {
		t.Fatal(failures)
	}
	if authorityDebtCount(head) >= authorityDebtCount(base) {
		t.Fatal("earlier operation was not removed")
	}
	moved := strings.Replace(before, "func legacy", "\n\n// moved\nfunc legacy", 1)
	if got := authorityDebtSites(debtAuthorityFindingsFromSource(t, path, moved)); !debtSitesEqual(got, base) {
		t.Fatal("line movement manufactured debt identities")
	}
	if got := authorityDebtSites(debtAuthorityFindingsFromSource(t, path, before+"\nfunc unrelated(){_ = 42}")); !debtSitesEqual(got, base) {
		t.Fatal("unrelated code edit changed inherited debt")
	}
}

func TestPersistenceAuthorityDebtRoleLabelsCannotGrantPermission(t *testing.T) {
	for _, label := range []string{"fixture-2151", "private-backend", "construction-owner", "adjacent-2149", "typed-public-facade"} {
		finding := authorityFinding{Kind: "raw-operation", File: "internal/runtime/legacy_test.go", Enclosing: "legacy", Member: "call:Exec#1", Resolved: "raw operation", RawSQL: true, Disposition: label}
		if len(authorityDebtSites([]authorityFinding{finding})) != 1 {
			t.Fatalf("label %s erased runtime debt", label)
		}
	}
	for _, path := range []string{"internal/store/internal/backend/postgres/backend.go", "internal/store/construction/open.go", "internal/testpostgres/connection.go"} {
		finding := authorityFinding{Kind: "raw-operation", File: path, Enclosing: "owner", Member: "call:Exec#1", Resolved: "owned physical operation", RawSQL: true}
		if len(authorityDebtSites([]authorityFinding{finding})) != 0 {
			t.Fatalf("legitimate role became ordinary debt: %s", path)
		}
		finding.Kind = "raw-authority-export"
		if len(authorityDebtSites([]authorityFinding{finding})) != 1 {
			t.Fatalf("private role permitted SQL export: %s", path)
		}
	}
}

func TestPersistenceAuthorityDebtSelectedScopeIncludesAliasesAndInactiveSource(t *testing.T) {
	for _, source := range []string{
		`package runtime;import alias "github.com/division-sh/swarm/internal/store/construction";var _ = alias.OpenPostgres`,
		`//go:build never_swarm_authority\npackage runtime;import alias "github.com/division-sh/swarm/internal/store/construction";var _ = alias.OpenPostgres`,
	} {
		source = strings.ReplaceAll(source, `\n`, "\n")
		file, err := parser.ParseFile(token.NewFileSet(), "disabled.go", source, parser.AllErrors)
		if err != nil {
			t.Fatal(err)
		}
		findings := debtSelectedBoundarySource("internal/runtime/disabled_test.go", file)
		if len(authorityDebtSites(findings)) == 0 {
			t.Fatalf("aliased/inactive constructor escaped: %+v", findings)
		}
	}
	root := t.TempDir()
	path := filepath.Join(root, "internal/runtime")
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "disabled.go"), []byte("//go:build never_swarm_authority\n\npackage runtime\nimport \"database/sql\"\nvar _ *sql.DB\n"), 0644); err != nil {
		t.Fatal(err)
	}
	source, err := os.ReadFile(filepath.Join(path, "disabled.go"))
	if err != nil {
		t.Fatal(err)
	}
	if got := debtAuthorityFindingsFromSource(t, "internal/runtime/disabled.go", string(source)); len(authorityDebtSites(got)) == 0 {
		t.Fatalf("inactive raw authority disappeared: %+v", got)
	}
}

func debtWriteModuleSource(t *testing.T, root, path, source string) {
	t.Helper()
	full := filepath.Join(root, path)
	if err := os.MkdirAll(filepath.Dir(full), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(source), 0644); err != nil {
		t.Fatal(err)
	}
}

func TestPersistenceAuthorityDebtCensusPackageInitializers(t *testing.T) {
	for name, source := range map[string]string{
		"blank-scalar":   `package probe;import "database/sql";var _=func(db *sql.DB)bool{db.Exec("one");return true}(nil)`,
		"named-scalar":   `package probe;import "database/sql";var value=func(db *sql.DB)bool{db.Exec("one");return true}(nil)`,
		"stored-closure": `package probe;import "database/sql";var value=func(db *sql.DB){db.Exec("one")}`,
		"context":        `package probe;import("context";"database/sql");var _=func(ctx context.Context,tx *sql.Tx)bool{_ = context.WithValue(ctx,"tx",tx);return true}(nil,nil)`,
	} {
		t.Run(name, func(t *testing.T) {
			findings := debtAuthorityFindingsFromSource(t, "internal/runtime/init.go", source)
			if authorityDebtCount(authorityDebtSites(findings)) == 0 {
				t.Fatalf("initializer authority escaped: %+v", findings)
			}
		})
	}
}

func TestPersistenceAuthorityDebtCensusRepeatedOccurrences(t *testing.T) {
	root := t.TempDir()
	debtWriteModuleSource(t, root, "go.mod", "module example.com/probe\n\ngo 1.25.0\n")
	source := "package probe;import \"database/sql\";var db *sql.DB;func init(){db.Exec(\"one\")}\n"
	debtWriteModuleSource(t, root, "internal/runtime/probe.go", source)
	before := authorityDebtSites(debtLoadPersistenceAuthorityFindings(t, root))
	debtWriteModuleSource(t, root, "internal/runtime/probe_test.go", "package probe;import \"testing\";func TestUnrelated(t *testing.T){}\n")
	if got := authorityDebtSites(debtLoadPersistenceAuthorityFindings(t, root)); !debtSitesEqual(before, got) {
		t.Fatal("test package variants multiplied the same source occurrences")
	}
	debtWriteModuleSource(t, root, "internal/runtime/probe.go", source+"func init(){db.Exec(\"two\")}\n")
	after := authorityDebtSites(debtLoadPersistenceAuthorityFindings(t, root))
	if authorityDebtCount(after) != authorityDebtCount(before)+1 || len(authorityDebtSubset(after, before, "second init")) == 0 {
		t.Fatalf("second initializer collapsed: before=%d after=%d", authorityDebtCount(before), authorityDebtCount(after))
	}
	blank := "package probe;import \"database/sql\";var _=func(db *sql.DB)bool{db.Exec(\"one\");return true}(nil)\n"
	debtWriteModuleSource(t, root, "internal/runtime/probe.go", blank)
	before = authorityDebtSites(debtLoadPersistenceAuthorityFindings(t, root))
	debtWriteModuleSource(t, root, "internal/runtime/probe.go", blank+"var _=func(db *sql.DB)bool{db.Exec(\"two\");return true}(nil)\n")
	after = authorityDebtSites(debtLoadPersistenceAuthorityFindings(t, root))
	if authorityDebtCount(after) <= authorityDebtCount(before) || len(authorityDebtSubset(after, before, "second blank initializer")) == 0 {
		t.Fatal("distinct blank declarations collapsed")
	}
}

func TestPersistenceAuthorityDebtCensusPreservesAugmentedVariantEvidence(t *testing.T) {
	root := t.TempDir()
	debtWriteModuleSource(t, root, "go.mod", "module example.com/probe\n\ngo 1.25.0\n")
	prefix := "internal/store/internal/runtimepersistence/"
	debtWriteModuleSource(t, root, prefix+"holder.go", "package probe;type Holder struct{};func NewHolder()*Holder{return nil}\n")
	debtWriteModuleSource(t, root, prefix+"holder_test.go", "package probe;import \"database/sql\";func(h *Holder)Database()*sql.DB{return nil}\n")
	foundType, foundResult := false, false
	for _, site := range authorityDebtSites(debtLoadPersistenceAuthorityFindings(t, root)) {
		if site.File != prefix+"holder.go" || site.Kind != "raw-authority-export" {
			continue
		}
		foundType = foundType || site.Declaration == "Holder"
		foundResult = foundResult || site.Declaration == "NewHolder"
	}
	if !foundType || !foundResult {
		t.Fatalf("first normal package load erased augmented raw evidence: type=%v result=%v", foundType, foundResult)
	}
}

func TestPersistenceAuthorityDebtCensusInactiveLocalOperations(t *testing.T) {
	root := t.TempDir()
	debtWriteModuleSource(t, root, "go.mod", "module example.com/probe\n\ngo 1.25.0\n")
	debtWriteModuleSource(t, root, "internal/runtime/active.go", "package probe;import \"database/sql\";func Database()*sql.DB{return nil}\n")
	debtWriteModuleSource(t, root, "internal/runtime/hidden.go", "//go:build never_swarm_authority\n\npackage probe;func hidden(){Database().Exec(\"one\")}\n")
	before := authorityDebtSites(debtLoadPersistenceAuthorityFindings(t, root))
	found := false
	for _, site := range before {
		found = found || site.File == "internal/runtime/hidden.go" && site.Kind == "raw-operation"
	}
	if !found {
		t.Fatal("inactive package-local SQL escaped")
	}
	debtWriteModuleSource(t, root, "internal/runtime/hidden.go", "//go:build never_swarm_authority\n\npackage probe;func hidden(){db:=Database();db.Exec(\"one\");db.Exec(\"two\")}\n")
	after := authorityDebtSites(debtLoadPersistenceAuthorityFindings(t, root))
	if len(authorityDebtSubset(after, before, "new inactive operation")) == 0 {
		t.Fatal("extra inactive SQL operation paid by its old source marker")
	}
	debtWriteModuleSource(t, root, "internal/runtime/hidden.go", "//go:build never_swarm_authority\n\npackage probe;func harmless()string{return \"no authority\"}\n")
	for _, site := range authorityDebtSites(debtLoadPersistenceAuthorityFindings(t, root)) {
		if site.File == "internal/runtime/hidden.go" {
			t.Fatalf("legitimate inactive source became authority: %+v", site)
		}
	}
}

func TestPersistenceAuthorityDebtCensusPlatformIndependentSource(t *testing.T) {
	root := t.TempDir()
	debtWriteModuleSource(t, root, "go.mod", "module example.com/probe\n\ngo 1.25.0\n")
	debtWriteModuleSource(t, root, "internal/runtime/active.go", "package probe;import \"database/sql\";func Database()*sql.DB{return nil}\n")
	for _, goos := range []string{"linux", "darwin"} {
		debtWriteModuleSource(t, root, "internal/runtime/platform_"+goos+".go", "package probe;func platform(){Database().Exec(\"one\")}\n")
	}
	var before map[string]authorityDebtSite
	for _, goos := range []string{"linux", "darwin", "windows"} {
		t.Setenv("GOOS", goos)
		t.Setenv("GOARCH", "arm64")
		t.Setenv("CGO_ENABLED", "1")
		t.Setenv("GOFLAGS", "-tags=ignored_host_selection")
		got := authorityDebtSites(debtLoadPersistenceAuthorityFindings(t, root))
		if before != nil && !debtSitesEqual(before, got) {
			t.Fatalf("identical source changed debt with host %s", goos)
		}
		before = got
	}
	debtWriteModuleSource(t, root, "internal/runtime/platform_darwin.go", "package probe;func platform(){Database().Exec(\"one\");Database().Exec(\"two\")}\n")
	if len(authorityDebtSubset(authorityDebtSites(debtLoadPersistenceAuthorityFindings(t, root)), before, "new platform operation")) == 0 {
		t.Fatal("new platform-specific operation escaped")
	}
}

func TestPersistenceAuthorityDebtCensusOwnedSourceBeforeLoading(t *testing.T) {
	root := t.TempDir()
	debtWriteModuleSource(t, root, "go.mod", "module example.com/probe\n\ngo 1.25.0\n")
	debtWriteModuleSource(t, root, "internal/runtime/active.go", "package probe;import \"database/sql\";func Database()*sql.DB{return nil}\n")
	before := authorityDebtSites(debtLoadPersistenceAuthorityFindings(t, root))
	debtWriteModuleSource(t, root, "foreign/.git", "gitdir: /unrelated/checkout\n")
	debtWriteModuleSource(t, root, "foreign/foreign.go", "package foreign;import \"database/sql\";func hidden(db *sql.DB){db.Exec(\"foreign\")}\n")
	if got := authorityDebtSites(debtLoadPersistenceAuthorityFindings(t, root)); !debtSitesEqual(before, got) {
		t.Fatal("active foreign checkout entered the owned census")
	}
	debtWriteModuleSource(t, root, "foreign/foreign.go", "this is invalid foreign source")
	if got := authorityDebtSites(debtLoadPersistenceAuthorityFindings(t, root)); !debtSitesEqual(before, got) {
		t.Fatal("broken foreign checkout influenced owned loading")
	}
	debtWriteModuleSource(t, root, "internal/runtime/untracked_test.go", "package probe;func untracked(){Database().Exec(\"owned\")}\n")
	if len(authorityDebtSubset(authorityDebtSites(debtLoadPersistenceAuthorityFindings(t, root)), before, "owned untracked source")) == 0 {
		t.Fatal("owned untracked source escaped")
	}
}

func TestPersistenceAuthorityDebtExcludedWindowsAndUnknownDeclarations(t *testing.T) {
	root := t.TempDir()
	debtWriteModuleSource(t, root, "go.mod", "module example.com/probe\n\ngo 1.25.0\n")
	debtWriteModuleSource(t, root, "active.go", "package probe;func harmless()string{return \"safe\"}\n")
	path := "internal/runtime/process_tree_windows.go"
	debtWriteModuleSource(t, root, path, `//go:build windows

package probe
import("os/exec";"syscall";"golang.org/x/sys/windows")
func prepare(cmd *exec.Cmd){cmd.SysProcAttr=&syscall.SysProcAttr{};cmd.SysProcAttr.CreationFlags|=windows.CREATE_NEW_PROCESS_GROUP}
func helper(){unknownDatabase().Exec("one")}
var callback=unknownDatabase().Exec
var _=func()bool{callback("two");return true}()
`)
	sites := authorityDebtSites(debtLoadPersistenceAuthorityFindings(t, root))
	unknown := 0
	for _, site := range sites {
		if site.File == path && site.Kind == "unresolved-excluded-source" {
			unknown += site.Multiplicity
			if site.Family != "excluded-source-uncertainty" || !strings.Contains(site.Replacement, "classification") {
				t.Fatalf("uncertainty mislabeled as SQL/permission: %+v", site)
			}
		}
	}
	if unknown != 4 {
		t.Fatalf("Windows field/helper/method-value/initializer uncertainty missing: got %d sites=%+v", unknown, sites)
	}
}

func TestPersistenceAuthorityDebtExcludedUncertaintyRatchet(t *testing.T) {
	root := t.TempDir()
	debtWriteModuleSource(t, root, "go.mod", "module example.com/probe\n\ngo 1.25.0\n")
	debtWriteModuleSource(t, root, "active.go", "package probe;func anchor(){}\n")
	path := "hidden.go"
	prefix := "//go:build never_swarm_authority\n\npackage probe\n"
	first := "func init(){unknownCallback(\"one\")}\n"
	second := "var _=func()bool{unknownMethodValue();return true}()\n"
	debtWriteModuleSource(t, root, path, prefix+first+second)
	before := authorityDebtSites(debtLoadPersistenceAuthorityFindings(t, root))
	debtWriteModuleSource(t, root, path, prefix+"\n// harmless position change\n"+first+second)
	if got := authorityDebtSites(debtLoadPersistenceAuthorityFindings(t, root)); !debtSitesEqual(before, got) {
		t.Fatal("comments/line movement changed uncertainty identity")
	}
	debtWriteModuleSource(t, root, path, prefix+first+second+second)
	more := authorityDebtSites(debtLoadPersistenceAuthorityFindings(t, root))
	if authorityDebtCount(more) != authorityDebtCount(before)+1 || len(authorityDebtSubset(more, before, "duplicate uncertainty")) == 0 {
		t.Fatal("distinct identical excluded initializers collapsed")
	}
	debtWriteModuleSource(t, root, path, prefix+"var _=func()bool{unknownMethodValue();unknownCallback(\"two\");return true}()\n")
	changed := authorityDebtSites(debtLoadPersistenceAuthorityFindings(t, root))
	if authorityDebtCount(changed) >= authorityDebtCount(before) || len(authorityDebtSubset(changed, before, "changed despite deletion")) == 0 {
		t.Fatal("deletion paid for changed unresolved authority")
	}
	debtWriteModuleSource(t, root, path, strings.Replace(prefix, "never_swarm_authority", "another_excluded_tag", 1)+first+second)
	if got := authorityDebtSites(debtLoadPersistenceAuthorityFindings(t, root)); len(authorityDebtSubset(got, before, "build context changed")) == 0 {
		t.Fatal("excluded build context change disappeared")
	}
}

func TestPersistenceAuthorityDebtExcludedPlatformIdentity(t *testing.T) {
	root := t.TempDir()
	debtWriteModuleSource(t, root, "go.mod", "module example.com/probe\n\ngo 1.25.0\n")
	debtWriteModuleSource(t, root, "active.go", "package probe;func anchor(){}\n")
	debtWriteModuleSource(t, root, "hidden_windows.go", "package probe;import \"syscall\";func windowsField(p *syscall.SysProcAttr){p.CreationFlags=1}\n")
	var before map[string]authorityDebtSite
	for _, goos := range []string{"linux", "darwin", "windows"} {
		t.Setenv("GOOS", goos)
		t.Setenv("GOARCH", "arm64")
		t.Setenv("GOAMD64", "v4")
		t.Setenv("GOEXPERIMENT", "not_a_real_experiment")
		t.Setenv("CGO_ENABLED", "0")
		t.Setenv("GOFLAGS", "-tags=hidden_selection")
		got := authorityDebtSites(debtLoadPersistenceAuthorityFindings(t, root))
		if authorityDebtCount(got) != 1 || (before != nil && !debtSitesEqual(before, got)) {
			t.Fatalf("excluded field uncertainty changed with host %s: %+v", goos, got)
		}
		before = got
	}
}

func TestPersistenceAuthorityDebtExcludedInheritedReceiver(t *testing.T) {
	root := t.TempDir()
	debtWriteModuleSource(t, root, "go.mod", "module example.com/probe\n\ngo 1.25.0\n")
	debtWriteModuleSource(t, root, "active.go", "package probe;type handle struct{}\n")
	path := "hidden_windows.go"
	debtWriteModuleSource(t, root, path, "package probe;func(h *handle)Close(){unknownCallback(h)}\n")
	before := authorityDebtSites(debtLoadPersistenceAuthorityFindings(t, root))
	if authorityDebtCount(before) != 1 {
		t.Fatalf("inherited receiver was skipped instead of accounted: %+v", before)
	}
	debtWriteModuleSource(t, root, path, "package probe;func(h *handle)Close(){unknownCallback(h);unknownCallback(h)}\n")
	after := authorityDebtSites(debtLoadPersistenceAuthorityFindings(t, root))
	if len(authorityDebtSubset(after, before, "changed receiver body")) == 0 {
		t.Fatal("extra receiver-body work escaped uncertainty accounting")
	}
}

func TestPersistenceAuthorityDebtCensusFailsClosedOnMissingAndTypeFailedSource(t *testing.T) {
	const probe = "SWARM_DEBT_CENSUS_HOSTILE_SOURCE"
	if root := os.Getenv(probe); root != "" {
		debtLoadPersistenceAuthorityFindings(t, root)
		t.Fatal("HOSTILE_CENSUS_RETURNED_SUCCESS")
		return
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"empty", "type-error", "excluded-parse-error"} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.com/authorityprobe\n\ngo 1.25.0\n"), 0644); err != nil {
				t.Fatal(err)
			}
			if name == "type-error" {
				if err := os.WriteFile(filepath.Join(root, "broken.go"), []byte("package probe;func broken(){missingSymbol()}"), 0644); err != nil {
					t.Fatal(err)
				}
			}
			if name == "excluded-parse-error" {
				debtWriteModuleSource(t, root, "active.go", "package probe;func anchor(){}\n")
				debtWriteModuleSource(t, root, "broken_windows.go", "package probe;func broken(\n")
			}
			command := exec.Command(executable, "-test.run=^TestPersistenceAuthorityDebtCensusFailsClosedOnMissingAndTypeFailedSource$", "-test.v")
			command.Env = append(os.Environ(), probe+"="+root)
			output, err := command.CombinedOutput()
			if err == nil || strings.Contains(string(output), "HOSTILE_CENSUS_RETURNED_SUCCESS") {
				t.Fatalf("partial census accepted: %v\n%s", err, output)
			}
			if !strings.Contains(string(output), "authority census matched no packages") && !strings.Contains(string(output), "load authority packages reported type errors") && !strings.Contains(string(output), "parse inactive authority source") {
				t.Fatalf("wrong failure: %v\n%s", err, output)
			}
		})
	}
}

func TestPersistenceAuthorityDebtCensusClearsAmbientSelectionFlags(t *testing.T) {
	t.Setenv("GOFLAGS", "-tags=never -overlay=not-a-real-overlay -modfile=not-a-real-module")
	t.Setenv("GOWORK", "/foreign/workspace")
	values := map[string]string{}
	for _, entry := range debtCensusEnvironment() {
		key, value, _ := strings.Cut(entry, "=")
		values[key] = value
	}
	if values["GOFLAGS"] != "" || values["GOWORK"] != "off" {
		t.Fatal(fmt.Sprintf("ambient source selection survived: %#v", values))
	}
}

func TestPersistenceAuthorityDebtTrustedBaseDistinguishesBootstrapFromLandedMerge(t *testing.T) {
	for _, state := range []struct{ landed, deleted, push, local bool }{{false, false, false, false}, {true, false, false, false}, {true, true, false, false}, {true, false, true, false}, {true, false, false, true}} {
		landed := state.landed
		name := "initial-bootstrap-unrelated-master-change"
		if landed {
			name = "landed-baseline-merged-integration"
		}
		if state.deleted {
			name = "deleted-landed-baseline-refuses-rebootstrap"
		}
		if state.push {
			name = "master-push-compares-observed-predecessor"
		}
		if state.local {
			name = "local-master-never-self-compares"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			git := func(args ...string) string {
				t.Helper()
				out, err := debtGit(root, args...)
				if err != nil {
					t.Fatal(err)
				}
				return strings.TrimSpace(string(out))
			}
			write := func(path, text string) {
				t.Helper()
				path = filepath.Join(root, path)
				if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte(text), 0644); err != nil {
					t.Fatal(err)
				}
			}
			git("init", "-q", "-b", "master")
			git("config", "user.name", "Debt Ratchet Control")
			git("config", "user.email", "ratchet@example.invalid")
			write("seed.txt", "seed\n")
			if landed {
				write(debtBaselinePath, "landed baseline presence; schema validated by the guard\n")
			}
			git("add", ".")
			git("commit", "-qm", "test: integration base")
			fork := git("rev-parse", "HEAD")
			git("switch", "-qc", "candidate")
			write("candidate.txt", "candidate\n")
			git("add", ".")
			git("commit", "-qm", "test: candidate")
			head := git("rev-parse", "HEAD")
			git("switch", "-q", "master")
			write("unrelated.txt", "unrelated master change\n")
			if state.deleted {
				if err := os.Remove(filepath.Join(root, debtBaselinePath)); err != nil {
					t.Fatal(err)
				}
			}
			git("add", ".")
			git("commit", "-qm", "test: unrelated master change")
			target := git("rev-parse", "HEAD")
			git("merge", "-q", "--no-ff", "--no-edit", "candidate")
			merged := git("rev-parse", "HEAD")
			git("update-ref", "refs/remotes/origin/master", merged)
			eventPath := filepath.Join(t.TempDir(), "event.json")
			event := fmt.Sprintf(`{"pull_request":{"base":{"sha":%q},"head":{"sha":%q}}}`, target, head)
			if state.push {
				event = fmt.Sprintf(`{"before":%q,"after":%q}`, target, merged)
			}
			if err := os.WriteFile(eventPath, []byte(event), 0644); err != nil {
				t.Fatal(err)
			}
			t.Setenv("GITHUB_EVENT_PATH", eventPath)
			if state.local {
				t.Setenv("GITHUB_EVENT_PATH", "")
			}
			got, err := debtTrustedBase(root)
			if state.deleted {
				if err == nil || !strings.Contains(err.Error(), "rebootstrap refused") {
					t.Fatalf("deleted landed baseline did not refuse rebootstrap: base=%s error=%v", got, err)
				}
				return
			}
			want := fork
			if landed {
				want = target
			}
			if err != nil || got != want {
				t.Fatalf("trusted base=%s error=%v, want %s", got, err, want)
			}
		})
	}
}

func TestPersistenceAuthorityDebtBootstrapOriginSurvivesRebase(t *testing.T) {
	root := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		out, err := debtGit(root, args...)
		if err != nil {
			t.Fatal(err)
		}
		return strings.TrimSpace(string(out))
	}
	git("init", "-q", "-b", "master")
	git("config", "user.name", "Debt Ratchet Control")
	git("config", "user.email", "ratchet@example.invalid")
	debtWriteModuleSource(t, root, "seed.txt", "original extraction\n")
	git("add", ".")
	git("commit", "-qm", "test: origin")
	origin := git("rev-parse", "HEAD")
	debtWriteModuleSource(t, root, "seed.txt", "new master\n")
	git("add", ".")
	git("commit", "-qm", "test: master advanced")
	base := git("rev-parse", "HEAD")
	if err := debtCheckBootstrapAncestry(root, origin, base); err != nil {
		t.Fatalf("rebase rejected unchanged origin: %v", err)
	}
	git("switch", "-qc", "unrelated", origin)
	debtWriteModuleSource(t, root, "other.txt", "unrelated source\n")
	git("add", ".")
	git("commit", "-qm", "test: unrelated origin")
	if err := debtCheckBootstrapAncestry(root, git("rev-parse", "HEAD"), base); err == nil {
		t.Fatal("unrelated bootstrap source accepted")
	}
	if err := debtCheckBootstrapAncestry(root, "bad", base); err == nil {
		t.Fatal("invalid bootstrap source accepted")
	}
}

func TestPersistenceAuthorityDebtConstructionDistinguishesNativeOwnerFromRecovery(t *testing.T) {
	for _, name := range []string{"StartSQLiteRuntimeStoreWithContext", "AdmitSQLiteRuntimeStore", "replacement"} {
		pkg := types.NewPackage("github.com/division-sh/swarm/internal/store/storetest", "storetest")
		id := ast.NewIdent(name)
		info := &types.Info{Uses: map[*ast.Ident]types.Object{id: types.NewFunc(0, pkg, name, types.NewSignatureType(nil, nil, nil, nil, nil, false))}}
		call := &ast.CallExpr{Fun: id}
		got := debtOwnedNativeFixtureConstruction("internal/runtime/new_test.go", call, info)
		if got != (name == "StartSQLiteRuntimeStoreWithContext") {
			t.Fatalf("construction role name=%s owned=%v", name, got)
		}
	}
	// An uninitialized concrete literal is not a native construction operation.
	if debtOwnedNativeFixtureConstruction("internal/runtime/new_test.go", &ast.CompositeLit{}, &types.Info{}) {
		t.Fatal("concrete literal gained native-owner authority")
	}
}

func TestPersistenceAuthorityDebtRatchetIsSelectedInEveryTier(t *testing.T) {
	file, err := os.Open(filepath.Join(persistenceAuthorityRepoRoot(t), ".github/test-proof-plan.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	policy, err := testplanning.LoadPolicy(file)
	if err != nil {
		t.Fatal(err)
	}
	const unitID = "store-admission-full"
	unit, ok := policy.Units[unitID]
	if !ok || !slices.Contains(unit.Packages, "github.com/division-sh/swarm/internal/store") {
		t.Fatal("ratchet owner is not in the existing admission unit")
	}
	for _, tier := range []string{testplanning.ProfileCore, testplanning.ProfileLifecycle, testplanning.ProfileFull} {
		if !slices.Contains(policy.Profiles[tier].Units, unitID) {
			t.Fatalf("%s omits the ratchet", tier)
		}
	}
	const root = "TestPersistenceAuthorityDebtRatchet"
	if unit.Run != "" {
		selection, err := regexp.Compile(unit.Run)
		if err != nil || !selection.MatchString(root) {
			t.Fatal("admission selector omits ratchet")
		}
	}
	if unit.Skip != "" {
		skip, err := regexp.Compile(unit.Skip)
		if err != nil || skip.MatchString(root) {
			t.Fatal("admission selector skips ratchet")
		}
	}
}
