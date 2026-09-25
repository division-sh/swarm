package testplanning

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/division-sh/swarm/internal/testchanged"
	"gopkg.in/yaml.v3"
)

// BuildContext is the effective Go selection context, not just shell variables.
type BuildContext struct {
	GOOS       string `json:"goos"`
	GOARCH     string `json:"goarch"`
	CGOEnabled string `json:"cgo_enabled"`
	GOWORK     string `json:"gowork"`
}

type TestRoot struct {
	Package string `json:"package"`
	Name    string `json:"name"`
}

type RequiredTest struct {
	TestRoot
	Children []string `json:"children,omitempty"`
}

type PackageRoots struct {
	Package      string
	HasTestFiles bool
	Roots        []string
}

type RootInventory struct {
	BuildContext   BuildContext
	Packages       map[string]PackageRoots
	ImpactPackages []testchanged.Package
}

type goListPackage struct {
	Dir          string
	ImportPath   string
	Imports      []string
	TestImports  []string
	XTestImports []string
	TestGoFiles  []string
	XTestGoFiles []string
}

func EffectiveBuildContext(ctx context.Context, dir string) (BuildContext, error) {
	command := exec.CommandContext(ctx, "go", "env", "-json", "GOFLAGS", "GOOS", "GOARCH", "CGO_ENABLED", "GOWORK")
	command.Dir = dir
	raw, err := command.Output()
	if err != nil {
		return BuildContext{}, fmt.Errorf("read effective Go environment: %w", err)
	}
	var env struct {
		GOFLAGS string
		BuildContext
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return BuildContext{}, fmt.Errorf("decode effective Go environment: %w", err)
	}
	if strings.TrimSpace(env.GOFLAGS) != "" {
		return BuildContext{}, fmt.Errorf("completion proof rejects effective GOFLAGS=%q; unset shell and go env -w flags", env.GOFLAGS)
	}
	if env.GOOS == "" || env.GOARCH == "" || env.CGOEnabled == "" {
		return BuildContext{}, fmt.Errorf("incomplete effective Go build context: %+v", env.BuildContext)
	}
	return env.BuildContext, nil
}

func DiscoverRootInventory(ctx context.Context, dir string) (RootInventory, error) {
	root, err := filepath.Abs(dir)
	if err != nil {
		return RootInventory{}, err
	}
	build, err := EffectiveBuildContext(ctx, dir)
	if err != nil {
		return RootInventory{}, err
	}
	command := exec.CommandContext(ctx, "go", "list", "-json", "./...")
	command.Dir = dir
	raw, err := command.Output()
	if err != nil {
		return RootInventory{}, fmt.Errorf("discover active Go test files: %w", err)
	}
	inventory := RootInventory{BuildContext: build, Packages: map[string]PackageRoots{}}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	for {
		var pkg goListPackage
		if err := decoder.Decode(&pkg); err != nil {
			if err == io.EOF {
				break
			}
			return RootInventory{}, fmt.Errorf("decode active Go package: %w", err)
		}
		files := append(append([]string{}, pkg.TestGoFiles...), pkg.XTestGoFiles...)
		roots := map[string]bool{}
		for _, name := range files {
			file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(pkg.Dir, name), nil, 0)
			if err != nil {
				return RootInventory{}, fmt.Errorf("parse active test file %s: %w", name, err)
			}
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if ok && executableTestRoot(fn) {
					roots[fn.Name.Name] = true
				}
			}
		}
		entry := PackageRoots{Package: pkg.ImportPath, HasTestFiles: len(files) > 0}
		for name := range roots {
			entry.Roots = append(entry.Roots, name)
		}
		sort.Strings(entry.Roots)
		inventory.Packages[pkg.ImportPath] = entry
		rel, err := filepath.Rel(root, pkg.Dir)
		if err != nil {
			return RootInventory{}, err
		}
		inventory.ImpactPackages = append(inventory.ImpactPackages, testchanged.Package{
			ImportPath: pkg.ImportPath, Dir: pkg.Dir, RelDir: filepath.ToSlash(rel),
			Imports: pkg.Imports, TestImports: pkg.TestImports, XTestImports: pkg.XTestImports,
		})
	}
	if len(inventory.Packages) == 0 {
		return RootInventory{}, fmt.Errorf("active Go package inventory is empty")
	}
	return inventory, nil
}

func executableTestRoot(fn *ast.FuncDecl) bool {
	if fn.Recv != nil || fn.Name == nil || fn.Type == nil || fn.Type.TypeParams != nil {
		return false
	}
	name := fn.Name.Name
	if name == "TestMain" || fn.Type.Results != nil && len(fn.Type.Results.List) != 0 {
		return false
	}
	for _, prefix := range []string{"Test", "Fuzz"} {
		if strings.HasPrefix(name, prefix) {
			suffix, _ := utf8.DecodeRuneInString(strings.TrimPrefix(name, prefix))
			if unicode.IsLower(suffix) {
				return false
			}
		}
	}
	if strings.HasPrefix(name, "Example") && fn.Type.Params != nil && len(fn.Type.Params.List) == 0 {
		return true
	}
	kind := "T"
	if strings.HasPrefix(name, "Fuzz") {
		kind = "F"
	} else if !strings.HasPrefix(name, "Test") {
		return false
	}
	if fn.Type.Params == nil || len(fn.Type.Params.List) != 1 {
		return false
	}
	field := fn.Type.Params.List[0]
	if len(field.Names) != 1 {
		return false
	}
	star, ok := field.Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	sel, ok := star.X.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != kind {
		return false
	}
	ident, ok := sel.X.(*ast.Ident)
	return ok && ident.Name == "testing"
}

type ParityProof struct {
	ID       string   `yaml:"id"`
	Kind     string   `yaml:"kind"`
	Name     string   `yaml:"name"`
	Path     string   `yaml:"path"`
	Profile  string   `yaml:"profile"`
	Backends []string `yaml:"backends"`
	Children []string `yaml:"required_children"`
}

func LoadParityProofs(path string) ([]ParityProof, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var ledger struct {
		StoreParity struct {
			ProofCatalog []ParityProof `yaml:"proof_catalog"`
		} `yaml:"store_parity"`
	}
	if err := yaml.Unmarshal(raw, &ledger); err != nil {
		return nil, err
	}
	if len(ledger.StoreParity.ProofCatalog) == 0 {
		return nil, fmt.Errorf("parity proof catalog is empty")
	}
	return ledger.StoreParity.ProofCatalog, nil
}

// BindExecution turns the selected source roots and existing parity references
// into the exact, digest-bound pre-run completion obligations.
func BindExecution(plan *RunPlan, inventory RootInventory, parity []ParityProof) error {
	if plan == nil || len(inventory.Packages) == 0 || inventory.BuildContext.GOOS == "" {
		return fmt.Errorf("cannot bind execution without a plan and active build inventory")
	}
	if plan.Profile == ProfileNightly {
		backends := map[string]bool{}
		for _, unit := range plan.Units {
			if backend, ok := SoakBackend(unit.Run); ok {
				backends[backend] = true
			}
		}
		if !backends["sqlite"] || !backends["postgres"] {
			return fmt.Errorf("nightly plan requires both soak cells")
		}
	}
	required := map[string]RequiredTest{}
	parityByName := map[string]ParityProof{}
	addRequired := func(pkg, name string, children ...string) {
		key := pkg + "\x00" + name
		test := required[key]
		test.TestRoot = TestRoot{Package: pkg, Name: name}
		test.Children = append(test.Children, children...)
		required[key] = test
	}
	for _, proof := range parity {
		if proof.Kind != "go_test" {
			continue
		}
		parityByName[proof.Name] = proof
		if plan.Profile == ProfileLocal || proof.Profile == ProfileFull && (plan.Profile == ProfilePRCommon || plan.Profile == ProfilePREscalated) {
			continue
		}
		if proof.Profile != ProfileFull && proof.Profile != ProfilePRCommon {
			return fmt.Errorf("parity proof %s has unsupported profile %s", proof.ID, proof.Profile)
		}
		pkg := strings.TrimSuffix(filepath.ToSlash(filepath.Dir(proof.Path)), "/")
		addRequired(pkgFromPath(pkg, plan), proof.Name, proof.Children...)
	}
	if plan.Profile == ProfileLocal {
		for _, unit := range plan.Units {
			if unit.ID == "catalog-required-inventory" || strings.HasPrefix(unit.ID, "local-") {
				for _, pkg := range unit.Packages {
					entry, ok := inventory.Packages[pkg]
					if !ok {
						return fmt.Errorf("local unit %s package %s absent from active inventory", unit.ID, pkg)
					}
					for _, name := range entry.Roots {
						matched, err := selectedByUnit(unit, name)
						if err != nil {
							return err
						}
						if matched {
							addRequired(pkg, name)
						}
					}
				}
			}
		}
	} else {
		const release = "github.com/division-sh/swarm/internal/releasee2e"
		if plan.Profile == ProfilePRCommon || plan.Profile == ProfilePREscalated {
			addRequired(release, "TestGoldenAgentWorkloadSQLiteSmoke")
			addRequired(release, "TestCompiledProcessFullLifecycleSQLiteSmoke")
		} else {
			for _, name := range []string{"TestGoldenAgentWorkloadRestartAndForcedKillOnBothBackends", "TestGoldenAgentWorkloadBurstConcurrencyOnBothBackendsIteration1", "TestGoldenAgentWorkloadBurstConcurrencyOnBothBackendsIteration2", "TestCompiledProcessFullLifecycleJourneysSQLitePostgres"} {
				addRequired(release, name)
			}
		}
	}
	for i := range plan.Units {
		unit := &plan.Units[i]
		unit.SelectedRoots = nil
		unit.TestBearingPackages = nil
		unit.RequiredTests = nil
		for _, pkg := range unit.Packages {
			entry, ok := inventory.Packages[pkg]
			if !ok {
				return fmt.Errorf("unit %s package %s absent from active inventory", unit.ID, pkg)
			}
			if entry.HasTestFiles {
				unit.TestBearingPackages = append(unit.TestBearingPackages, pkg)
			}
			for _, name := range entry.Roots {
				match, err := selectedByUnit(*unit, name)
				if err != nil {
					return err
				}
				if !match {
					continue
				}
				unit.SelectedRoots = append(unit.SelectedRoots, TestRoot{Package: pkg, Name: name})
				if obligation, ok := required[pkg+"\x00"+name]; ok {
					obligation.Children = append(obligation.Children, unit.RequiredChildren[name]...)
					sort.Strings(obligation.Children)
					obligation.Children = compactStrings(obligation.Children)
					unit.RequiredTests = append(unit.RequiredTests, obligation)
				}
			}
		}
		if len(unit.SelectedRoots) == 0 && len(unit.TestBearingPackages) > 0 {
			return fmt.Errorf("unit %s selects no active test roots", unit.ID)
		}
		if strings.HasPrefix(unit.ID, "parity-") {
			for _, root := range unit.SelectedRoots {
				proof, ok := parityByName[root.Name]
				if !ok || proof.Profile != ProfileFull {
					return fmt.Errorf("supplement %s selects root %s absent from full parity catalog", unit.ID, root.Name)
				}
				addRequired(root.Package, root.Name, proof.Children...)
				unit.RequiredTests = append(unit.RequiredTests, required[root.Package+"\x00"+root.Name])
			}
		}
		if backend, soak := SoakBackend(unit.Run); soak {
			unit.RequiredTests = append(unit.RequiredTests, RequiredTest{
				TestRoot: TestRoot{Package: SoakPackage, Name: SoakTest}, Children: []string{backend},
			})
		}
		for name := range unit.RequiredChildren {
			found := false
			for _, root := range unit.SelectedRoots {
				if root.Name == name {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("unit %s declares children for unselected root %s", unit.ID, name)
			}
		}
	}
	for key, obligation := range required {
		found := false
		for _, unit := range plan.Units {
			for _, selected := range unit.SelectedRoots {
				if selected.Package+"\x00"+selected.Name == key {
					found = true
					break
				}
			}
		}
		if !found {
			return fmt.Errorf("required proof %s.%s has no selected unit", obligation.Package, obligation.Name)
		}
	}
	if err := validateRootSelection(plan, inventory); err != nil {
		return err
	}
	plan.BuildContext = inventory.BuildContext
	digest, err := planDigest(*plan)
	if err != nil {
		return err
	}
	plan.Digest = digest
	return plan.Validate()
}

func compactStrings(values []string) []string {
	if len(values) < 2 {
		return values
	}
	end := 1
	for _, value := range values[1:] {
		if value != values[end-1] {
			values[end] = value
			end++
		}
	}
	return values[:end]
}

func validateRootSelection(plan *RunPlan, inventory RootInventory) error {
	owners := map[string][]string{}
	for _, unit := range plan.Units {
		if strings.HasPrefix(unit.ID, "parity-") {
			continue // supplemental evidence is not an alternate partition owner
		}
		for _, root := range unit.SelectedRoots {
			key := root.Package + "\x00" + root.Name
			owners[key] = append(owners[key], unit.ID)
		}
	}
	const catalog = "github.com/division-sh/swarm/internal/testcatalog"
	for _, pkg := range plan.Packages {
		entry := inventory.Packages[pkg]
		for _, name := range entry.Roots {
			if plan.Profile == ProfileLocal && pkg != catalog && strings.Contains(pkg, "/internal/") {
				// Local special packages are intentionally sparse; broad packages
				// are still complete and checked below.
				isSpecial := false
				for _, unit := range plan.Units {
					if unit.BudgetClass != "broad" && len(unit.Packages) == 1 && unit.Packages[0] == pkg {
						isSpecial = true
					}
				}
				if isSpecial {
					continue
				}
			}
			if (plan.Profile == ProfilePRCommon || plan.Profile == ProfilePREscalated) && pkg == "github.com/division-sh/swarm/internal/runtime/cataloge2e" && plan.Profile == ProfilePRCommon && name != "TestCatalogRequiredSmoke" {
				continue
			}
			count := len(owners[pkg+"\x00"+name])
			if name == SoakTest {
				if count != 0 && count != 2 {
					return fmt.Errorf("soak proof %s.%s has %d owners, want 0 or exact backend pair", pkg, name, count)
				}
				if plan.Profile == ProfileNightly && count != 2 {
					return fmt.Errorf("nightly soak proof %s.%s is absent", pkg, name)
				}
			} else if count != 1 {
				return fmt.Errorf("active proof %s.%s has %d primary owners, want 1", pkg, name, count)
			}
		}
	}
	return nil
}

func pkgFromPath(path string, plan *RunPlan) string {
	for _, pkg := range plan.Packages {
		if strings.HasSuffix(pkg, "/"+path) {
			return pkg
		}
	}
	return path
}

func selectedByUnit(unit ProofUnit, name string) (bool, error) {
	run := strings.Split(unit.Run, "/")[0]
	if run != "" {
		matched, err := regexp.MatchString(run, name)
		if err != nil {
			return false, err
		}
		if !matched {
			return false, nil
		}
	}
	if unit.Skip != "" {
		skipped, err := regexp.MatchString(unit.Skip, name)
		if err != nil {
			return false, err
		}
		if skipped {
			return false, nil
		}
	}
	return true, nil
}
