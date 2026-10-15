package store_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// #2495 ruling 6114916081: seven native constructor occurrences move only
// declaration identity. Eight retained occurrences earn no deletion credit.
func debtProcessLeaseRelocations() [7][2]authorityDebtSite {
	const native = "*github.com/division-sh/swarm/internal/store/internal/runtimepersistence.PostgresStore"
	const tuple = "(*github.com/division-sh/swarm/internal/store/internal/runtimepersistence.SQLiteRuntimeStore, error)"
	rows := [7][3]string{
		{"internal/store/construction/open.go", "OpenSQLiteRuntimeWithOwnershipBinding", "openSQLiteUnderPossession"},
		{"internal/store/internal/runtimepersistence/runtime_startup_ownership_test.go", "TestProcessCapabilitySuccessorClassificationParity", "TestProcessCapabilityGracefulAndCrashSuccessionParity"},
		{"internal/store/internal/runtimepersistence/runtime_startup_ownership_test.go", "TestProcessCapabilityLiveOwnerRefusalParity", "TestProcessCapabilityLiveOwnerWaitPreservesGenerationParity"},
		{"internal/store/internal/runtimepersistence/runtime_startup_ownership_test.go", "TestAuthorityInspectionRejectsSynchronizedBindingForgeryParity", "TestProcessCapabilityRejectsChangedCanonicalStampParity"},
		{"internal/store/internal/runtimepersistence/runtime_startup_ownership_test.go", "TestProcessAuthorityDiagnosticTimestampDoesNotInvalidateHeadParity", "TestProcessGenerationIgnoresDiagnosticMetadataClockParity"},
		{"internal/store/internal/runtimepersistence/runtime_startup_ownership_test.go", "TestAuthorityRepairParity", "TestProcessGenerationMissingRefusesReseedingParity"},
		{"internal/store/internal/runtimepersistence/runtime_startup_ownership_test.go", "TestAuthorityRepairRefusesProvenLiveOwnerParity", "TestProcessInspectionGrantsNoServingAuthorityParity"},
	}
	var pairs [7][2]authorityDebtSite
	for i, row := range rows {
		resolved, count := native, 1
		if i == 0 {
			resolved = tuple
		} else if i == 1 {
			count = 2
		}
		old := authorityDebtSite{Kind: "selected-store-construction", File: row[0], Declaration: row[1], Operation: "construction:" + resolved,
			Resolved: resolved, Family: "authority-carrier", Replacement: "original selected construction/work-lifetime owner; no getter or transaction-context recovery", Multiplicity: count}
		current := old
		current.Declaration = row[2]
		pairs[i] = [2]authorityDebtSite{old, current}
	}
	return pairs
}

func debtProcessLeaseRelocationComparisons(actual, head, base, source map[string]authorityDebtSite) (map[string]authorityDebtSite, map[string]authorityDebtSite, map[string]authorityDebtSite, []string) {
	for _, pair := range debtProcessLeaseRelocations() {
		old, current := pair[0], pair[1]
		for _, state := range []map[string]authorityDebtSite{actual, head, base, source} {
			if state[old.identity()].Multiplicity > 0 && state[current.identity()].Multiplicity > 0 {
				return head, base, source, []string{"both reviewed process-lease constructor identities survive: " + old.Declaration}
			}
		}
		if actual[current.identity()] == current && actual[old.identity()].Multiplicity == 0 &&
			base[old.identity()] == old && base[current.identity()].Multiplicity == 0 &&
			source[old.identity()] == old && source[current.identity()].Multiplicity == 0 {
			head = debtForwardQuiescenceUncertainty(head, old, current)
			base = debtForwardQuiescenceUncertainty(base, old, current)
			source = debtForwardQuiescenceUncertainty(source, old, current)
		}
	}
	return head, base, source, nil
}

func debtProcessLeaseDeclarationDigest(root, path, declaration string) (string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, path), nil, parser.AllErrors)
	if err != nil {
		return "", err
	}
	for _, node := range file.Decls {
		if fn, ok := node.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.Name == declaration {
			var canonical bytes.Buffer
			if err := format.Node(&canonical, token.NewFileSet(), fn); err != nil {
				return "", err
			}
			digest := sha256.Sum256(canonical.Bytes())
			return hex.EncodeToString(digest[:]), nil
		}
	}
	return "", fmt.Errorf("missing reviewed process-lease declaration %s:%s", path, declaration)
}

func debtValidateProcessLeaseRelocationSource(root, baseRoot string, actual, source map[string]authorityDebtSite) error {
	for i, pair := range debtProcessLeaseRelocations() {
		old, current := pair[0], pair[1]
		if actual[current.identity()] != current || source[old.identity()] != old {
			continue // Deletion and already-landed successors need no substitution.
		}
		for j, tree := range []string{baseRoot, root} {
			digest, err := debtProcessLeaseDeclarationDigest(tree, pair[j].File, pair[j].Declaration)
			if err != nil || digest != debtProcessLeaseSourcePins[i][j] {
				return fmt.Errorf("reviewed process-lease constructor provenance changed: %s:%s digest=%s: %v", pair[j].File, pair[j].Declaration, digest, err)
			}
		}
	}
	return nil
}

// All methods, including new files, participate. Pinning only the carrier's
// declaration would miss a newly added raw getter or custody-forwarding method.
func debtProcessLeaseCustodyDigest(root string) (string, error) {
	const dir = "internal/store/internal/startupownership"
	entries, err := os.ReadDir(filepath.Join(root, dir))
	if err != nil {
		return "", err
	}
	var parts []string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}
		path := dir + "/" + entry.Name()
		file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, path), nil, parser.AllErrors)
		if err != nil {
			return "", err
		}
		for _, node := range file.Decls {
			owned := false
			switch decl := node.(type) {
			case *ast.GenDecl:
				for _, spec := range decl.Specs {
					if named, ok := spec.(*ast.TypeSpec); ok && named.Name.Name == "StartupPostgresOwner" {
						owned = true
					}
				}
			case *ast.FuncDecl:
				owned = decl.Recv == nil && (decl.Name.Name == "NewPostgres" || decl.Name.Name == "RetainPostgresConstructionPossession")
				if decl.Recv != nil {
					for _, field := range decl.Recv.List {
						typ := field.Type
						if pointer, ok := typ.(*ast.StarExpr); ok {
							typ = pointer.X
						}
						if named, ok := typ.(*ast.Ident); ok && named.Name == "StartupPostgresOwner" {
							owned = true
						}
					}
				}
			}
			if owned {
				var canonical bytes.Buffer
				if err := format.Node(&canonical, token.NewFileSet(), node); err != nil {
					return "", err
				}
				parts = append(parts, path+"\x00"+canonical.String())
			}
		}
	}
	sort.Strings(parts)
	digest := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(digest[:]), nil
}

func debtProcessLeaseCustodyFinding(finding authorityFinding) bool {
	if finding.Kind != "raw-authority-export" || !finding.RawSQL || finding.File != "internal/store/internal/startupownership/owner.go" {
		return false
	}
	const owner = "github.com/division-sh/swarm/internal/store/internal/startupownership.StartupPostgresOwner"
	return finding.Enclosing == "StartupPostgresOwner" && finding.Member == "exported-type" && finding.Resolved == owner ||
		finding.Enclosing == "NewPostgres" && finding.Member == "result:#1" && finding.Resolved == "*"+owner
}

func debtClassifyProcessLeaseCustody(root string, findings []authorityFinding) {
	digest, err := debtProcessLeaseCustodyDigest(root)
	if err != nil || digest != debtProcessLeaseCustodyPin {
		return // Unproven/changed custody remains visible, unclassified debt.
	}
	for i := range findings {
		if debtProcessLeaseCustodyFinding(findings[i]) {
			findings[i].Disposition = "native-process-lease-custody:" + digest
		}
	}
}

const debtProcessLeaseCustodyPin = "8aa1f9f4119f9a72056b3a4a0ebde68c462b6b2e8b35f381438ce656815c8560"

var debtProcessLeaseSourcePins = [7][2]string{
	{"7e75914657c1f1b2f76fccc721dec583214956a8a26921f419a076bd56df19c7", "e9bc335965128cb3d6d4d9f73e9f733150894148b5f694b594b53efab6a90d8e"},
	{"40be79e11ee98d40b8b64acc152b023b3a01fc73e7fdc6694c0a96a2cef33720", "afe0553d53b2e693837a134268c94dcac59a9d0f6568913017e96cbc84eeefa4"},
	{"8fcb4efbd9880cb92c3f5be39540096ec311d383a7181dc7b4ea16eb7c44fd34", "fb42e96f78e2eb05a50ccf5d0919285fe10ead0ef17953299a33c8e7d1f6295a"},
	{"de38f76bc4296315043723c21b7ae73517f9ef0ce08102bbe692acf7cb6381a0", "4884a7818f3dc811ca2960e384af697e31bab9d9bb15ca03bdfa68ea4c77960c"},
	{"93ec5e7562cb603f59fa91267aabb9f16849ced110c6ae864ad8150d2dc0e9db", "aaa05bde472a17fb7d4ff7f660b42faf63f88f60040c19ed6926c55ad59dc924"},
	{"184ca505542b2a7a4d09140e478e3b5413264442c78ca71ebc5291b0d4ee6402", "b1721a00a89d797be389d3d58b31eb32e31f4565caba39bb337d1fd20d2392b0"},
	{"e0c78cd4130361a71acf9a4244cd76ced448bd33562ccfcfba0d7fed61f26450", "1683d1e8cadd288bcff7f9edee09e94385b003c739ea114c938946381b5395ca"},
}
