package contracts

import (
	"fmt"
	"go/ast"
	"go/types"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

type handlerRuleDisplayLabelReader struct {
	Path      string
	Enclosing string
}

func (r handlerRuleDisplayLabelReader) key() string {
	return r.Path + "::" + r.Enclosing
}

type handlerRuleDisplayLabelAllowance struct {
	Count  int
	Reason string
}

func TestHandlerRuleDisplayLabelReaderBoundary(t *testing.T) {
	transitionKey := "internal/runtime/contracts/workflow_contract_semantics.go::deriveRuleTransitions"
	if allowance := allowedHandlerRuleDisplayLabelReaders()[transitionKey]; !strings.Contains(allowance.Reason, "#1769/#1775") {
		t.Fatalf("transition-lowering display authority is not explicitly tracked: %#v", allowance)
	}
	for _, activityKey := range []string{
		"internal/runtime/contracts/workflow_contract_activity.go::ActivitySitesForNode",
		"internal/runtime/engine/executor.go::(*Executor).stepActivity",
	} {
		allowance := allowedHandlerRuleDisplayLabelReaders()[activityKey]
		if !strings.Contains(allowance.Reason, "ActivitySite.RuleID -> DefaultActivityID -> generated event identity") || !strings.Contains(allowance.Reason, "#1769/#1775") {
			t.Fatalf("activity display authority is not truthfully classified: %s %#v", activityKey, allowance)
		}
	}
	readers := loadHandlerRuleDisplayLabelReaders(t, nil)
	got := map[string]int{}
	for _, reader := range readers {
		got[reader.key()]++
	}
	var problems []string
	for key, count := range got {
		allowance, ok := allowedHandlerRuleDisplayLabelReaders()[key]
		if !ok {
			problems = append(problems, fmt.Sprintf("%s count=%d (unclassified)", key, count))
			continue
		}
		if count != allowance.Count {
			problems = append(problems, fmt.Sprintf("%s count=%d want=%d", key, count, allowance.Count))
		}
	}
	for key := range allowedHandlerRuleDisplayLabelReaders() {
		if allowedHandlerRuleDisplayLabelReaders()[key].Reason == "" {
			problems = append(problems, key+" (missing classification reason)")
		}
		if got[key] == 0 {
			problems = append(problems, key+" (missing)")
		}
	}
	sort.Strings(problems)
	if len(problems) > 0 {
		t.Fatalf("HandlerRuleEntry.ID reader boundary changed:\n%s\nThe field is presentation-only except the explicitly deferred transition-lowering seam; use ContractElementRef for semantic identity.", strings.Join(problems, "\n"))
	}
}

func TestHandlerRuleDisplayLabelReaderBoundaryRejectsHostileSemanticUse(t *testing.T) {
	root := handlerRuleIdentityGuardRepoRoot(t)
	path := filepath.Join(root, "internal", "runtime", "contracts", "handler_rule_identity_guard_hostile.go")
	overlay := map[string][]byte{path: []byte(`package contracts

import "strings"

func hostileRuleLabelAuthority(arbitraryReceiver HandlerRuleEntry, sibling HandlerRuleEntry) bool {
    return strings.TrimSpace(arbitraryReceiver.ID) == strings.TrimSpace(sibling.ID)
}
`)}
	readers := loadHandlerRuleDisplayLabelReaders(t, overlay)
	count := 0
	for _, reader := range readers {
		if reader.Enclosing == "hostileRuleLabelAuthority" {
			count++
			if _, allowed := allowedHandlerRuleDisplayLabelReaders()[reader.key()]; allowed {
				t.Fatalf("hostile semantic reader unexpectedly allowlisted: %s", reader.key())
			}
		}
	}
	if count != 2 {
		t.Fatalf("typed guard detected %d hostile HandlerRuleEntry.ID readers, want 2: %#v", count, readers)
	}
}

func allowedHandlerRuleDisplayLabelReaders() map[string]handlerRuleDisplayLabelAllowance {
	return map[string]handlerRuleDisplayLabelAllowance{
		"internal/runtime/activity_validation.go::validateHandlerActivitySurface":                                                  {Count: 3, Reason: "diagnostic rule label"},
		"internal/runtime/authoringview/view.go::containedOperationRefs":                                                           {Count: 2, Reason: "authoring presentation label"},
		"internal/runtime/bootverify/workflow_compute_module_checks.go::checkComputeModuleValueRows":                               {Count: 1, Reason: "diagnostic row label"},
		"internal/runtime/bootverify/workflow_contained_state_operation_checks.go::wave1ContainedStateOperations":                  {Count: 2, Reason: "diagnostic operation label"},
		"internal/runtime/bootverify/workflow_entity_contract_coverage_checks.go::wave1HandlerWriteTargets":                        {Count: 2, Reason: "diagnostic write-site label"},
		"internal/runtime/bootverify/workflow_executable_reader_census.go::appendRulesExecutableReaders":                           {Count: 1, Reason: "diagnostic reader label"},
		"internal/runtime/bootverify/workflow_handler_contract_checks.go::(*checkerContext).handlerFieldCompliance":                {Count: 1, Reason: "diagnostic rule label"},
		"internal/runtime/bootverify/workflow_handler_contract_checks.go::rejectUnsupportedRuleActions":                            {Count: 1, Reason: "diagnostic rule label"},
		"internal/runtime/bootverify/workflow_input_alignment_checks.go::payloadFieldCoverageSites":                                {Count: 2, Reason: "diagnostic payload-site label"},
		"internal/runtime/bootverify/workflow_policy_sheet_lookup_checks.go::checkPolicySheetLookupValueRows":                      {Count: 1, Reason: "diagnostic row label"},
		"internal/runtime/bootverify/workflow_policy_sheet_lookup_checks.go::policySheetLookupDuplicateComputedBindings":           {Count: 1, Reason: "diagnostic row label"},
		"internal/runtime/bootverify/workflow_policy_sheet_validation_checks.go::checkPolicySheetValidationValueRows":              {Count: 1, Reason: "diagnostic row label"},
		"internal/runtime/bootverify/workflow_policy_sheet_validation_checks.go::validatePolicySheetValidationDispositionConsumer": {Count: 1, Reason: "diagnostic consumer label; self-exclusion uses RuleIndex"},
		"internal/runtime/contracts/compute_module_validation.go::validatePolicySheetComputeModuleRows":                            {Count: 1, Reason: "diagnostic row label"},
		"internal/runtime/contracts/validation_policy_validation.go::validatePolicySheetValidationRows":                            {Count: 1, Reason: "diagnostic row label"},
		"internal/runtime/contracts/workflow_contract_activity.go::ActivitySitesForNode":                                           {Count: 1, Reason: "deferred activity identity authority: HandlerRuleEntry.ID -> ActivitySite.RuleID -> DefaultActivityID -> generated event identity; tracked by #1769/#1775"},
		"internal/runtime/contracts/workflow_contract_emit.go::HandlerDeclarativeEmitSites":                                        {Count: 6, Reason: "emit-site presentation label"},
		"internal/runtime/contracts/workflow_contract_emit.go::HandlerRuleEmitTemplateSites":                                       {Count: 1, Reason: "emit-template presentation label"},
		"internal/runtime/contracts/workflow_contract_policy_sheet.go::lowerPolicySheetRuleNode":                                   {Count: 9, Reason: "decode diagnostics and presentation metadata"},
		"internal/runtime/contracts/workflow_contract_rule_identity.go::QualifySystemNodeHandlerRuleRefs":                          {Count: 2, Reason: "identity-admission diagnostics only"},
		"internal/runtime/contracts/workflow_contract_semantics.go::deriveRuleTransitions":                                         {Count: 2, Reason: "deferred transition identity authority tracked by #1769/#1775"},
		"internal/runtime/contracts/workflow_contract_yaml_handlers.go::decodeHandlerRuleEntriesNode":                              {Count: 2, Reason: "keyed-map presentation projection"},
		"internal/runtime/contracts/workflow_contract_yaml_handlers.go::decodeHandlerRuleEntryNode":                                {Count: 1, Reason: "non-empty presentation field check"},
		"internal/runtime/contracts/workflow_fan_out_semantics.go::HandlerFanOutSites":                                             {Count: 2, Reason: "fan-out-site presentation label"},
		"internal/runtime/contracts/workflow_transition_carriers.go::handlerAdvanceCarriers":                                       {Count: 1, Reason: "transition diagnostic carrier label"},
		"internal/runtime/engine/executor.go::(*Executor).applyRule":                                                               {Count: 2, Reason: "durable fact presentation label"},
		"internal/runtime/engine/executor.go::(*Executor).selectRule":                                                              {Count: 2, Reason: "failed-evaluation fact and diagnostic presentation label"},
		"internal/runtime/engine/executor.go::(*Executor).stepActivity":                                                            {Count: 1, Reason: "deferred activity identity authority: HandlerRuleEntry.ID -> ActivitySite.RuleID -> DefaultActivityID -> generated event identity; tracked by #1769/#1775"},
		"internal/runtime/engine/executor.go::validateUnsupportedRuleActions":                                                      {Count: 1, Reason: "diagnostic rule label"},
	}
}

func loadHandlerRuleDisplayLabelReaders(t *testing.T, overlay map[string][]byte) []handlerRuleDisplayLabelReader {
	t.Helper()
	root := handlerRuleIdentityGuardRepoRoot(t)
	cfg := &packages.Config{
		Dir: root,
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles |
			packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports,
		Tests:   false,
		Overlay: overlay,
	}
	pkgs, err := packages.Load(cfg, "./internal/runtime/...", "./internal/store/...", "./internal/operatorread/...")
	if err != nil {
		t.Fatalf("load handler rule identity ownership packages: %v", err)
	}
	if packages.PrintErrors(pkgs) > 0 {
		t.Fatal("load handler rule identity ownership packages reported type errors")
	}
	var readers []handlerRuleDisplayLabelReader
	for _, pkg := range pkgs {
		for index, file := range pkg.Syntax {
			if index >= len(pkg.CompiledGoFiles) || strings.HasSuffix(pkg.CompiledGoFiles[index], "_test.go") {
				continue
			}
			relative, err := filepath.Rel(root, pkg.CompiledGoFiles[index])
			if err != nil {
				t.Fatal(err)
			}
			readers = append(readers, collectHandlerRuleDisplayLabelReaders(filepath.ToSlash(relative), file, pkg.TypesInfo)...)
		}
	}
	sort.Slice(readers, func(i, j int) bool { return readers[i].key() < readers[j].key() })
	return readers
}

func collectHandlerRuleDisplayLabelReaders(path string, file *ast.File, info *types.Info) []handlerRuleDisplayLabelReader {
	var readers []handlerRuleDisplayLabelReader
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Body == nil {
			continue
		}
		enclosing := handlerRuleGuardFunctionName(function)
		ast.Inspect(function.Body, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if !ok || !isHandlerRuleDisplayLabelField(selector, info) {
				return true
			}
			readers = append(readers, handlerRuleDisplayLabelReader{Path: path, Enclosing: enclosing})
			return true
		})
	}
	return readers
}

func isHandlerRuleDisplayLabelField(selector *ast.SelectorExpr, info *types.Info) bool {
	if selector == nil || selector.Sel == nil || selector.Sel.Name != "ID" || info == nil {
		return false
	}
	selection := info.Selections[selector]
	if selection == nil || selection.Kind() != types.FieldVal {
		return false
	}
	receiver := types.Unalias(selection.Recv())
	if pointer, ok := receiver.(*types.Pointer); ok {
		receiver = types.Unalias(pointer.Elem())
	}
	named, ok := receiver.(*types.Named)
	return ok && named.Obj() != nil && named.Obj().Pkg() != nil &&
		named.Obj().Pkg().Path() == "github.com/division-sh/swarm/internal/runtime/contracts" &&
		named.Obj().Name() == "HandlerRuleEntry"
}

func handlerRuleGuardFunctionName(function *ast.FuncDecl) string {
	if function.Recv == nil || len(function.Recv.List) == 0 {
		return function.Name.Name
	}
	receiver := function.Recv.List[0].Type
	switch typed := receiver.(type) {
	case *ast.Ident:
		return "(" + typed.Name + ")." + function.Name.Name
	case *ast.StarExpr:
		if name, ok := typed.X.(*ast.Ident); ok {
			return "(*" + name.Name + ")." + function.Name.Name
		}
	}
	return function.Name.Name
}

func handlerRuleIdentityGuardRepoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}
