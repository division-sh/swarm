package store

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const selectedStoreAbstractionGuardMatrixPath = "internal/store/testdata/selected_store_abstraction_guard_matrix.yaml"

type selectedStoreAbstractionGuardMatrix struct {
	Version     int                                       `yaml:"version"`
	Kind        string                                    `yaml:"kind"`
	Issue       int                                       `yaml:"issue"`
	ParentIssue int                                       `yaml:"parent_issue"`
	Policy      selectedStoreAbstractionGuardMatrixPolicy `yaml:"policy"`
	Rows        []selectedStoreAbstractionGuardMatrixRow  `yaml:"rows"`
}

type selectedStoreAbstractionGuardMatrixPolicy struct {
	ClosureLevel                string `yaml:"closure_level"`
	ClaimsParentClosure         bool   `yaml:"claims_parent_closure"`
	RequiredSmokePolicy         string `yaml:"required_smoke_policy"`
	NamedFastCommand            string `yaml:"named_fast_command"`
	NamedFullConformanceCommand string `yaml:"named_full_conformance_command"`
}

type selectedStoreAbstractionGuardMatrixRow struct {
	ID             string                             `yaml:"id"`
	Classification string                             `yaml:"classification"`
	Tier           string                             `yaml:"tier"`
	Concepts       []string                           `yaml:"concepts"`
	Owners         []string                           `yaml:"owners"`
	SplitIssues    []int                              `yaml:"split_issues"`
	ProofRefs      []selectedStoreAbstractionProofRef `yaml:"proof_refs"`
	GuardProofRefs []selectedStoreAbstractionProofRef `yaml:"guard_proof_refs"`
	Notes          string                             `yaml:"notes"`
}

type selectedStoreAbstractionProofRef struct {
	Kind string `yaml:"kind"`
	Name string `yaml:"name,omitempty"`
	Path string `yaml:"path,omitempty"`
}

type selectedStoreAbstractionValidationContext struct {
	goTests map[string]string
}

func TestSelectedStoreAbstractionGuardMatrixCoversApprovedBoundary(t *testing.T) {
	root := selectedStoreAbstractionRepoRoot(t)
	matrix := loadSelectedStoreAbstractionGuardMatrix(t, root)
	ctx := newSelectedStoreAbstractionValidationContext(t, root)

	if problems := validateSelectedStoreAbstractionGuardMatrix(root, matrix, ctx); len(problems) > 0 {
		t.Fatalf("selected store abstraction guard matrix validation failed:\n- %s", strings.Join(problems, "\n- "))
	}
}

func TestSelectedStoreAbstractionGuardMatrixRejectsStaleProofRefs(t *testing.T) {
	root := selectedStoreAbstractionRepoRoot(t)
	ctx := newSelectedStoreAbstractionValidationContext(t, root)
	tests := []struct {
		name   string
		mutate func(*selectedStoreAbstractionGuardMatrix)
		want   string
	}{
		{
			name: "stale go test proof",
			mutate: func(matrix *selectedStoreAbstractionGuardMatrix) {
				row := selectedStoreAbstractionRowByID(t, matrix, "producer_backend_branching_guard")
				row.ProofRefs[0].Name = "TestMissingStoreAbstractionProof"
			},
			want: "producer_backend_branching_guard go_test proof_ref TestMissingStoreAbstractionProof does not resolve",
		},
		{
			name: "guard row requires regression proof",
			mutate: func(matrix *selectedStoreAbstractionGuardMatrix) {
				row := selectedStoreAbstractionRowByID(t, matrix, "raw_selected_runtime_writer_boundary_guard")
				row.GuardProofRefs = nil
			},
			want: "raw_selected_runtime_writer_boundary_guard guard row missing guard_proof_refs",
		},
		{
			name: "guard row classification is pinned",
			mutate: func(matrix *selectedStoreAbstractionGuardMatrix) {
				row := selectedStoreAbstractionRowByID(t, matrix, "producer_backend_branching_guard")
				row.Classification = "matrix_owner"
			},
			want: "producer_backend_branching_guard classification = \"matrix_owner\", want \"guard\"",
		},
		{
			name: "split row classification is pinned",
			mutate: func(matrix *selectedStoreAbstractionGuardMatrix) {
				row := selectedStoreAbstractionRowByID(t, matrix, "sqlite_busy_serialization_behavior_split")
				row.Classification = "guard"
				row.Tier = "required_smoke"
			},
			want: "sqlite_busy_serialization_behavior_split classification = \"guard\", want \"split_to_existing_issue\"",
		},
		{
			name: "split row issue set is pinned",
			mutate: func(matrix *selectedStoreAbstractionGuardMatrix) {
				row := selectedStoreAbstractionRowByID(t, matrix, "mutation_uow_behavior_split")
				row.SplitIssues = nil
			},
			want: "mutation_uow_behavior_split split_issues = [], want [1403]",
		},
		{
			name: "public idempotency row is required",
			mutate: func(matrix *selectedStoreAbstractionGuardMatrix) {
				matrix.Rows = selectedStoreAbstractionRowsExcept(matrix.Rows, "api_idempotency_public_surface_accounting")
			},
			want: "matrix missing required row api_idempotency_public_surface_accounting",
		},
		{
			name: "runtime log readback row is required",
			mutate: func(matrix *selectedStoreAbstractionGuardMatrix) {
				matrix.Rows = selectedStoreAbstractionRowsExcept(matrix.Rows, "runtime_log_readback_public_surface_accounting")
			},
			want: "matrix missing required row runtime_log_readback_public_surface_accounting",
		},
		{
			name: "workflow proof must resolve",
			mutate: func(matrix *selectedStoreAbstractionGuardMatrix) {
				row := selectedStoreAbstractionRowByID(t, matrix, "default_sqlite_required_smoke")
				row.ProofRefs[0].Name = "missing-sqlite-local-dev-job"
			},
			want: "default_sqlite_required_smoke workflow proof_ref missing-sqlite-local-dev-job missing from .github/workflows/ci.yml",
		},
		{
			name: "workflow proof path must be repo relative",
			mutate: func(matrix *selectedStoreAbstractionGuardMatrix) {
				row := selectedStoreAbstractionRowByID(t, matrix, "default_sqlite_required_smoke")
				row.ProofRefs[0].Path = "../../etc/passwd"
			},
			want: "default_sqlite_required_smoke workflow proof_ref path ../../etc/passwd must be repo-relative",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			matrix := loadSelectedStoreAbstractionGuardMatrix(t, root)
			tc.mutate(&matrix)
			problems := validateSelectedStoreAbstractionGuardMatrix(root, matrix, ctx)
			if !selectedStoreAbstractionProblemsContain(problems, tc.want) {
				t.Fatalf("validation problems missing %q:\n- %s", tc.want, strings.Join(problems, "\n- "))
			}
		})
	}
}

func loadSelectedStoreAbstractionGuardMatrix(t *testing.T, root string) selectedStoreAbstractionGuardMatrix {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, selectedStoreAbstractionGuardMatrixPath))
	if err != nil {
		t.Fatalf("read selected store abstraction guard matrix: %v", err)
	}
	var matrix selectedStoreAbstractionGuardMatrix
	if err := yaml.Unmarshal(raw, &matrix); err != nil {
		t.Fatalf("parse selected store abstraction guard matrix: %v", err)
	}
	return matrix
}

func newSelectedStoreAbstractionValidationContext(t *testing.T, root string) selectedStoreAbstractionValidationContext {
	t.Helper()
	goTests, err := loadSelectedStoreAbstractionGoTests(root)
	if err != nil {
		t.Fatalf("load selected store abstraction go test symbols: %v", err)
	}
	return selectedStoreAbstractionValidationContext{goTests: goTests}
}

func validateSelectedStoreAbstractionGuardMatrix(root string, matrix selectedStoreAbstractionGuardMatrix, ctx selectedStoreAbstractionValidationContext) []string {
	var problems []string
	if matrix.Version != 1 {
		problems = append(problems, fmt.Sprintf("matrix version = %d, want 1", matrix.Version))
	}
	if matrix.Kind != "selected_runtime_store_abstraction_guard_matrix" {
		problems = append(problems, fmt.Sprintf("matrix kind = %q, want selected_runtime_store_abstraction_guard_matrix", matrix.Kind))
	}
	if matrix.Issue != 1402 {
		problems = append(problems, fmt.Sprintf("matrix issue = %d, want 1402", matrix.Issue))
	}
	if matrix.ParentIssue != 1400 {
		problems = append(problems, fmt.Sprintf("matrix parent_issue = %d, want 1400", matrix.ParentIssue))
	}
	problems = append(problems, validateSelectedStoreAbstractionPolicy(matrix.Policy)...)

	rowsByID := map[string]selectedStoreAbstractionGuardMatrixRow{}
	for _, row := range matrix.Rows {
		id := strings.TrimSpace(row.ID)
		if id == "" {
			problems = append(problems, "matrix row missing id")
			continue
		}
		if _, exists := rowsByID[id]; exists {
			problems = append(problems, fmt.Sprintf("matrix row %s appears more than once", id))
		}
		rowsByID[id] = row
		problems = append(problems, validateSelectedStoreAbstractionRow(root, row, ctx)...)
	}
	for rowID := range requiredSelectedStoreAbstractionRows() {
		if _, ok := rowsByID[rowID]; !ok {
			problems = append(problems, fmt.Sprintf("matrix missing required row %s", rowID))
		}
	}
	problems = append(problems, validateSelectedStoreAbstractionRequiredRowShapes(rowsByID)...)
	sort.Strings(problems)
	return problems
}

func validateSelectedStoreAbstractionPolicy(policy selectedStoreAbstractionGuardMatrixPolicy) []string {
	var problems []string
	if policy.ClosureLevel != "matrix_guard_child_complete" {
		problems = append(problems, fmt.Sprintf("policy closure_level = %q, want matrix_guard_child_complete", policy.ClosureLevel))
	}
	if policy.ClaimsParentClosure {
		problems = append(problems, "policy claims_parent_closure = true, want false")
	}
	if strings.TrimSpace(policy.RequiredSmokePolicy) == "" {
		problems = append(problems, "policy required_smoke_policy missing")
	}
	if !strings.Contains(policy.NamedFastCommand, "TestSelectedStoreAbstractionGuardMatrix") {
		problems = append(problems, "policy named_fast_command missing selected store abstraction matrix proof")
	}
	if !strings.HasPrefix(strings.TrimSpace(policy.NamedFullConformanceCommand), "go test ./internal/runtime/cataloge2e") {
		problems = append(problems, fmt.Sprintf("policy named_full_conformance_command = %q, want cataloge2e go test command", policy.NamedFullConformanceCommand))
	}
	return problems
}

type selectedStoreAbstractionExpectedRowShape struct {
	Classification         string
	Tier                   string
	SplitIssues            []int
	RequiresGuardProofRefs bool
}

func validateSelectedStoreAbstractionRequiredRowShapes(rowsByID map[string]selectedStoreAbstractionGuardMatrixRow) []string {
	var problems []string
	for id, want := range expectedSelectedStoreAbstractionRowShapes() {
		row, ok := rowsByID[id]
		if !ok {
			continue
		}
		if row.Classification != want.Classification {
			problems = append(problems, fmt.Sprintf("%s classification = %q, want %q", id, row.Classification, want.Classification))
		}
		if row.Tier != want.Tier {
			problems = append(problems, fmt.Sprintf("%s tier = %q, want %q", id, row.Tier, want.Tier))
		}
		if !selectedStoreAbstractionSameIntSet(row.SplitIssues, want.SplitIssues) {
			problems = append(problems, fmt.Sprintf("%s split_issues = %v, want %v", id, selectedStoreAbstractionSortedInts(row.SplitIssues), selectedStoreAbstractionSortedInts(want.SplitIssues)))
		}
		if want.RequiresGuardProofRefs && len(row.GuardProofRefs) == 0 {
			problems = append(problems, fmt.Sprintf("%s required row missing guard_proof_refs", id))
		}
	}
	return problems
}

func expectedSelectedStoreAbstractionRowShapes() map[string]selectedStoreAbstractionExpectedRowShape {
	return map[string]selectedStoreAbstractionExpectedRowShape{
		"public_surface_backend_matrix_accounting": {
			Classification:         "matrix_owner",
			Tier:                   "required_smoke",
			RequiresGuardProofRefs: true,
		},
		"producer_backend_branching_guard": {
			Classification:         "guard",
			Tier:                   "required_smoke",
			RequiresGuardProofRefs: true,
		},
		"raw_selected_runtime_writer_boundary_guard": {
			Classification:         "guard",
			Tier:                   "required_smoke",
			SplitIssues:            []int{1403, 1405},
			RequiresGuardProofRefs: true,
		},
		"sqlite_runtime_sqldb_omission_guard": {
			Classification:         "guard",
			Tier:                   "required_smoke",
			RequiresGuardProofRefs: true,
		},
		"postgres_not_forced_through_sqlite_serialization_guard": {
			Classification:         "guard",
			Tier:                   "required_smoke",
			RequiresGuardProofRefs: true,
		},
		"default_sqlite_required_smoke": {
			Classification: "required_smoke",
			Tier:           "required_smoke",
		},
		"explicit_postgres_required_smoke": {
			Classification: "required_smoke",
			Tier:           "required_smoke",
		},
		"api_idempotency_public_surface_accounting": {
			Classification: "public_surface_accounting",
			Tier:           "required_smoke",
		},
		"runtime_log_readback_public_surface_accounting": {
			Classification: "public_surface_accounting",
			Tier:           "required_smoke",
		},
		"mutation_uow_behavior_split": {
			Classification: "split_to_existing_issue",
			Tier:           "split_open",
			SplitIssues:    []int{1403},
		},
		"sqlite_busy_serialization_behavior_split": {
			Classification: "split_to_existing_issue",
			Tier:           "split_open",
			SplitIssues:    []int{1405},
		},
	}
}

func validateSelectedStoreAbstractionRow(root string, row selectedStoreAbstractionGuardMatrixRow, ctx selectedStoreAbstractionValidationContext) []string {
	var problems []string
	id := strings.TrimSpace(row.ID)
	if _, ok := allowedSelectedStoreAbstractionClassifications()[row.Classification]; !ok {
		problems = append(problems, fmt.Sprintf("%s classification %q is not allowed", id, row.Classification))
	}
	if _, ok := allowedSelectedStoreAbstractionTiers()[row.Tier]; !ok {
		problems = append(problems, fmt.Sprintf("%s tier %q is not allowed", id, row.Tier))
	}
	if len(row.Concepts) == 0 {
		problems = append(problems, fmt.Sprintf("%s missing concepts", id))
	}
	if len(row.Owners) == 0 {
		problems = append(problems, fmt.Sprintf("%s missing owners", id))
	}
	if len(row.ProofRefs) == 0 {
		problems = append(problems, fmt.Sprintf("%s missing proof_refs", id))
	}
	if row.Classification == "guard" && len(row.GuardProofRefs) == 0 {
		problems = append(problems, fmt.Sprintf("%s guard row missing guard_proof_refs", id))
	}
	if row.Classification == "split_to_existing_issue" && len(row.SplitIssues) == 0 {
		problems = append(problems, fmt.Sprintf("%s split row missing split_issues", id))
	}
	for _, issue := range row.SplitIssues {
		switch issue {
		case 1403, 1405:
		default:
			problems = append(problems, fmt.Sprintf("%s split_issue %d is not an approved #1402 sibling", id, issue))
		}
	}
	problems = append(problems, validateSelectedStoreAbstractionProofRefs(root, id, row.ProofRefs, ctx)...)
	problems = append(problems, validateSelectedStoreAbstractionProofRefs(root, id, row.GuardProofRefs, ctx)...)
	return problems
}

func validateSelectedStoreAbstractionProofRefs(root, rowID string, refs []selectedStoreAbstractionProofRef, ctx selectedStoreAbstractionValidationContext) []string {
	var problems []string
	for _, ref := range refs {
		kind := strings.TrimSpace(ref.Kind)
		if _, ok := allowedSelectedStoreAbstractionProofKinds()[kind]; !ok {
			problems = append(problems, fmt.Sprintf("%s proof_ref kind %q is not allowed", rowID, ref.Kind))
			continue
		}
		switch kind {
		case "go_test":
			if strings.TrimSpace(ref.Name) == "" {
				problems = append(problems, fmt.Sprintf("%s go_test proof_ref missing name", rowID))
				continue
			}
			if _, ok := ctx.goTests[ref.Name]; !ok {
				problems = append(problems, fmt.Sprintf("%s go_test proof_ref %s does not resolve", rowID, ref.Name))
			}
		case "artifact":
			if strings.TrimSpace(ref.Path) == "" {
				problems = append(problems, fmt.Sprintf("%s artifact proof_ref missing path", rowID))
				continue
			}
			if filepath.IsAbs(ref.Path) || strings.HasPrefix(filepath.Clean(ref.Path), "..") {
				problems = append(problems, fmt.Sprintf("%s artifact proof_ref path %s must be repo-relative", rowID, ref.Path))
				continue
			}
			if _, err := os.Stat(filepath.Join(root, filepath.Clean(ref.Path))); err != nil {
				problems = append(problems, fmt.Sprintf("%s artifact proof_ref path %s does not exist", rowID, ref.Path))
			}
		case "workflow":
			if strings.TrimSpace(ref.Path) == "" || strings.TrimSpace(ref.Name) == "" {
				problems = append(problems, fmt.Sprintf("%s workflow proof_ref requires path and name", rowID))
				continue
			}
			if filepath.IsAbs(ref.Path) || strings.HasPrefix(filepath.Clean(ref.Path), "..") {
				problems = append(problems, fmt.Sprintf("%s workflow proof_ref path %s must be repo-relative", rowID, ref.Path))
				continue
			}
			raw, err := os.ReadFile(filepath.Join(root, filepath.Clean(ref.Path)))
			if err != nil {
				problems = append(problems, fmt.Sprintf("%s workflow proof_ref path %s does not exist", rowID, ref.Path))
				continue
			}
			if !strings.Contains(string(raw), ref.Name) {
				problems = append(problems, fmt.Sprintf("%s workflow proof_ref %s missing from %s", rowID, ref.Name, ref.Path))
			}
		}
	}
	return problems
}

func loadSelectedStoreAbstractionGoTests(root string) (map[string]string, error) {
	out := map[string]string{}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", ".swarm", "node_modules", "vendor":
				return filepath.SkipDir
			default:
				return nil
			}
		}
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		fileSet := token.NewFileSet()
		parsed, err := parser.ParseFile(fileSet, path, nil, 0)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil {
				continue
			}
			if name := fn.Name.Name; strings.HasPrefix(name, "Test") {
				out[name] = rel
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func selectedStoreAbstractionRowByID(t *testing.T, matrix *selectedStoreAbstractionGuardMatrix, id string) *selectedStoreAbstractionGuardMatrixRow {
	t.Helper()
	for i := range matrix.Rows {
		if matrix.Rows[i].ID == id {
			return &matrix.Rows[i]
		}
	}
	t.Fatalf("matrix row %s not found", id)
	return nil
}

func selectedStoreAbstractionRowsExcept(rows []selectedStoreAbstractionGuardMatrixRow, id string) []selectedStoreAbstractionGuardMatrixRow {
	out := rows[:0]
	for _, row := range rows {
		if row.ID != id {
			out = append(out, row)
		}
	}
	return out
}

func selectedStoreAbstractionProblemsContain(problems []string, want string) bool {
	for _, problem := range problems {
		if strings.Contains(problem, want) {
			return true
		}
	}
	return false
}

func requiredSelectedStoreAbstractionRows() map[string]struct{} {
	return selectedStoreAbstractionStringSet([]string{
		"public_surface_backend_matrix_accounting",
		"producer_backend_branching_guard",
		"raw_selected_runtime_writer_boundary_guard",
		"sqlite_runtime_sqldb_omission_guard",
		"postgres_not_forced_through_sqlite_serialization_guard",
		"default_sqlite_required_smoke",
		"explicit_postgres_required_smoke",
		"api_idempotency_public_surface_accounting",
		"runtime_log_readback_public_surface_accounting",
		"mutation_uow_behavior_split",
		"sqlite_busy_serialization_behavior_split",
	})
}

func allowedSelectedStoreAbstractionClassifications() map[string]struct{} {
	return selectedStoreAbstractionStringSet([]string{
		"guard",
		"matrix_owner",
		"public_surface_accounting",
		"required_smoke",
		"split_to_existing_issue",
	})
}

func allowedSelectedStoreAbstractionTiers() map[string]struct{} {
	return selectedStoreAbstractionStringSet([]string{
		"required_smoke",
		"split_open",
	})
}

func allowedSelectedStoreAbstractionProofKinds() map[string]struct{} {
	return selectedStoreAbstractionStringSet([]string{
		"artifact",
		"go_test",
		"workflow",
	})
}

func selectedStoreAbstractionStringSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		out[value] = struct{}{}
	}
	return out
}

func selectedStoreAbstractionSameIntSet(actual, want []int) bool {
	actualSorted := selectedStoreAbstractionSortedInts(actual)
	wantSorted := selectedStoreAbstractionSortedInts(want)
	if len(actualSorted) != len(wantSorted) {
		return false
	}
	for i := range actualSorted {
		if actualSorted[i] != wantSorted[i] {
			return false
		}
	}
	return true
}

func selectedStoreAbstractionSortedInts(values []int) []int {
	out := append([]int(nil), values...)
	sort.Ints(out)
	return out
}

func selectedStoreAbstractionRepoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(wd, "go.mod")); err == nil {
			return wd
		}
		parent := filepath.Dir(wd)
		if parent == wd {
			t.Fatal("repo root not found")
		}
		wd = parent
	}
}
