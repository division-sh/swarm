package testutil

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestCIPostgresJobsShareOwnedRunner(t *testing.T) {
	root := testRepoRoot(t)
	data, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "ci.yml"))
	if err != nil {
		t.Fatalf("read ci.yml: %v", err)
	}
	var workflow struct {
		Jobs map[string]struct {
			Env      map[string]string `yaml:"env"`
			Services map[string]any    `yaml:"services"`
			Steps    []struct {
				Name string `yaml:"name"`
				Run  string `yaml:"run"`
			} `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse ci.yml: %v", err)
	}

	for _, jobName := range []string{"full-conformance", "nightly-extras", "semantic-smoke", "test-shard"} {
		job, ok := workflow.Jobs[jobName]
		if !ok {
			t.Fatalf("missing Postgres-consuming CI job %s", jobName)
		}
		if job.Env["SWARM_TEST_POSTGRES_DSN"] != "" {
			t.Fatalf("job %s retains job-owned Postgres DSN", jobName)
		}
		if _, hasLegacyService := job.Services["postgres"]; hasLegacyService {
			t.Fatalf("job %s retains a Postgres service instead of the canonical runner", jobName)
		}
		hasRunner := false
		for _, step := range job.Steps {
			hasRunner = hasRunner || strings.Contains(step.Run, "go run ./cmd/swarm-test-postgres -- go test")
			if strings.Contains(step.Run, "start-postgres-ci.sh") || strings.Contains(step.Run, "docker run") || strings.Contains(step.Run, "docker rm") {
				t.Fatalf("job %s retains a competing Docker lifecycle in step %q", jobName, step.Name)
			}
		}
		if !hasRunner {
			t.Fatalf("job %s does not consume the canonical Postgres runner", jobName)
		}
	}
	if _, err := os.Stat(filepath.Join(root, "internal", "testutil", "start-postgres-ci.sh")); !os.IsNotExist(err) {
		t.Fatalf("legacy CI launcher survives: %v", err)
	}
}

func TestPostgresTestEnvironmentHasNoCompetingReaderOrProjector(t *testing.T) {
	root := testRepoRoot(t)
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		text := string(data)
		isProduction := !strings.HasSuffix(path, "_test.go")
		if strings.Contains(text, "withDB"+"Name(") {
			t.Errorf("non-authoritative Postgres DSN projector survives in %s", rel)
		}
		manualParserFragments := []string{
			"strings.Fields(" + "dsn)",
			"strings.HasPrefix(part, " + `"port=")`,
			"strings.HasPrefix(part, " + `"dbname=")`,
		}
		for _, fragment := range manualParserFragments {
			if strings.Contains(text, fragment) {
				t.Errorf("manual Postgres DSN interpreter %q survives in %s", fragment, rel)
			}
		}
		if isProduction && strings.Contains(text, `pq.NewConfig(`) && filepath.ToSlash(rel) != "internal/testpostgres/connection.go" {
			t.Errorf("competing pq.NewConfig owner survives in %s", rel)
		}
		if isProduction && strings.Contains(text, `pq.NewConnectorConfig(`) && filepath.ToSlash(rel) != "internal/testpostgres/connection.go" {
			t.Errorf("competing pq.NewConnectorConfig owner survives in %s", rel)
		}
		if isProduction && filepath.ToSlash(rel) != "internal/testpostgres/connection.go" {
			file, err := parser.ParseFile(token.NewFileSet(), path, data, 0)
			if err != nil {
				return err
			}
			for _, violation := range postgresSourceAuthorityViolations(file) {
				t.Errorf("%s survives in %s", violation, rel)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan Postgres environment owners: %v", err)
	}
}

func postgresSourceAuthorityViolations(file *ast.File) []string {
	aliases := make(map[string]string)
	for _, imp := range file.Imports {
		importPath, _ := strconv.Unquote(imp.Path.Value)
		name := filepath.Base(importPath)
		if imp.Name != nil {
			name = imp.Name.Name
		}
		aliases[name] = importPath
	}
	var violations []string
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		pkg, _ := selector.X.(*ast.Ident)
		if pkg == nil {
			return true
		}
		if aliases[pkg.Name] == "os" && selector.Sel.Name == "Getenv" && len(call.Args) == 1 && postgresSourceExpression(call.Args[0]) {
			violations = append(violations, "competing SWARM_TEST_POSTGRES_DSN reader")
		}
		if aliases[pkg.Name] == "github.com/division-sh/swarm/internal/testpostgres" && selector.Sel.Name == "ParseConnection" {
			violations = append(violations, "competing Postgres source parser")
		}
		return true
	})
	return violations
}

func postgresSourceExpression(expression ast.Expr) bool {
	switch value := expression.(type) {
	case *ast.Ident:
		return value.Name == "SourceEnv"
	case *ast.SelectorExpr:
		return value.Sel.Name == "SourceEnv"
	case *ast.BasicLit:
		literal, _ := strconv.Unquote(value.Value)
		return literal == "SWARM_TEST_POSTGRES_DSN"
	default:
		return false
	}
}

func TestPostgresContributorGuideIsCanonicalAndQuarantined(t *testing.T) {
	root := testRepoRoot(t)
	guidePath := filepath.Join(root, "internal", "testutil", "POSTGRES.md")
	guide, err := os.ReadFile(guidePath)
	if err != nil {
		t.Fatalf("read POSTGRES.md: %v", err)
	}
	guideText := string(guide)
	for _, want := range []string{
		"SWARM_TEST_POSTGRES_DSN",
		"PostgreSQL 16",
		"CREATEDB",
		"PGPASSWORD",
		"fsync=off",
		"synchronous_commit=off",
		"full_page_writes=off",
		"Runner-Owned Docker",
	} {
		if !strings.Contains(guideText, want) {
			t.Fatalf("POSTGRES.md missing %q", want)
		}
	}
	contributing, err := os.ReadFile(filepath.Join(root, "CONTRIBUTING.md"))
	if err != nil {
		t.Fatalf("read CONTRIBUTING.md: %v", err)
	}
	if !strings.Contains(string(contributing), "internal/testutil/POSTGRES.md") {
		t.Fatal("CONTRIBUTING.md does not link canonical Postgres test guide")
	}
	readme, err := os.ReadFile(filepath.Join(root, "README.md"))
	if err != nil {
		t.Fatalf("read README.md: %v", err)
	}
	const runnerCommand = "go run ./cmd/swarm-test-postgres -- go test ./..."
	if !strings.Contains(string(readme), runnerCommand) || !strings.Contains(string(contributing), runnerCommand) {
		t.Fatal("README and CONTRIBUTING must consume the canonical Postgres runner")
	}
	if strings.Contains(string(readme), "\ngo test ./...\n") || strings.Contains(string(contributing), "\ngo test ./...\n") {
		t.Fatal("public contributor workflow retains bare no-DSN full-suite command")
	}

	for _, rel := range []string{"README.md", ".env.example", "swarm.example.yaml"} {
		data, err := os.ReadFile(filepath.Join(root, rel))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			t.Fatalf("read %s: %v", rel, err)
		}
		if strings.Contains(string(data), "SWARM_TEST_POSTGRES_DSN") {
			t.Fatalf("public onboarding surface %s advertises quarantined test env", rel)
		}
	}
}

func testRepoRoot(t *testing.T) string {
	t.Helper()
	specPath, err := platformSpecPath()
	if err != nil {
		t.Fatalf("platformSpecPath: %v", err)
	}
	return filepath.Dir(specPath)
}
