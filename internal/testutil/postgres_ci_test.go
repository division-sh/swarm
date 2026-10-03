package testutil

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
	"github.com/division-sh/swarm/internal/testpostgres"

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

	for _, jobName := range []string{"proof-unit", "mandatory-soak", "semantic-smoke"} {
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
			command := step.Run
			if command == "bash .github/scripts/run-proof-batch.sh" {
				batch, err := os.ReadFile(filepath.Join(root, ".github/scripts/run-proof-batch.sh"))
				if err != nil {
					t.Fatal(err)
				}
				command = string(batch)
			}
			hasRunner = hasRunner || strings.Contains(command, "go run ./cmd/swarm-test --")
			if strings.Contains(command, "start-postgres-ci.sh") || strings.Contains(command, "docker run") || strings.Contains(command, "docker rm") {
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

func TestCanonicalTestRunnerHasNoRemovedBinaryAlias(t *testing.T) {
	root := testRepoRoot(t)
	newPath := filepath.Join(root, "cmd", "swarm-test", "main.go")
	if _, err := os.Stat(newPath); err != nil {
		t.Fatalf("canonical test runner is missing: %v", err)
	}
	oldRel := filepath.Join("cmd", "swarm-test-"+"postgres", "main.go")
	if _, err := os.Stat(filepath.Join(root, oldRel)); !os.IsNotExist(err) {
		t.Fatalf("removed test runner alias survives: %v", err)
	}
	oldReference := filepath.ToSlash(oldRel)
	for _, rel := range []string{"README.md", "CONTRIBUTING.md", ".github/workflows/ci.yml", "internal/testutil/POSTGRES.md"} {
		raw, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), oldReference) {
			t.Fatalf("removed binary path survives in %s", rel)
		}
	}
}

func TestPostgresTestEnvironmentHasNoCompetingReaderOrProjector(t *testing.T) {
	root := testRepoRoot(t)
	violations, err := postgresEnvironmentAuthorityViolations(root)
	if err != nil {
		t.Fatalf("scan Postgres environment owners: %v", err)
	}
	for _, violation := range violations {
		t.Error(violation)
	}
}

func postgresEnvironmentAuthorityViolations(root string) ([]string, error) {
	var violations []string
	err := checkoutsource.WalkDir(root, root, func(path string, entry os.DirEntry, walkErr error) error {
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
			violations = append(violations, fmt.Sprintf("non-authoritative Postgres DSN projector survives in %s", rel))
		}
		manualParserFragments := []string{
			"strings.Fields(" + "dsn)",
			"strings.HasPrefix(part, " + `"port=")`,
			"strings.HasPrefix(part, " + `"dbname=")`,
		}
		for _, fragment := range manualParserFragments {
			if strings.Contains(text, fragment) {
				violations = append(violations, fmt.Sprintf("manual Postgres DSN interpreter %q survives in %s", fragment, rel))
			}
		}
		if isProduction && strings.Contains(text, `pq.NewConfig(`) && filepath.ToSlash(rel) != "internal/testpostgres/connection.go" {
			violations = append(violations, fmt.Sprintf("competing pq.NewConfig owner survives in %s", rel))
		}
		if isProduction && strings.Contains(text, `pq.NewConnectorConfig(`) && filepath.ToSlash(rel) != "internal/testpostgres/connection.go" {
			violations = append(violations, fmt.Sprintf("competing pq.NewConnectorConfig owner survives in %s", rel))
		}
		if isProduction && filepath.ToSlash(rel) != "internal/testpostgres/connection.go" {
			file, err := parser.ParseFile(token.NewFileSet(), path, data, 0)
			if err != nil {
				return err
			}
			for _, violation := range postgresSourceAuthorityViolations(file) {
				violations = append(violations, fmt.Sprintf("%s survives in %s", violation, rel))
			}
		}
		return nil
	})
	return violations, err
}

func TestPostgresGuardIgnoresNestedCheckoutAndRejectsCurrentSource(t *testing.T) {
	root := t.TempDir()
	foreign := filepath.Join(root, "internal", "review-nested")
	if err := os.MkdirAll(foreign, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(foreign, ".git"), []byte("gitdir: elsewhere\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	stale := []byte("package probe\nfunc x() { pq.NewConfig() }\n")
	if err := os.WriteFile(filepath.Join(foreign, "stale.go"), stale, 0o644); err != nil {
		t.Fatal(err)
	}
	check := func(want bool) {
		t.Helper()
		violations, err := postgresEnvironmentAuthorityViolations(root)
		if err != nil || (len(violations) > 0) != want {
			t.Fatalf("guard violations = %v, %v; want violation %v", violations, err, want)
		}
	}
	check(false)
	current := filepath.Join(root, "internal", "current.go")
	if err := os.WriteFile(current, stale, 0o644); err != nil {
		t.Fatal(err)
	}
	check(true)
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
	violations, err := postgresContributorGuideViolations(guidePath)
	if err != nil {
		t.Fatalf("read POSTGRES.md: %v", err)
	}
	for _, violation := range violations {
		t.Error(violation)
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
	const runnerCommand = "go run ./cmd/swarm-test"
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

func postgresContributorGuideViolations(path string) ([]string, error) {
	guide, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	// Ignore Markdown line wrapping without interpreting or owning capacity policy.
	guideText := strings.Join(strings.Fields(string(guide)), " ")
	var violations []string
	for _, want := range []string{
		"SWARM_TEST_POSTGRES_DSN",
		"PostgreSQL 16",
		"CREATEDB",
		"PGPASSWORD",
		"fsync=off",
		"synchronous_commit=off",
		"full_page_writes=off",
		"Runner-Owned Docker",
		"`testpostgres.RequiredMaxConnections`",
		fmt.Sprintf("`max_connections >= %d`", testpostgres.RequiredMaxConnections),
		"-c 'SHOW max_connections;'",
		fmt.Sprintf("-c max_connections=%d -c fsync=off", testpostgres.RequiredMaxConnections),
		fmt.Sprintf("ask the administrator to set `max_connections` to at least %d and restart the dedicated test server", testpostgres.RequiredMaxConnections),
		"Any configuration change and restart must be manual and administrator-approved",
		"the harness never changes host configuration automatically",
		"fails closed with no Docker fallback",
	} {
		if !strings.Contains(guideText, want) {
			violations = append(violations, fmt.Sprintf("POSTGRES.md missing %q", want))
		}
	}
	return violations, nil
}

func TestPostgresContributorGuideGuardRejectsCapacityDrift(t *testing.T) {
	guide, err := os.ReadFile(filepath.Join(testRepoRoot(t), "internal", "testutil", "POSTGRES.md"))
	if err != nil {
		t.Fatal(err)
	}
	threshold := fmt.Sprintf("`max_connections >= %d`", testpostgres.RequiredMaxConnections)
	setupArg := fmt.Sprintf("-c max_connections=%d", testpostgres.RequiredMaxConnections)
	remediation := fmt.Sprintf("least %d and restart the dedicated test server", testpostgres.RequiredMaxConnections)
	for _, tc := range []struct {
		name        string
		old         string
		replacement string
		want        string
	}{
		{name: "canonical copy"},
		{name: "missing threshold", old: threshold, want: threshold},
		{name: "lower threshold", old: threshold, replacement: fmt.Sprintf("`max_connections >= %d`", testpostgres.RequiredMaxConnections-1), want: threshold},
		{name: "higher threshold", old: threshold, replacement: fmt.Sprintf("`max_connections >= %d`", testpostgres.RequiredMaxConnections+1), want: threshold},
		{name: "missing observation", old: "-c 'SHOW max_connections;'", want: "SHOW max_connections"},
		{name: "wrong observation", old: "SHOW max_connections;", replacement: "SHOW shared_buffers;", want: "SHOW max_connections"},
		{name: "missing setup argument", old: setupArg, want: setupArg},
		{name: "lower setup argument", old: setupArg, replacement: fmt.Sprintf("-c max_connections=%d", testpostgres.RequiredMaxConnections-1), want: setupArg},
		{name: "setup argument prefix mismatch", old: setupArg, replacement: setupArg + "0", want: setupArg},
		{name: "missing remediation", old: remediation, want: "ask the administrator"},
		{name: "wrong remediation threshold", old: remediation, replacement: fmt.Sprintf("least %d and restart the dedicated test server", testpostgres.RequiredMaxConnections-1), want: "ask the administrator"},
		{name: "missing administrator", old: "ask the administrator", replacement: "ask the harness", want: "ask the administrator"},
		{name: "missing restart", old: "and restart the dedicated test server", replacement: "without restarting the dedicated test server", want: "ask the administrator"},
		{name: "automatic remediation", old: "manual and administrator-approved", replacement: "automatic", want: "manual and administrator-approved"},
		{name: "host mutation", old: "never changes host configuration automatically", replacement: "changes host configuration automatically", want: "never changes host configuration automatically"},
		{name: "Docker fallback", old: "fails closed with no Docker fallback", replacement: "falls back to Docker", want: "fails closed with no Docker fallback"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := string(guide)
			if tc.old != "" {
				if strings.Count(fixture, tc.old) != 1 {
					t.Fatalf("guide must contain exactly one mutation target %q", tc.old)
				}
				fixture = strings.Replace(fixture, tc.old, tc.replacement, 1)
			}
			path := filepath.Join(t.TempDir(), "POSTGRES.md")
			if err := os.WriteFile(path, []byte(fixture), 0o644); err != nil {
				t.Fatal(err)
			}
			violations, err := postgresContributorGuideViolations(path)
			if err != nil {
				t.Fatal(err)
			}
			if tc.want == "" {
				if len(violations) != 0 {
					t.Fatalf("canonical guide rejected: %v", violations)
				}
			} else if len(violations) != 1 || !strings.Contains(violations[0], tc.want) {
				t.Fatalf("guide guard violations = %v; want one containing %q", violations, tc.want)
			}
		})
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
