package testutil

import (
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestCIPostgresJobsShareOwnedLauncher(t *testing.T) {
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

	wantJobs := []string{"full-conformance", "nightly-extras", "semantic-smoke", "test-shard"}
	var gotJobs []string
	for jobName, job := range workflow.Jobs {
		if job.Env["SWARM_TEST_POSTGRES_DSN"] == "" {
			continue
		}
		gotJobs = append(gotJobs, jobName)
		if job.Env["SWARM_TEST_POSTGRES_DSN"] != "host=127.0.0.1 port=5432 user=postgres password=postgres dbname=postgres sslmode=disable" {
			t.Fatalf("job %s has non-canonical test DSN %q", jobName, job.Env["SWARM_TEST_POSTGRES_DSN"])
		}
		if _, hasLegacyService := job.Services["postgres"]; hasLegacyService {
			t.Fatalf("job %s retains a Postgres service instead of the canonical launcher", jobName)
		}
		hasLauncher := false
		for _, step := range job.Steps {
			hasLauncher = hasLauncher || (step.Name == "Start and verify disposable Postgres" && strings.TrimSpace(step.Run) == "./internal/testutil/start-postgres-ci.sh")
		}
		if !hasLauncher {
			t.Fatalf("job %s does not consume the canonical Postgres launcher", jobName)
		}
	}
	sort.Strings(gotJobs)
	if !reflect.DeepEqual(gotJobs, wantJobs) {
		t.Fatalf("CI Postgres jobs = %v, want %v", gotJobs, wantJobs)
	}
	launcher, err := os.ReadFile(filepath.Join(root, "internal", "testutil", "start-postgres-ci.sh"))
	if err != nil {
		t.Fatalf("read canonical CI Postgres launcher: %v", err)
	}
	launcherText := string(launcher)
	for _, want := range []string{
		"docker run --detach --rm",
		"--tmpfs /var/lib/postgresql/data:rw",
		"--publish 127.0.0.1:5432:5432",
		"postgres:16",
		"-c max_connections=300",
		"-c fsync=off",
		"-c synchronous_commit=off",
		"-c full_page_writes=off",
		"pg_isready -U postgres -d postgres",
		"for setting in max_connections fsync synchronous_commit full_page_writes",
		`psql -U postgres -d postgres -tAc "SHOW $setting"`,
	} {
		if !strings.Contains(launcherText, want) {
			t.Fatalf("canonical CI Postgres launcher missing %q", want)
		}
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
		if !strings.HasSuffix(path, "_test.go") && strings.Contains(text, `os.Getenv("SWARM_TEST_POSTGRES_DSN")`) && filepath.ToSlash(rel) != "internal/testutil/postgres.go" {
			t.Errorf("competing SWARM_TEST_POSTGRES_DSN reader survives in %s", rel)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan Postgres environment owners: %v", err)
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
		"Docker Fallback",
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
