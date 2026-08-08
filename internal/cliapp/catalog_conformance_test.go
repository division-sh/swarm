package cliapp

import (
	"bytes"
	"context"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/testcatalog"
)

func TestCatalogRequiredVerifyAll(t *testing.T) {
	t.Setenv("SWARM_EMIT_SCHEMA_STRICT", "true")
	repoRoot := RepoRoot()
	inventory, err := testcatalog.Load(repoRoot)
	if err != nil {
		t.Fatalf("load catalog inventory: %v", err)
	}
	configPath := writeTestVerifyRuntimeConfig(t)
	verified := 0
	for _, fixture := range inventory.Fixtures {
		fixture := fixture
		t.Run(fixture.RelativePath, func(t *testing.T) {
			t.Setenv("SWARM_BOOT_WARNINGS_FATAL", catalogWarningsFatal(fixture))
			opts := defaultVerifyCommandOptions()
			opts.contractsPath = fixture.Root
			opts.configPath = configPath
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			code := runVerifyCommandWithOutput(context.Background(), repoRoot, opts, &stdout, &stderr)
			verified++

			switch fixture.Metadata.Disposition {
			case testcatalog.DispositionRuntime:
				if code != 0 {
					t.Fatalf("runtime fixture failed supported verify: code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
				}
			case testcatalog.DispositionVerifyOnly:
				assertCatalogVerifyOnlyResult(t, repoRoot, fixture, code, stdout.String(), stderr.String())
			case testcatalog.DispositionRetired:
				// Retired rows are still verified for census completeness but receive no result or claim credit.
			default:
				t.Fatalf("unsupported catalog disposition %q", fixture.Metadata.Disposition)
			}
		})
	}
	if verified != len(inventory.Fixtures) {
		t.Fatalf("supported verify executions = %d, want %d", verified, len(inventory.Fixtures))
	}
}

func catalogWarningsFatal(fixture testcatalog.Fixture) string {
	if fixture.Metadata.Disposition == testcatalog.DispositionVerifyOnly && fixture.Metadata.Verify == testcatalog.VerifyWarning {
		return "false"
	}
	return "true"
}

func assertCatalogVerifyOnlyResult(t *testing.T, repoRoot string, fixture testcatalog.Fixture, code int, stdout, stderr string) {
	t.Helper()
	switch fixture.Metadata.Verify {
	case testcatalog.VerifyPass:
		if code != 0 {
			t.Fatalf("verify-pass fixture failed: code=%d stdout=%q stderr=%q", code, stdout, stderr)
		}
	case testcatalog.VerifyWarning:
		if code != 0 {
			t.Fatalf("verify-warning fixture failed: code=%d stdout=%q stderr=%q", code, stdout, stderr)
		}
		assertCatalogVerifyDiagnostic(t, repoRoot, fixture, stdout, stderr)
	case testcatalog.VerifyReject:
		if code == 0 {
			t.Fatalf("verify-%s fixture succeeded: stdout=%q stderr=%q", fixture.Metadata.Verify, stdout, stderr)
		}
		assertCatalogVerifyDiagnostic(t, repoRoot, fixture, stdout, stderr)
	default:
		t.Fatalf("unsupported verify-only result %q", fixture.Metadata.Verify)
	}
}

func assertCatalogVerifyDiagnostic(t *testing.T, repoRoot string, fixture testcatalog.Fixture, stdout, stderr string) {
	t.Helper()
	want := fixture.Metadata.Diagnostic
	if want == nil {
		t.Fatal("verify warning/reject fixture has no diagnostic metadata")
	}
	combined := stdout + "\n" + stderr
	if !strings.Contains(combined, want.Contains) {
		t.Fatalf("verify-%s output missing teaching evidence %q: stdout=%q stderr=%q", fixture.Metadata.Verify, want.Contains, stdout, stderr)
	}
	if strings.Contains(combined, want.Category) {
		return
	}
	_, _, err := NewSwarmWorkflowModule(repoRoot, fixture.Root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if diagnostic, ok := runtimecontracts.AsLoaderDiagnostic(err); ok && diagnostic.Code == want.Category {
		return
	}
	t.Fatalf("verify-%s path missing canonical diagnostic category %q: stdout=%q stderr=%q loader_error=%v", fixture.Metadata.Verify, want.Category, stdout, stderr, err)
}
