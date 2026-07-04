package bootverify

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestRun_PlatformVersionCompatibility(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		rootRange   string
		childRange  string
		requires    string
		wantMessage string
		wantLoc     string
	}{
		{
			name:      "compatible root package",
			rootRange: `">=0.7.0 <0.8.0"`,
		},
		{
			name:        "missing root package range",
			wantMessage: "platform_version missing",
			wantLoc:     "package.yaml",
		},
		{
			name:        "malformed root package range",
			rootRange:   `"latest"`,
			wantMessage: "not valid Masterminds semver/v3 constraints",
			wantLoc:     "package.yaml",
		},
		{
			name:        "out of range root package",
			rootRange:   `">=0.8.0"`,
			wantMessage: `does not include running platform "0.7.0"`,
			wantLoc:     "package.yaml",
		},
		{
			name:        "out of range child package",
			rootRange:   `">=0.7.0 <0.8.0"`,
			childRange:  `">=0.8.0"`,
			wantMessage: "package flows/support (support) declares platform_version",
			wantLoc:     "flows/support/package.yaml",
		},
		{
			name:      "requires platform version is import metadata only",
			rootRange: `">=0.7.0 <0.8.0"`,
			requires:  `">=999.0.0"`,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			root := writePlatformVersionCompatibilityFixture(t, tc.rootRange, tc.childRange, tc.requires)
			repoRoot := repoRootForBootverifyTest(t)
			platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
			report := Run(context.Background(), semanticview.Wrap(loadFixtureBundleAt(t, repoRoot, root, platformSpec)), Options{})
			if tc.wantMessage == "" {
				if reportContains(report.Errors(), "platform_version_compatibility", "") {
					t.Fatalf("unexpected platform_version_compatibility finding: %#v", report.Errors())
				}
				return
			}
			if !findingContains(report.Errors(), "platform_version_compatibility", tc.wantMessage, tc.wantLoc) {
				t.Fatalf("expected platform_version_compatibility containing %q at %q, got %#v", tc.wantMessage, tc.wantLoc, report.Errors())
			}
		})
	}
}

func writePlatformVersionCompatibilityFixture(t *testing.T, rootRange, childRange, requiresRange string) string {
	t.Helper()

	root := t.TempDir()
	var rootPackage strings.Builder
	rootPackage.WriteString("name: platform-version-compatibility\n")
	rootPackage.WriteString("version: \"1.0.0\"\n")
	if strings.TrimSpace(rootRange) != "" {
		rootPackage.WriteString("platform_version: ")
		rootPackage.WriteString(rootRange)
		rootPackage.WriteString("\n")
	}
	if strings.TrimSpace(requiresRange) != "" {
		rootPackage.WriteString("requires:\n  platform_version: ")
		rootPackage.WriteString(requiresRange)
		rootPackage.WriteString("\n")
	}
	if strings.TrimSpace(childRange) != "" {
		rootPackage.WriteString("flows:\n  - id: support\n    flow: support\n    mode: static\n")
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), rootPackage.String())
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: platform-version-compatibility\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")

	if strings.TrimSpace(childRange) != "" {
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "package.yaml"), "name: support\nversion: \"1.0.0\"\nplatform_version: "+childRange+"\nflows: []\n")
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "schema.yaml"), "name: support\nmode: static\n")
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "policy.yaml"), "{}\n")
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "tools.yaml"), "{}\n")
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "agents.yaml"), "{}\n")
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "events.yaml"), "{}\n")
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "nodes.yaml"), "{}\n")
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "entities.yaml"), "{}\n")
	}
	return root
}

func findingContains(items []Finding, checkID, message, location string) bool {
	for _, item := range items {
		if item.CheckID == checkID && strings.Contains(item.Message, message) && (location == "" || item.Location == location) {
			return true
		}
	}
	return false
}
