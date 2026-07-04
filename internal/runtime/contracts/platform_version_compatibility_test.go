package contracts

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestValidateBundlePlatformVersionCompatibilityFailsClosed(t *testing.T) {
	t.Parallel()

	bundle := platformVersionCompatibilityTestBundle(">=0.8.0")
	err := ValidateBundlePlatformVersionCompatibility(bundle)
	if err == nil {
		t.Fatal("ValidateBundlePlatformVersionCompatibility error = nil, want incompatible platform_version")
	}
	for _, want := range []string{
		"platform version compatibility failed",
		`package . (incompatible-package) declares platform_version ">=0.8.0"`,
		`running platform.version is "0.7.0"`,
		"remediation: update package.yaml platform_version after re-verifying",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error = %v, want substring %q", err, want)
		}
	}
}

func TestBuildBundleCatalogProjectionRejectsIncompatiblePlatformVersion(t *testing.T) {
	t.Parallel()

	_, err := BuildBundleCatalogProjection(platformVersionCompatibilityTestBundle(">=0.8.0"))
	if err == nil || !strings.Contains(err.Error(), `platform_version range ">=0.8.0" does not include running platform "0.7.0"`) {
		t.Fatalf("BuildBundleCatalogProjection error = %v, want platform_version compatibility failure", err)
	}
}

func TestBuildBundleRegistrationDirectoryUploadRejectsIncompatiblePlatformVersion(t *testing.T) {
	t.Parallel()

	repo := repoRootForContractsTest(t)
	root := writePlatformVersionCompatibilityContractsDir(t, ">=0.8.0")
	_, err := BuildBundleRegistrationDirectoryUpload(repo, root, DefaultPlatformSpecFile(repo))
	if err == nil || !strings.Contains(err.Error(), `platform_version range ">=0.8.0" does not include running platform "0.7.0"`) {
		t.Fatalf("BuildBundleRegistrationDirectoryUpload error = %v, want platform_version compatibility failure", err)
	}
}

func TestLoadBundleCatalogRuntimeSourceRejectsIncompatiblePersistedBytes(t *testing.T) {
	t.Parallel()

	repo := repoRootForContractsTest(t)
	root := writePlatformVersionCompatibilityContractsDir(t, ">=0.7.0 <0.8.0")
	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	projection, err := BuildBundleCatalogProjection(bundle)
	if err != nil {
		t.Fatalf("BuildBundleCatalogProjection: %v", err)
	}
	projection.ContentYAML = rewriteBundleCatalogPackageYAML(t, projection.ContentYAML, `platform_version: ">=0.7.0 <0.8.0"`, `platform_version: ">=0.8.0"`)

	_, err = LoadBundleCatalogRuntimeSource(repo, BundleCatalogRuntimeLoadRequest{
		BundleHash:  projection.BundleHash,
		ContentYAML: projection.ContentYAML,
		DataBlob:    projection.DataBlob,
	})
	if err == nil || !strings.Contains(err.Error(), `admit bundle catalog runtime source: platform version compatibility failed`) {
		t.Fatalf("LoadBundleCatalogRuntimeSource error = %v, want platform_version compatibility failure", err)
	}
}

func TestLoadBundleCatalogRuntimeSourceUsesRunningPlatformVersionForAdmission(t *testing.T) {
	t.Parallel()

	repo := repoRootForContractsTest(t)
	root := writePlatformVersionCompatibilityContractsDir(t, ">=0.7.0 <0.8.0")
	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	projection, err := BuildBundleCatalogProjection(bundle)
	if err != nil {
		t.Fatalf("BuildBundleCatalogProjection: %v", err)
	}
	runningPlatformSpec := writePlatformVersionCompatibilityPlatformSpec(t, repo, "0.8.0")

	_, err = LoadBundleCatalogRuntimeSource(repo, BundleCatalogRuntimeLoadRequest{
		BundleHash:              projection.BundleHash,
		ContentYAML:             projection.ContentYAML,
		DataBlob:                projection.DataBlob,
		RunningPlatformSpecPath: runningPlatformSpec,
	})
	if err == nil {
		t.Fatal("LoadBundleCatalogRuntimeSource error = nil, want running platform compatibility failure")
	}
	for _, want := range []string{
		`platform_version range ">=0.7.0 <0.8.0" does not include running platform "0.8.0"`,
		`running platform.version is "0.8.0"`,
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("LoadBundleCatalogRuntimeSource error = %v, want substring %q", err, want)
		}
	}
}

func platformVersionCompatibilityTestBundle(declaredRange string) *WorkflowContractBundle {
	bundle := &WorkflowContractBundle{
		PackageTree: []LoadedProjectPackage{{
			Key: ".",
			Manifest: ProjectPackageDocument{
				Name:            "incompatible-package",
				PlatformVersion: declaredRange,
			},
		}},
	}
	bundle.Platform.Platform.Version = "0.7.0"
	return bundle
}

func writePlatformVersionCompatibilityContractsDir(t *testing.T, declaredRange string) string {
	t.Helper()

	root := t.TempDir()
	writeBundleHashText(t, filepath.Join(root, "package.yaml"), `name: platform-version-admission
version: "1.0.0"
platform_version: "`+declaredRange+`"
flows: []
`)
	writeBundleHashText(t, filepath.Join(root, "schema.yaml"), "name: platform-version-admission\n")
	writeBundleHashText(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBundleHashText(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeBundleHashText(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeBundleHashText(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeBundleHashText(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	return root
}

func rewriteBundleCatalogPackageYAML(t *testing.T, contentYAML, old, replacement string) string {
	t.Helper()

	var archive bundleCatalogContentArchive
	if err := yaml.Unmarshal([]byte(contentYAML), &archive); err != nil {
		t.Fatalf("decode content_yaml: %v", err)
	}
	updatedSize := -1
	for i := range archive.Files {
		if archive.Files[i].Label != "bundle/package.yaml" {
			continue
		}
		raw, err := base64.StdEncoding.DecodeString(archive.Files[i].ContentBase64)
		if err != nil {
			t.Fatalf("decode package.yaml entry: %v", err)
		}
		updated := strings.Replace(string(raw), old, replacement, 1)
		if updated == string(raw) {
			updated = strings.Replace(string(raw), ">=0.7.0 <0.8.0", ">=0.8.0", 1)
		}
		if updated == string(raw) {
			t.Fatalf("package.yaml content did not contain %q", old)
		}
		archive.Files[i].ContentBase64 = base64.StdEncoding.EncodeToString([]byte(updated))
		archive.Files[i].SizeBytes = len(updated)
		updatedSize = len(updated)
	}
	if updatedSize < 0 {
		t.Fatal("content_yaml missing bundle/package.yaml")
	}
	for i := range archive.CanonicalInputs {
		if archive.CanonicalInputs[i].Label == "bundle/package.yaml" {
			archive.CanonicalInputs[i].SizeBytes = updatedSize
		}
	}
	raw, err := yaml.Marshal(archive)
	if err != nil {
		t.Fatalf("encode content_yaml: %v", err)
	}
	return string(raw)
}

func writePlatformVersionCompatibilityPlatformSpec(t *testing.T, repoRoot, version string) string {
	t.Helper()

	raw, err := os.ReadFile(DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("read default platform spec: %v", err)
	}
	updated := strings.Replace(string(raw), "version: 0.7.0", "version: "+version, 1)
	if updated == string(raw) {
		t.Fatal("default platform spec did not contain expected version line")
	}
	path := filepath.Join(t.TempDir(), "platform-spec.yaml")
	if err := os.WriteFile(path, []byte(updated), 0o644); err != nil {
		t.Fatalf("write running platform spec: %v", err)
	}
	return path
}
