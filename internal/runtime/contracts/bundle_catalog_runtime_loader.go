package contracts

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

type BundleCatalogRuntimeLoadRequest struct {
	BundleHash  string
	ContentYAML string
	DataBlob    []byte
}

type BundleCatalogRuntimeSource struct {
	BundleHash       string
	ContractsRoot    string
	PlatformSpecPath string
	Bundle           *WorkflowContractBundle
	tempRoot         string
}

type bundleCatalogContentArchive struct {
	ProjectionVersion string                       `yaml:"projection_version"`
	Files             []bundleCatalogDataEntry     `yaml:"files"`
	CanonicalInputs   []bundleCatalogProjectedFile `yaml:"canonical_inputs"`
}

// LoadBundleCatalogRuntimeSource reconstructs a runtime-loadable bundle from a
// persisted bundle catalog row. The stored archives remain the canonical source
// bytes; parsed_json is intentionally not accepted as runtime authority.
func LoadBundleCatalogRuntimeSource(repoRoot string, req BundleCatalogRuntimeLoadRequest) (BundleCatalogRuntimeSource, error) {
	req.BundleHash = strings.TrimSpace(req.BundleHash)
	if err := ValidateBundleHash(req.BundleHash); err != nil {
		return BundleCatalogRuntimeSource{}, err
	}
	content, err := decodeBundleCatalogContentArchive(req.ContentYAML)
	if err != nil {
		return BundleCatalogRuntimeSource{}, err
	}
	dataEntries, err := decodeBundleCatalogRuntimeDataBlob(req.DataBlob)
	if err != nil {
		return BundleCatalogRuntimeSource{}, err
	}
	files, err := bundleCatalogRuntimeFiles(content, dataEntries)
	if err != nil {
		return BundleCatalogRuntimeSource{}, err
	}

	tempRoot, err := os.MkdirTemp("", "swarm-bundle-catalog-*")
	if err != nil {
		return BundleCatalogRuntimeSource{}, fmt.Errorf("create bundle catalog runtime root: %w", err)
	}
	if err := os.Chmod(tempRoot, 0o755); err != nil {
		_ = os.RemoveAll(tempRoot)
		return BundleCatalogRuntimeSource{}, fmt.Errorf("make bundle catalog runtime root readable: %w", err)
	}
	cleanupOnError := true
	defer func() {
		if cleanupOnError {
			_ = os.RemoveAll(tempRoot)
		}
	}()

	contractsRoot := filepath.Join(tempRoot, "contracts")
	platformSpecPath := filepath.Join(tempRoot, "platform", "platform-spec.yaml")
	if err := materializeBundleCatalogRuntimeFiles(tempRoot, files); err != nil {
		return BundleCatalogRuntimeSource{}, err
	}
	if _, err := os.Stat(filepath.Join(contractsRoot, "package.yaml")); err != nil {
		return BundleCatalogRuntimeSource{}, fmt.Errorf("bundle catalog runtime source missing bundle/package.yaml: %w", err)
	}
	if _, err := os.Stat(platformSpecPath); err != nil {
		return BundleCatalogRuntimeSource{}, fmt.Errorf("bundle catalog runtime source missing platform/platform-spec.yaml: %w", err)
	}

	bundle, err := LoadWorkflowContractBundleWithOverrides(repoRoot, contractsRoot, platformSpecPath)
	if err != nil {
		return BundleCatalogRuntimeSource{}, fmt.Errorf("load bundle catalog runtime source: %w", err)
	}
	if err := ValidateBundlePlatformVersionCompatibility(bundle); err != nil {
		return BundleCatalogRuntimeSource{}, fmt.Errorf("admit bundle catalog runtime source: %w", err)
	}
	gotHash, err := BundleHash(bundle)
	if err != nil {
		return BundleCatalogRuntimeSource{}, fmt.Errorf("verify bundle catalog runtime hash: %w", err)
	}
	if gotHash != req.BundleHash {
		return BundleCatalogRuntimeSource{}, fmt.Errorf("bundle catalog runtime hash mismatch: reconstructed %s, requested %s", gotHash, req.BundleHash)
	}

	cleanupOnError = false
	return BundleCatalogRuntimeSource{
		BundleHash:       req.BundleHash,
		ContractsRoot:    contractsRoot,
		PlatformSpecPath: platformSpecPath,
		Bundle:           bundle,
		tempRoot:         tempRoot,
	}, nil
}

func (s BundleCatalogRuntimeSource) Cleanup() error {
	if strings.TrimSpace(s.tempRoot) == "" {
		return nil
	}
	return os.RemoveAll(s.tempRoot)
}

func decodeBundleCatalogContentArchive(contentYAML string) (bundleCatalogContentArchive, error) {
	contentYAML = strings.TrimSpace(contentYAML)
	if contentYAML == "" {
		return bundleCatalogContentArchive{}, fmt.Errorf("bundle catalog content_yaml is required")
	}
	var archive bundleCatalogContentArchive
	if err := yaml.Unmarshal([]byte(contentYAML), &archive); err != nil {
		return bundleCatalogContentArchive{}, fmt.Errorf("decode bundle catalog content_yaml: %w", err)
	}
	if strings.TrimSpace(archive.ProjectionVersion) != bundleCatalogProjectionVersion {
		return bundleCatalogContentArchive{}, fmt.Errorf("bundle catalog content_yaml projection_version = %q, want %q", archive.ProjectionVersion, bundleCatalogProjectionVersion)
	}
	if len(archive.CanonicalInputs) == 0 {
		return bundleCatalogContentArchive{}, fmt.Errorf("bundle catalog content_yaml canonical_inputs is empty")
	}
	return archive, nil
}

func decodeBundleCatalogRuntimeDataBlob(dataBlob []byte) ([]bundleCatalogDataEntry, error) {
	if len(dataBlob) == 0 {
		return nil, nil
	}
	var archive bundleCatalogDataArchive
	if err := json.Unmarshal(dataBlob, &archive); err != nil {
		return nil, fmt.Errorf("decode bundle catalog data_blob: %w", err)
	}
	if strings.TrimSpace(archive.ProjectionVersion) != bundleCatalogProjectionVersion {
		return nil, fmt.Errorf("bundle catalog data_blob projection_version = %q, want %q", archive.ProjectionVersion, bundleCatalogProjectionVersion)
	}
	return archive.Entries, nil
}

func bundleCatalogRuntimeFiles(content bundleCatalogContentArchive, dataEntries []bundleCatalogDataEntry) (map[string][]byte, error) {
	inputs := make(map[string]bundleCatalogProjectedFile, len(content.CanonicalInputs))
	for _, input := range content.CanonicalInputs {
		input.Label = strings.TrimSpace(input.Label)
		input.Policy = strings.TrimSpace(input.Policy)
		if err := validateBundleCatalogRuntimeInput(input); err != nil {
			return nil, err
		}
		if _, exists := inputs[input.Label]; exists {
			return nil, fmt.Errorf("duplicate bundle catalog canonical input %q", input.Label)
		}
		inputs[input.Label] = input
	}

	files := make(map[string][]byte, len(inputs))
	if err := appendBundleCatalogRuntimeEntries(files, inputs, content.Files, false); err != nil {
		return nil, err
	}
	if err := appendBundleCatalogRuntimeEntries(files, inputs, dataEntries, true); err != nil {
		return nil, err
	}

	for label := range inputs {
		if _, exists := files[label]; !exists {
			return nil, fmt.Errorf("bundle catalog runtime source missing canonical input %q", label)
		}
	}
	return files, nil
}

func validateBundleCatalogRuntimeInput(input bundleCatalogProjectedFile) error {
	if err := validateBundleHashLabel(input.Label); err != nil {
		return err
	}
	switch input.Policy {
	case "yaml", "prompt_text", "raw_data":
	default:
		return fmt.Errorf("bundle catalog canonical input %q has unsupported policy %q", input.Label, input.Policy)
	}
	if input.SizeBytes < 0 {
		return fmt.Errorf("bundle catalog canonical input %q has negative size", input.Label)
	}
	return nil
}

func appendBundleCatalogRuntimeEntries(files map[string][]byte, inputs map[string]bundleCatalogProjectedFile, entries []bundleCatalogDataEntry, wantRaw bool) error {
	for _, entry := range entries {
		label := strings.TrimSpace(entry.Label)
		input, exists := inputs[label]
		if !exists {
			return fmt.Errorf("bundle catalog runtime entry %q is not listed in canonical_inputs", label)
		}
		isRaw := input.Policy == "raw_data"
		if wantRaw != isRaw {
			return fmt.Errorf("bundle catalog runtime entry %q stored in wrong archive for policy %q", label, input.Policy)
		}
		if _, duplicate := files[label]; duplicate {
			return fmt.Errorf("duplicate bundle catalog runtime entry %q", label)
		}
		content, err := base64.StdEncoding.DecodeString(strings.TrimSpace(entry.ContentBase64))
		if err != nil {
			return fmt.Errorf("decode bundle catalog runtime entry %q: %w", label, err)
		}
		if entry.SizeBytes != len(content) || input.SizeBytes != len(content) {
			return fmt.Errorf("bundle catalog runtime entry %q size mismatch: entry=%d canonical=%d decoded=%d", label, entry.SizeBytes, input.SizeBytes, len(content))
		}
		files[label] = content
	}
	return nil
}

func materializeBundleCatalogRuntimeFiles(tempRoot string, files map[string][]byte) error {
	labels := make([]string, 0, len(files))
	for label := range files {
		labels = append(labels, label)
	}
	sort.Strings(labels)
	for _, label := range labels {
		target, err := bundleCatalogRuntimeFilePath(tempRoot, label)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return fmt.Errorf("create bundle catalog runtime dir for %s: %w", label, err)
		}
		if err := os.WriteFile(target, files[label], 0o644); err != nil {
			return fmt.Errorf("write bundle catalog runtime file %s: %w", label, err)
		}
	}
	return nil
}

func bundleCatalogRuntimeFilePath(tempRoot, label string) (string, error) {
	if err := validateBundleHashLabel(label); err != nil {
		return "", err
	}
	switch {
	case label == "platform/platform-spec.yaml":
		return filepath.Join(tempRoot, "platform", "platform-spec.yaml"), nil
	case strings.HasPrefix(label, "bundle/"):
		rel := strings.TrimPrefix(label, "bundle/")
		if rel == "" {
			return "", fmt.Errorf("bundle catalog runtime label %q has empty bundle path", label)
		}
		return filepath.Join(tempRoot, "contracts", filepath.FromSlash(rel)), nil
	default:
		return "", fmt.Errorf("bundle catalog runtime label %q is outside supported platform/bundle namespaces", label)
	}
}
