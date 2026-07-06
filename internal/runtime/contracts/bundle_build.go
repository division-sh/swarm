package contracts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	bundleBuildManifestAPIVersion = "swarm.bundle.build.v1"
	bundleBuildStepWasmModules    = "wasm_policy_modules"
	bundleBuildKindWasm           = "wasm"
)

type BundleBuildRequest struct {
	RepoRoot         string
	ContractsRoot    string
	PlatformSpecPath string
	OutputRoot       string
	Steps            []BundleBuildStep
}

type BundleBuildStep interface {
	Name() string
	Run(context.Context, *BundleBuildContext) error
}

type BundleBuildContext struct {
	Bundle          *WorkflowContractBundle
	BundleHash      string
	ContractsRoot   string
	PlatformSpec    string
	Inputs          []BundleBuildInput
	Modules         []BundleBuildModule
	MaterializedDir string
}

type BundleBuildReport struct {
	APIVersion      string                  `json:"api_version"`
	Status          string                  `json:"status"`
	DriftStatus     string                  `json:"drift_status"`
	BundleHash      string                  `json:"bundle_hash"`
	OutputPath      string                  `json:"output_path"`
	ManifestPath    string                  `json:"manifest_path"`
	PlatformVersion string                  `json:"platform_version"`
	SpecVersion     string                  `json:"spec_version"`
	Steps           []BundleBuildStepReport `json:"steps"`
	Modules         []BundleBuildModule     `json:"modules"`
	Inputs          []BundleBuildInput      `json:"inputs"`
	Diagnostics     []BundleBuildDiagnostic `json:"diagnostics"`
}

type BundleBuildManifest struct {
	APIVersion      string              `json:"api_version"`
	BundleHash      string              `json:"bundle_hash"`
	PlatformVersion string              `json:"platform_version"`
	SpecVersion     string              `json:"spec_version"`
	Modules         []BundleBuildModule `json:"modules"`
	Inputs          []BundleBuildInput  `json:"inputs"`
}

type BundleBuildModule struct {
	ID         string `json:"id"`
	FlowID     string `json:"flow_id,omitempty"`
	Kind       string `json:"kind"`
	Path       string `json:"path"`
	Digest     string `json:"digest"`
	SourcePath string `json:"source_path,omitempty"`
	SourceHash string `json:"source_hash"`
}

type BundleBuildInput struct {
	Label     string `json:"label"`
	Path      string `json:"path"`
	Policy    string `json:"policy"`
	SizeBytes int64  `json:"size_bytes"`
}

type BundleBuildStepReport struct {
	Name   string `json:"name"`
	Status string `json:"status"`
}

type BundleBuildDiagnostic struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func DefaultBundleBuildSteps() []BundleBuildStep {
	return []BundleBuildStep{wasmPolicyModulesBuildStep{}}
}

func BundleBuildStepNames(steps []BundleBuildStep) []string {
	steps = normalizedBundleBuildSteps(steps)
	names := make([]string, 0, len(steps))
	for _, step := range steps {
		if step == nil {
			continue
		}
		names = append(names, step.Name())
	}
	return names
}

func BuildBundleMaterialization(ctx context.Context, req BundleBuildRequest) (BundleBuildReport, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	contractsRoot, err := canonicalAbsDir(req.ContractsRoot, "contracts root")
	if err != nil {
		return BundleBuildReport{}, err
	}
	outputRoot, err := canonicalOutputDir(req.OutputRoot)
	if err != nil {
		return BundleBuildReport{}, err
	}
	bundle, err := LoadWorkflowContractBundleWithOverrides(req.RepoRoot, contractsRoot, req.PlatformSpecPath)
	if err != nil {
		return BundleBuildReport{}, err
	}
	if err := ValidateBundlePlatformVersionCompatibility(bundle); err != nil {
		return BundleBuildReport{}, err
	}
	bundleHash, err := BundleHash(bundle)
	if err != nil {
		return BundleBuildReport{}, err
	}
	entries, err := bundleHashEntries(bundle)
	if err != nil {
		return BundleBuildReport{}, err
	}
	inputs, err := bundleBuildInputs(entries)
	if err != nil {
		return BundleBuildReport{}, err
	}
	modules, err := collectBundleBuildModules(bundle)
	if err != nil {
		return BundleBuildReport{}, err
	}

	target := filepath.Join(outputRoot, bundleHash)
	if err := os.MkdirAll(outputRoot, 0o755); err != nil {
		return BundleBuildReport{}, fmt.Errorf("create bundle build output root: %w", err)
	}
	tmp, err := os.MkdirTemp(outputRoot, ".tmp-bundle-build-")
	if err != nil {
		return BundleBuildReport{}, fmt.Errorf("create bundle build temp dir: %w", err)
	}
	cleanupTmp := true
	defer func() {
		if cleanupTmp {
			_ = os.RemoveAll(tmp)
		}
	}()

	ctxData := &BundleBuildContext{
		Bundle:          bundle,
		BundleHash:      bundleHash,
		ContractsRoot:   contractsRoot,
		PlatformSpec:    req.PlatformSpecPath,
		Inputs:          inputs,
		Modules:         modules,
		MaterializedDir: tmp,
	}
	stepReports := make([]BundleBuildStepReport, 0)
	for _, step := range normalizedBundleBuildSteps(req.Steps) {
		if step == nil {
			continue
		}
		if err := step.Run(ctx, ctxData); err != nil {
			return BundleBuildReport{}, fmt.Errorf("bundle build step %s: %w", step.Name(), err)
		}
		stepReports = append(stepReports, BundleBuildStepReport{Name: step.Name(), Status: "passed"})
	}

	if err := materializeBundleInputs(entries, tmp); err != nil {
		return BundleBuildReport{}, err
	}
	if err := materializeBundleSourceInputs(contractsRoot, modules, tmp); err != nil {
		return BundleBuildReport{}, err
	}
	manifest := BundleBuildManifest{
		APIVersion:      bundleBuildManifestAPIVersion,
		BundleHash:      bundleHash,
		PlatformVersion: strings.TrimSpace(bundle.Platform.Platform.Version),
		SpecVersion:     bundleBuildManifestAPIVersion,
		Modules:         modules,
		Inputs:          inputs,
	}
	manifestPath := filepath.Join(tmp, "build-manifest.json")
	if err := writeDeterministicJSONFile(manifestPath, manifest); err != nil {
		return BundleBuildReport{}, fmt.Errorf("write build manifest: %w", err)
	}
	materialized, err := LoadWorkflowContractBundleWithOverrides(req.RepoRoot, tmp, req.PlatformSpecPath)
	if err != nil {
		return BundleBuildReport{}, fmt.Errorf("validate materialized contracts root: %w", err)
	}
	materializedHash, err := BundleHash(materialized)
	if err != nil {
		return BundleBuildReport{}, fmt.Errorf("hash materialized contracts root: %w", err)
	}
	if materializedHash != bundleHash {
		return BundleBuildReport{}, fmt.Errorf("materialized bundle hash mismatch: source %s materialized %s", bundleHash, materializedHash)
	}
	if err := os.RemoveAll(target); err != nil {
		return BundleBuildReport{}, fmt.Errorf("replace bundle build output %s: %w", target, err)
	}
	if err := os.Rename(tmp, target); err != nil {
		return BundleBuildReport{}, fmt.Errorf("move bundle build output %s: %w", target, err)
	}
	cleanupTmp = false
	finalManifestPath := filepath.Join(target, "build-manifest.json")
	return BundleBuildReport{
		APIVersion:      bundleBuildManifestAPIVersion,
		Status:          "passed",
		DriftStatus:     "clean",
		BundleHash:      bundleHash,
		OutputPath:      target,
		ManifestPath:    finalManifestPath,
		PlatformVersion: strings.TrimSpace(bundle.Platform.Platform.Version),
		SpecVersion:     bundleBuildManifestAPIVersion,
		Steps:           stepReports,
		Modules:         modules,
		Inputs:          inputs,
		Diagnostics:     []BundleBuildDiagnostic{},
	}, nil
}

func normalizedBundleBuildSteps(steps []BundleBuildStep) []BundleBuildStep {
	if steps == nil {
		return DefaultBundleBuildSteps()
	}
	out := make([]BundleBuildStep, 0, len(steps))
	for _, step := range steps {
		if step != nil {
			out = append(out, step)
		}
	}
	return out
}

type wasmPolicyModulesBuildStep struct{}

func (wasmPolicyModulesBuildStep) Name() string {
	return bundleBuildStepWasmModules
}

func (wasmPolicyModulesBuildStep) Run(_ context.Context, ctx *BundleBuildContext) error {
	if ctx == nil {
		return fmt.Errorf("bundle build context is required")
	}
	for _, module := range ctx.Modules {
		if module.Kind != bundleBuildKindWasm {
			return fmt.Errorf("unsupported module kind %q for policy module %s", module.Kind, module.ID)
		}
	}
	return nil
}

func canonicalOutputDir(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", fmt.Errorf("bundle build output root is required")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("bundle build output root %q: %w", path, err)
	}
	return filepath.Clean(abs), nil
}

func bundleBuildInputs(entries []bundleHashEntry) ([]BundleBuildInput, error) {
	inputs := make([]BundleBuildInput, 0, len(entries))
	for _, entry := range entries {
		if entry.Label == "platform/platform-spec.yaml" {
			continue
		}
		rel, ok := strings.CutPrefix(entry.Label, "bundle/")
		if !ok {
			return nil, fmt.Errorf("bundle build input %q is not under bundle/", entry.Label)
		}
		info, err := os.Stat(entry.Path)
		if err != nil {
			return nil, fmt.Errorf("stat bundle build input %s: %w", rel, err)
		}
		inputs = append(inputs, BundleBuildInput{
			Label:     entry.Label,
			Path:      rel,
			Policy:    bundleCatalogPolicyName(entry.Policy),
			SizeBytes: info.Size(),
		})
	}
	return inputs, nil
}

func materializeBundleInputs(entries []bundleHashEntry, outputDir string) error {
	for _, entry := range entries {
		if entry.Label == "platform/platform-spec.yaml" {
			continue
		}
		rel, ok := strings.CutPrefix(entry.Label, "bundle/")
		if !ok {
			return fmt.Errorf("bundle build input %q is not under bundle/", entry.Label)
		}
		dst := filepath.Join(outputDir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return fmt.Errorf("create materialized input dir %s: %w", filepath.Dir(dst), err)
		}
		raw, err := os.ReadFile(entry.Path)
		if err != nil {
			return fmt.Errorf("read bundle build input %s: %w", rel, err)
		}
		if err := os.WriteFile(dst, raw, 0o644); err != nil {
			return fmt.Errorf("write materialized input %s: %w", rel, err)
		}
	}
	return nil
}

func materializeBundleSourceInputs(contractsRoot string, modules []BundleBuildModule, outputDir string) error {
	for _, module := range modules {
		sourcePath := strings.TrimSpace(module.SourcePath)
		if sourcePath == "" {
			continue
		}
		src := filepath.Join(contractsRoot, filepath.FromSlash(sourcePath))
		dst := filepath.Join(outputDir, filepath.FromSlash(sourcePath))
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return fmt.Errorf("create materialized source dir %s: %w", filepath.Dir(dst), err)
		}
		raw, err := os.ReadFile(src)
		if err != nil {
			return fmt.Errorf("read policy module source %s: %w", sourcePath, err)
		}
		if err := os.WriteFile(dst, raw, 0o644); err != nil {
			return fmt.Errorf("write materialized source %s: %w", sourcePath, err)
		}
	}
	return nil
}

func collectBundleBuildModules(bundle *WorkflowContractBundle) ([]BundleBuildModule, error) {
	if bundle == nil {
		return nil, nil
	}
	modules := make([]BundleBuildModule, 0)
	for _, flow := range bundle.FlowViews() {
		flowID := strings.TrimSpace(flow.Paths.ID)
		for _, moduleID := range sortedPolicyModuleNames(flow.Policy.Modules) {
			module := flow.Policy.Modules[moduleID]
			path, err := ResolvePolicyModulePath(bundle, module)
			if err != nil {
				return nil, fmt.Errorf("policy module %s: %w", moduleID, err)
			}
			relPath, err := contractsRootRelativePath(bundle.Paths.ContractsRoot, path)
			if err != nil {
				return nil, fmt.Errorf("policy module %s path: %w", moduleID, err)
			}
			sourcePath := strings.TrimSpace(module.SourcePath)
			sourceHash := strings.TrimSpace(module.SourceHash)
			if sourcePath != "" || sourceHash != "" {
				sourceRel, verifiedHash, err := verifyPolicyModuleSourceHash(bundle, moduleID, module)
				if err != nil {
					return nil, err
				}
				sourcePath = sourceRel
				sourceHash = verifiedHash
			}
			modules = append(modules, BundleBuildModule{
				ID:         moduleID,
				FlowID:     flowID,
				Kind:       bundleBuildKindWasm,
				Path:       relPath,
				Digest:     strings.TrimSpace(module.Digest),
				SourcePath: sourcePath,
				SourceHash: sourceHash,
			})
		}
	}
	sort.Slice(modules, func(i, j int) bool {
		if modules[i].FlowID == modules[j].FlowID {
			return modules[i].ID < modules[j].ID
		}
		return modules[i].FlowID < modules[j].FlowID
	})
	return modules, nil
}

func verifyPolicyModuleSourceHash(bundle *WorkflowContractBundle, moduleID string, module PolicyModule) (string, string, error) {
	sourcePath := strings.TrimSpace(module.SourcePath)
	sourceHash := strings.TrimSpace(module.SourceHash)
	if sourcePath == "" {
		return "", "", fmt.Errorf("policy module %s source_hash requires source_path", moduleID)
	}
	if !computeModuleDigestPattern.MatchString(sourceHash) {
		return "", "", fmt.Errorf("policy module %s source_hash must be sha256:<64 lowercase hex>", moduleID)
	}
	path, err := resolvePolicyModuleSourcePath(bundle, module)
	if err != nil {
		return "", "", fmt.Errorf("policy module %s: %w", moduleID, err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", "", fmt.Errorf("read policy module %s source_path: %w", moduleID, err)
	}
	sum := sha256.Sum256(raw)
	actual := "sha256:" + hex.EncodeToString(sum[:])
	if sourceHash != actual {
		return "", "", fmt.Errorf("policy module %s stale source_hash %s does not match source_path bytes %s", moduleID, sourceHash, actual)
	}
	rel, err := contractsRootRelativePath(bundle.Paths.ContractsRoot, path)
	if err != nil {
		return "", "", err
	}
	return rel, sourceHash, nil
}

func resolvePolicyModuleSourcePath(bundle *WorkflowContractBundle, module PolicyModule) (string, error) {
	if bundle == nil {
		return "", fmt.Errorf("workflow contract bundle is required")
	}
	root := strings.TrimSpace(bundle.Paths.ContractsRoot)
	if root == "" {
		return "", fmt.Errorf("contracts root is required for compute_module source_path")
	}
	sourcePath := strings.TrimSpace(module.SourcePath)
	if sourcePath == "" {
		return "", fmt.Errorf("source_path is required")
	}
	if filepath.IsAbs(sourcePath) {
		return "", fmt.Errorf("source_path %q must be relative to the contracts root", sourcePath)
	}
	clean := filepath.Clean(filepath.FromSlash(sourcePath))
	if clean == "." || clean == "" || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("source_path %q must remain inside the contracts root", sourcePath)
	}
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	pathAbs, err := filepath.Abs(filepath.Join(rootAbs, clean))
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(rootAbs, pathAbs)
	if err != nil {
		return "", err
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("source_path %q must remain inside the contracts root", sourcePath)
	}
	stat, err := lstatNoSymlinkPath(rootAbs, rel, sourcePath)
	if err != nil {
		return "", fmt.Errorf("source_path %q: %w", sourcePath, err)
	}
	if !stat.Mode().IsRegular() {
		return "", fmt.Errorf("source_path %q must be a regular file", sourcePath)
	}
	return pathAbs, nil
}

func contractsRootRelativePath(root, path string) (string, error) {
	rootAbs, err := filepath.Abs(strings.TrimSpace(root))
	if err != nil {
		return "", err
	}
	pathAbs, err := filepath.Abs(strings.TrimSpace(path))
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(rootAbs, pathAbs)
	if err != nil {
		return "", err
	}
	if rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("%s is outside contracts root %s", pathAbs, rootAbs)
	}
	return filepath.ToSlash(filepath.Clean(rel)), nil
}

func writeDeterministicJSONFile(path string, value any) error {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, raw, 0o644)
}
