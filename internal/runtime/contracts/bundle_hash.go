package contracts

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/division-sh/swarm/internal/packartifact"
	runtimebundleidentity "github.com/division-sh/swarm/internal/runtime/core/bundleidentity"
	"github.com/division-sh/swarm/internal/yamlsource"
	"golang.org/x/text/unicode/norm"
)

const (
	bundleHashV1Prefix  = "bundle-v1:sha256:"
	bundleHashV1Prelude = "swarm-bundle-hash-v1\n"
)

var yamlJSONNumberPattern = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$`)

type bundleHashContentPolicy int

const (
	bundleHashYAML bundleHashContentPolicy = iota
	bundleHashIntent
	bundleHashRaw
)

type bundleHashEntry struct {
	Label         string
	Path          string
	Policy        bundleHashContentPolicy
	SourceExact   []byte
	ExpectedExact []byte
	ExactOwner    string
}

type canonicalJSONNumber float64

// BundleHash is the canonical v1 bundle identity owner.
//
// It implements platform-spec.yaml#multi_bundle_persistence.bundle_identity.canonicalization_v1
// and emits bundle-v1:sha256:<hex>.
func BundleHash(bundle *WorkflowContractBundle) (string, error) {
	entries, err := bundleHashEntries(bundle)
	if err != nil {
		return "", err
	}
	if len(entries) == 0 {
		return "", fmt.Errorf("workflow contract bundle has no identity inputs")
	}

	hasher := sha256.New()
	if _, err := hasher.Write([]byte(bundleHashV1Prelude)); err != nil {
		return "", err
	}
	var length [8]byte
	for _, entry := range entries {
		content, err := canonicalBundleHashEntryContent(entry)
		if err != nil {
			return "", fmt.Errorf("canonicalize %s: %w", entry.Label, err)
		}
		label := []byte(entry.Label)
		binary.BigEndian.PutUint64(length[:], uint64(len(label)))
		hasher.Write(length[:])
		hasher.Write(label)
		binary.BigEndian.PutUint64(length[:], uint64(len(content)))
		hasher.Write(length[:])
		hasher.Write(content)
	}
	return bundleHashV1Prefix + hex.EncodeToString(hasher.Sum(nil)), nil
}

func IsBundleHash(value string) bool {
	return runtimebundleidentity.IsCanonicalHash(value)
}

func ValidateBundleHash(value string) error {
	return runtimebundleidentity.ValidateCanonicalHash(value)
}

func bundleHashEntries(bundle *WorkflowContractBundle) ([]bundleHashEntry, error) {
	if bundle == nil {
		return nil, fmt.Errorf("workflow contract bundle is required")
	}
	paths := bundle.Paths
	contractsRoot, err := canonicalAbsDir(paths.ContractsRoot, "contracts root")
	if err != nil {
		return nil, err
	}

	builder := &bundleHashEntryBuilder{
		contractsRoot: contractsRoot,
		seenPaths:     map[string]struct{}{},
		labels:        map[string]string{},
		foldedLabels:  map[string]string{},
	}
	if err := builder.addRequiredPlatformSpec(paths.PlatformSpecFile); err != nil {
		return nil, err
	}
	if err := builder.addRequiredBundleYAML(paths.ProjectPackageFile); err != nil {
		return nil, err
	}

	for _, path := range []string{
		paths.RootSchemaFile,
		paths.RootTypesFile,
		paths.RootEntitiesFile,
		paths.ProjectNodesFile,
		paths.ProjectEventsFile,
		paths.ProjectAgentsFile,
		paths.ProjectToolsFile,
		paths.ProjectPolicyFile,
	} {
		if err := builder.addOptionalBundleFile(path, bundleHashYAML); err != nil {
			return nil, err
		}
	}
	rootPackagePath := strings.TrimSpace(paths.ProjectPackageFile)
	for _, pkg := range paths.ProjectPackages {
		isRootPackage := rootPackagePath != "" && sameFilePath(pkg.PackageFile, rootPackagePath)
		if !isRootPackage {
			if err := builder.addOptionalBundleFile(pkg.PackageFile, bundleHashYAML); err != nil {
				return nil, err
			}
			for _, path := range []string{
				pkg.ProjectNodesFile,
				pkg.ProjectEventsFile,
				pkg.ProjectAgentsFile,
				pkg.ProjectToolsFile,
				pkg.ProjectPolicyFile,
			} {
				if err := builder.addOptionalBundleFile(path, bundleHashYAML); err != nil {
					return nil, err
				}
			}
		}
		for _, flow := range pkg.Flows {
			if err := builder.addFlow(flow); err != nil {
				return nil, err
			}
		}
	}
	if len(paths.ProjectPackages) == 0 {
		for _, flow := range paths.Flows {
			if err := builder.addFlow(flow); err != nil {
				return nil, err
			}
		}
	}
	if err := builder.addPolicyModuleFiles(bundle); err != nil {
		return nil, err
	}
	if err := builder.addAgentMockModuleFiles(bundle); err != nil {
		return nil, err
	}
	if err := builder.addAgentIntentFiles(bundle); err != nil {
		return nil, err
	}
	if err := builder.addPackInputs(bundle); err != nil {
		return nil, err
	}

	sort.Slice(builder.entries, func(i, j int) bool {
		return builder.entries[i].Label < builder.entries[j].Label
	})
	return builder.entries, nil
}

func (b *bundleHashEntryBuilder) addPackInputs(bundle *WorkflowContractBundle) error {
	if bundle == nil || bundle.PackInventory == nil {
		return nil
	}
	receipt := append([]byte(nil), bundle.PackSelectionBody...)
	if len(receipt) == 0 {
		return fmt.Errorf("workflow contract bundle pack selection receipt is required")
	}
	if err := b.addPackSelectionInput(bundle.PackSelectionPath, receipt); err != nil {
		return err
	}
	for _, file := range bundle.ProjectPacks.Files {
		var err error
		if file.RelativePath == packartifact.ProjectPackManifestLabel {
			err = b.addExactYAMLBundleInput(file.AbsolutePath, file.RelativePath, file.Body, "project pack manifest admission")
		} else {
			err = b.addExactBundleInput(file.AbsolutePath, file.RelativePath, file.Body, "project pack admission")
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *bundleHashEntryBuilder) addPackSelectionInput(filePath string, source []byte) error {
	label := "bundle/" + packartifact.PackSelectionRelativePath
	if err := validateBundleHashLabel(label); err != nil {
		return err
	}
	if _, exists := b.labels[label]; exists {
		return fmt.Errorf("duplicate bundle hash label %q", label)
	}
	folded := asciiFoldBundleHashLabel(label)
	if existing, exists := b.foldedLabels[folded]; exists && existing != label {
		return fmt.Errorf("case-colliding bundle hash labels %q and %q", existing, label)
	}
	if strings.TrimSpace(filePath) != "" {
		abs, err := canonicalContractsRootInput(b.contractsRoot, filePath, "effective pack base selection", false)
		if err != nil {
			return err
		}
		filePath = abs
		b.seenPaths[filePath] = struct{}{}
	}
	b.labels[label] = filePath
	b.foldedLabels[folded] = label
	b.entries = append(b.entries, bundleHashEntry{
		Label: label, Path: filePath, Policy: bundleHashYAML,
		SourceExact: append([]byte(nil), source...), ExactOwner: "effective pack base selection",
	})
	return nil
}

func (b *bundleHashEntryBuilder) addExactBundleInput(filePath, relative string, expected []byte, owner string) error {
	return b.addExactBundleInputWithPolicy(filePath, relative, expected, owner, bundleHashRaw)
}

func (b *bundleHashEntryBuilder) addExactYAMLBundleInput(filePath, relative string, expected []byte, owner string) error {
	return b.addExactBundleInputWithPolicy(filePath, relative, expected, owner, bundleHashYAML)
}

func (b *bundleHashEntryBuilder) addExactBundleInputWithPolicy(filePath, relative string, expected []byte, owner string, policy bundleHashContentPolicy) error {
	relative = filepath.ToSlash(strings.TrimSpace(relative))
	if relative == "" || len(expected) == 0 {
		return fmt.Errorf("%s requires a relative path and exact bytes", owner)
	}
	label := "bundle/" + relative
	if err := validateBundleHashLabel(label); err != nil {
		return err
	}
	if existing, exists := b.labels[label]; exists {
		return fmt.Errorf("duplicate bundle hash label %q for %s and %s", label, existing, filePath)
	}
	folded := asciiFoldBundleHashLabel(label)
	if existing, exists := b.foldedLabels[folded]; exists && existing != label {
		return fmt.Errorf("case-colliding bundle hash labels %q and %q", existing, label)
	}
	if strings.TrimSpace(filePath) != "" {
		abs, err := canonicalContractsRootInput(b.contractsRoot, filePath, owner, false)
		if err != nil {
			return err
		}
		filePath = abs
		if _, exists := b.seenPaths[filePath]; exists {
			return fmt.Errorf("%s %s overlaps another canonical input", owner, relative)
		}
		b.seenPaths[filePath] = struct{}{}
	}
	b.labels[label] = filePath
	b.foldedLabels[folded] = label
	entry := bundleHashEntry{Label: label, Path: filePath, Policy: policy, ExactOwner: owner}
	if policy == bundleHashYAML {
		entry.SourceExact = append([]byte(nil), expected...)
	} else {
		entry.ExpectedExact = append([]byte(nil), expected...)
	}
	b.entries = append(b.entries, entry)
	return nil
}

type bundleHashEntryBuilder struct {
	contractsRoot string
	seenPaths     map[string]struct{}
	labels        map[string]string
	foldedLabels  map[string]string
	entries       []bundleHashEntry
}

func (b *bundleHashEntryBuilder) addFlow(flow FlowContractPaths) error {
	for _, path := range []string{
		flow.SchemaFile,
		flow.TypesFile,
		flow.EntitiesFile,
		flow.NodesFile,
		flow.EventsFile,
		flow.AgentsFile,
		flow.ToolsFile,
		flow.PolicyFile,
	} {
		if err := b.addOptionalBundleFile(path, bundleHashYAML); err != nil {
			return err
		}
	}
	return b.addRecursiveDir(flow.DataDir, bundleHashRaw)
}

func (b *bundleHashEntryBuilder) addAgentIntentFiles(bundle *WorkflowContractBundle) error {
	if bundle == nil {
		return nil
	}
	for _, record := range bundleAgentRecords(bundle) {
		scopedID := contractScopeKey(record.Source, record.LogicalID)
		agent := record.Entry
		if err := agent.ResolvedIntent.Validate(); err != nil {
			return fmt.Errorf("agent %s resolved intent: %w", scopedID, err)
		}
		if agent.ResolvedIntent.Kind != "local" {
			continue
		}
		path := filepath.Join(b.contractsRoot, filepath.FromSlash(agent.ResolvedIntent.Coordinate))
		if err := b.addExactIntentFile(path, []byte(agent.ResolvedIntent.Content)); err != nil {
			return fmt.Errorf("agent %s intent input: %w", scopedID, err)
		}
	}
	return nil
}

func (b *bundleHashEntryBuilder) addExactIntentFile(path string, expected []byte) error {
	abs, err := canonicalContractsRootInput(b.contractsRoot, path, "agent intent input", false)
	if err != nil {
		return err
	}
	label, err := bundleHashBundleLabel(b.contractsRoot, abs)
	if err != nil {
		return err
	}
	if _, exists := b.seenPaths[abs]; exists {
		for i := range b.entries {
			if b.entries[i].Path != abs {
				continue
			}
			if b.entries[i].Policy != bundleHashIntent && b.entries[i].Policy != bundleHashRaw {
				return fmt.Errorf("declared intent %s overlaps canonical input %s with non-intent policy", label, b.entries[i].Label)
			}
			if len(b.entries[i].ExpectedExact) > 0 && !bytes.Equal(b.entries[i].ExpectedExact, expected) {
				return fmt.Errorf("declared intent %s has conflicting exact content owners", label)
			}
			b.entries[i].Policy = bundleHashIntent
			b.entries[i].ExpectedExact = append([]byte(nil), expected...)
			b.entries[i].ExactOwner = mergeBundleHashExactOwners(b.entries[i].ExactOwner, "exact agent intent resolution")
			return nil
		}
		return fmt.Errorf("declared intent %s has no canonical input record", label)
	}
	if err := b.addEntry(abs, label, bundleHashIntent); err != nil {
		return err
	}
	b.entries[len(b.entries)-1].ExpectedExact = append([]byte(nil), expected...)
	b.entries[len(b.entries)-1].ExactOwner = "exact agent intent resolution"
	return nil
}

func (b *bundleHashEntryBuilder) addPolicyModuleFiles(bundle *WorkflowContractBundle) error {
	if bundle == nil {
		return nil
	}
	for _, flow := range bundle.FlowViews() {
		for _, moduleID := range sortedPolicyModuleNames(flow.Policy.Modules) {
			module := flow.Policy.Modules[moduleID]
			path, err := ResolvePolicyModulePath(bundle, module)
			if err != nil {
				return fmt.Errorf("policy module %s: %w", moduleID, err)
			}
			if err := b.addOptionalBundleFile(path, bundleHashRaw); err != nil {
				return err
			}
		}
	}
	return nil
}

// addAgentMockModuleFiles captures every declared agent mock module as a raw
// bundle hash input. Mock modules are behavior-bearing authored content: in
// mock execution mode the module is the agent's behavior, so the bundle
// identity must cover its exact bytes, not just the path string authored in
// agents.yaml.
func (b *bundleHashEntryBuilder) addAgentMockModuleFiles(bundle *WorkflowContractBundle) error {
	if bundle == nil {
		return nil
	}
	for _, record := range bundleAgentRecords(bundle) {
		agent := record.Entry
		agentID, err := DeclaredAgentID(record.LogicalID, agent)
		if err != nil {
			return err
		}
		if !agent.Mock.Configured() {
			continue
		}
		sourcePath := agent.Mock.SourcePath
		if sourcePath == "" {
			return fmt.Errorf("agent %s mock module is configured but has no source path", agentID)
		}
		canonicalSourcePath, pathErr := validateAgentMockModulePath(sourcePath)
		if pathErr != nil {
			return fmt.Errorf("agent %s mock module source path %q is not a canonical contracts-root-relative path: %w", agentID, sourcePath, pathErr)
		}
		if canonicalSourcePath != sourcePath {
			return fmt.Errorf("agent %s mock module source path %q is not canonical; want %q", agentID, sourcePath, canonicalSourcePath)
		}
		if err := validateBundleHashLabel("bundle/" + sourcePath); err != nil {
			return fmt.Errorf("agent %s mock module source path %q is not canonical: %w", agentID, sourcePath, err)
		}
		if clean := filepath.ToSlash(filepath.Clean(filepath.FromSlash(sourcePath))); clean != sourcePath {
			return fmt.Errorf("agent %s mock module source path %q is not canonical; want %q", agentID, sourcePath, clean)
		}
		if len(agent.Mock.Source) == 0 {
			return fmt.Errorf("agent %s mock module %s has no compiled source bytes", agentID, sourcePath)
		}
		sum := sha256.Sum256(agent.Mock.Source)
		compiledDigest := "sha256:" + hex.EncodeToString(sum[:])
		if strings.TrimSpace(agent.Mock.Digest) != compiledDigest {
			return fmt.Errorf("agent %s mock module %s compiled digest %q does not match compiled source bytes %s", agentID, sourcePath, strings.TrimSpace(agent.Mock.Digest), compiledDigest)
		}
		path := filepath.Join(b.contractsRoot, filepath.FromSlash(sourcePath))
		canonicalRel, err := contractsRootRelativePath(b.contractsRoot, path)
		if err != nil {
			return fmt.Errorf("agent %s mock module: %w", agentID, err)
		}
		if canonicalRel != sourcePath {
			return fmt.Errorf("agent %s mock module source path %q resolves as non-canonical %q", agentID, sourcePath, canonicalRel)
		}
		if err := b.addExactAgentMockModuleFile(path, agent.Mock.Source); err != nil {
			return fmt.Errorf("agent %s mock module: %w", agentID, err)
		}
	}
	return nil
}

func (b *bundleHashEntryBuilder) addExactAgentMockModuleFile(path string, expected []byte) error {
	abs, err := canonicalContractsRootInput(b.contractsRoot, path, "agent mock module input", false)
	if err != nil {
		return err
	}
	label, err := bundleHashBundleLabel(b.contractsRoot, abs)
	if err != nil {
		return err
	}
	if _, exists := b.seenPaths[abs]; exists {
		for i := range b.entries {
			if b.entries[i].Path != abs {
				continue
			}
			if b.entries[i].Policy != bundleHashRaw {
				return fmt.Errorf("compiled mock module %s overlaps canonical input %s with non-raw policy", label, b.entries[i].Label)
			}
			if b.entries[i].ExpectedExact != nil && !bytes.Equal(b.entries[i].ExpectedExact, expected) {
				return fmt.Errorf("compiled mock module %s has conflicting exact content owners", label)
			}
			b.entries[i].ExpectedExact = append([]byte(nil), expected...)
			b.entries[i].ExactOwner = mergeBundleHashExactOwners(b.entries[i].ExactOwner, "exact agent mock module compilation")
			return nil
		}
		return fmt.Errorf("compiled mock module %s has no canonical input record", label)
	}
	if err := b.addEntry(abs, label, bundleHashRaw); err != nil {
		return err
	}
	b.entries[len(b.entries)-1].ExpectedExact = append([]byte(nil), expected...)
	b.entries[len(b.entries)-1].ExactOwner = "exact agent mock module compilation"
	return nil
}

func mergeBundleHashExactOwners(existing, owner string) string {
	existing = strings.TrimSpace(existing)
	owner = strings.TrimSpace(owner)
	switch {
	case existing == "":
		return owner
	case owner == "" || existing == owner:
		return existing
	default:
		return existing + " and " + owner
	}
}

func (b *bundleHashEntryBuilder) addRequiredPlatformSpec(path string) error {
	abs, err := canonicalRegularFile(path, "platform spec")
	if err != nil {
		return err
	}
	return b.addEntry(abs, "platform/platform-spec.yaml", bundleHashYAML)
}

func (b *bundleHashEntryBuilder) addRequiredBundleYAML(path string) error {
	abs, err := canonicalContractsRootInput(b.contractsRoot, path, "root package manifest", false)
	if err != nil {
		return err
	}
	label, err := bundleHashBundleLabel(b.contractsRoot, abs)
	if err != nil {
		return err
	}
	return b.addEntry(abs, label, bundleHashYAML)
}

func (b *bundleHashEntryBuilder) addOptionalBundleFile(path string, policy bundleHashContentPolicy) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}
	abs, err := canonicalContractsRootInput(b.contractsRoot, path, "bundle input", false)
	if err != nil {
		return err
	}
	label, err := bundleHashBundleLabel(b.contractsRoot, abs)
	if err != nil {
		return err
	}
	return b.addEntry(abs, label, policy)
}

func (b *bundleHashEntryBuilder) addRecursiveDir(dir string, policy bundleHashContentPolicy) error {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return nil
	}
	absDir, err := canonicalContractsRootInput(b.contractsRoot, dir, "bundle recursive input", true)
	if err != nil {
		return err
	}
	return filepath.WalkDir(absDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d == nil {
			return nil
		}
		name := d.Name()
		if d.IsDir() {
			if path != absDir && ignoredBundleHashDirName(name) {
				return filepath.SkipDir
			}
			return nil
		}
		if ignoredBundleHashFileName(name) {
			return nil
		}
		if d.Type()&fs.ModeSymlink != 0 {
			return fmt.Errorf("bundle canonicalization rejects symlink %s", path)
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		abs, err := filepath.Abs(path)
		if err != nil {
			return err
		}
		label, err := bundleHashBundleLabel(b.contractsRoot, abs)
		if err != nil {
			return err
		}
		return b.addEntry(abs, label, policy)
	})
}

func (b *bundleHashEntryBuilder) addEntry(path, label string, policy bundleHashContentPolicy) error {
	if _, exists := b.seenPaths[path]; exists {
		return nil
	}
	if err := validateBundleHashLabel(label); err != nil {
		return err
	}
	if existing, exists := b.labels[label]; exists {
		return fmt.Errorf("duplicate bundle hash label %q for %s and %s", label, existing, path)
	}
	folded := asciiFoldBundleHashLabel(label)
	if existing, exists := b.foldedLabels[folded]; exists && existing != label {
		return fmt.Errorf("case-colliding bundle hash labels %q and %q", existing, label)
	}
	b.seenPaths[path] = struct{}{}
	b.labels[label] = path
	b.foldedLabels[folded] = label
	b.entries = append(b.entries, bundleHashEntry{Label: label, Path: path, Policy: policy})
	return nil
}

func canonicalRegularFile(path, label string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", fmt.Errorf("%s is required", label)
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("%s path %q: %w", label, path, err)
	}
	info, err := os.Lstat(abs)
	if err != nil {
		return "", fmt.Errorf("%s %s: %w", label, abs, err)
	}
	if info.Mode()&fs.ModeSymlink != 0 {
		return "", fmt.Errorf("%s %s is a symlink", label, abs)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("%s %s is not a regular file", label, abs)
	}
	return filepath.Clean(abs), nil
}

func canonicalAbsDir(path, label string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", fmt.Errorf("%s is required", label)
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("%s path %q: %w", label, path, err)
	}
	info, err := os.Lstat(abs)
	if err != nil {
		return "", fmt.Errorf("%s %s: %w", label, abs, err)
	}
	if info.Mode()&fs.ModeSymlink != 0 {
		return "", fmt.Errorf("%s %s is a symlink", label, abs)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("%s %s is not a directory", label, abs)
	}
	return filepath.Clean(abs), nil
}

func canonicalContractsRootInput(root, path, label string, wantDir bool) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", fmt.Errorf("%s is required", label)
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("%s path %q: %w", label, path, err)
	}
	abs = filepath.Clean(abs)
	rel, err := pathRelativeToRoot(root, abs)
	if err != nil {
		return "", fmt.Errorf("%s %s: %w", label, abs, err)
	}
	info, err := lstatNoSymlinkPath(root, rel, abs)
	if err != nil {
		return "", fmt.Errorf("%s %s: %w", label, abs, err)
	}
	if wantDir {
		if !info.IsDir() {
			return "", fmt.Errorf("%s %s is not a directory", label, abs)
		}
		return abs, nil
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("%s %s is not a regular file", label, abs)
	}
	return abs, nil
}

func bundleHashBundleLabel(root, path string) (string, error) {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return "", err
	}
	if rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("bundle input %s is outside contracts root %s", path, root)
	}
	return "bundle/" + filepath.ToSlash(filepath.Clean(rel)), nil
}

func validateBundleHashLabel(label string) error {
	if label == "" {
		return fmt.Errorf("bundle hash label is empty")
	}
	if !utf8.ValidString(label) {
		return fmt.Errorf("bundle hash label %q is not valid UTF-8", label)
	}
	if norm.NFC.String(label) != label {
		return fmt.Errorf("bundle hash label %q is not NFC-normalized", label)
	}
	if strings.ContainsAny(label, "\x00\\") {
		return fmt.Errorf("bundle hash label %q contains forbidden byte", label)
	}
	for _, segment := range strings.Split(label, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return fmt.Errorf("bundle hash label %q contains invalid segment", label)
		}
	}
	return nil
}

func asciiFoldBundleHashLabel(label string) string {
	buf := []byte(label)
	for i, b := range buf {
		if b >= 'A' && b <= 'Z' {
			buf[i] = b + ('a' - 'A')
		}
	}
	return string(buf)
}

func ignoredBundleHashDirName(name string) bool {
	switch name {
	case ".git", ".hg", ".svn", ".idea", ".vscode", "__pycache__", ".pytest_cache", ".mypy_cache", ".ruff_cache":
		return true
	default:
		return false
	}
}

func ignoredBundleHashFileName(name string) bool {
	switch name {
	case ".DS_Store", "Thumbs.db":
		return true
	}
	return strings.HasSuffix(name, "~") ||
		strings.HasSuffix(name, ".swp") ||
		strings.HasSuffix(name, ".swo") ||
		strings.HasSuffix(name, ".tmp") ||
		strings.HasSuffix(name, ".bak") ||
		strings.HasPrefix(name, ".#")
}

func canonicalBundleHashContent(path string, policy bundleHashContentPolicy) ([]byte, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return canonicalBundleHashRawContent(raw, policy)
}

func canonicalBundleHashEntryContent(entry bundleHashEntry) ([]byte, error) {
	raw, err := bundleHashEntryRawContent(entry)
	if err != nil {
		return nil, err
	}
	content, err := canonicalBundleHashRawContent(raw, entry.Policy)
	if err != nil {
		return nil, err
	}
	if entry.ExpectedExact != nil && !bytes.Equal(content, entry.ExpectedExact) {
		owner := strings.TrimSpace(entry.ExactOwner)
		if owner == "" {
			owner = "exact artifact compilation"
		}
		return nil, fmt.Errorf("canonical input changed after %s", owner)
	}
	return content, nil
}

func bundleHashEntryRawContent(entry bundleHashEntry) ([]byte, error) {
	if entry.SourceExact != nil {
		if strings.TrimSpace(entry.Path) != "" {
			raw, err := os.ReadFile(entry.Path)
			if err != nil {
				return nil, err
			}
			if !bytes.Equal(raw, entry.SourceExact) {
				return nil, fmt.Errorf("canonical input changed after %s", firstNonEmpty(strings.TrimSpace(entry.ExactOwner), "exact source admission"))
			}
		}
		return append([]byte(nil), entry.SourceExact...), nil
	}
	if strings.TrimSpace(entry.Path) == "" {
		if entry.ExpectedExact == nil {
			return nil, fmt.Errorf("canonical input %s has neither a source path nor exact bytes", entry.Label)
		}
		return append([]byte(nil), entry.ExpectedExact...), nil
	}
	raw, err := os.ReadFile(entry.Path)
	if err != nil {
		return nil, err
	}
	return raw, nil
}

func canonicalBundleHashRawContent(raw []byte, policy bundleHashContentPolicy) ([]byte, error) {
	switch policy {
	case bundleHashYAML:
		return canonicalBundleHashYAML(raw)
	case bundleHashIntent:
		if !utf8.Valid(raw) {
			return nil, fmt.Errorf("agent intent text is not valid UTF-8")
		}
		return raw, nil
	case bundleHashRaw:
		return raw, nil
	default:
		return nil, fmt.Errorf("unknown bundle hash content policy %d", policy)
	}
}

func canonicalBundleHashYAML(raw []byte) ([]byte, error) {
	if !utf8.Valid(raw) {
		return nil, fmt.Errorf("YAML input is not valid UTF-8")
	}
	source, err := yamlsource.Load(raw)
	if err != nil {
		return nil, err
	}
	return canonicalBundleHashYAMLSnapshot(source)
}

func canonicalBundleHashYAMLSnapshot(source yamlsource.Snapshot) ([]byte, error) {
	value, err := canonicalBundleHashYAMLValue(source.Root(), map[yamlsource.Node]bool{})
	if err != nil {
		return nil, err
	}
	var out []byte
	out, err = appendCanonicalBundleHashJSON(out, value)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func canonicalBundleHashYAMLValue(node yamlsource.Node, stack map[yamlsource.Node]bool) (any, error) {
	if !node.Valid() {
		return nil, fmt.Errorf("nil YAML node")
	}
	switch node.Kind() {
	case yamlsource.DocumentNode:
		if node.Len() != 1 {
			return nil, fmt.Errorf("YAML document must contain exactly one root node")
		}
		child, _ := node.Child(0)
		return canonicalBundleHashYAMLValue(child, stack)
	case yamlsource.MappingNode:
		if node.Tag() != "" && node.Tag() != "!!map" {
			return nil, fmt.Errorf("unsupported YAML mapping tag %q", node.Tag())
		}
		if node.Len()%2 != 0 {
			return nil, fmt.Errorf("YAML mapping has an odd number of nodes")
		}
		out := make(map[string]any, node.Len()/2)
		for i := 0; i < node.Len(); i += 2 {
			keyNode, _ := node.Child(i)
			keyValue, err := canonicalBundleHashYAMLValue(keyNode, stack)
			if err != nil {
				return nil, err
			}
			key, ok := keyValue.(string)
			if !ok {
				return nil, fmt.Errorf("YAML mapping key is not a string")
			}
			if _, exists := out[key]; exists {
				return nil, fmt.Errorf("duplicate YAML mapping key %q", key)
			}
			valueNode, _ := node.Child(i + 1)
			value, err := canonicalBundleHashYAMLValue(valueNode, stack)
			if err != nil {
				return nil, err
			}
			out[key] = value
		}
		return out, nil
	case yamlsource.SequenceNode:
		if node.Tag() != "" && node.Tag() != "!!seq" {
			return nil, fmt.Errorf("unsupported YAML sequence tag %q", node.Tag())
		}
		out := make([]any, 0, node.Len())
		for i := 0; i < node.Len(); i++ {
			child, _ := node.Child(i)
			value, err := canonicalBundleHashYAMLValue(child, stack)
			if err != nil {
				return nil, err
			}
			out = append(out, value)
		}
		return out, nil
	case yamlsource.AliasNode:
		alias, ok := node.Alias()
		if !ok {
			return nil, fmt.Errorf("YAML alias has no target")
		}
		if stack[alias] {
			return nil, fmt.Errorf("YAML alias cycle detected")
		}
		stack[alias] = true
		value, err := canonicalBundleHashYAMLValue(alias, stack)
		delete(stack, alias)
		return value, err
	case yamlsource.ScalarNode:
		return canonicalBundleHashYAMLScalar(node)
	default:
		return nil, fmt.Errorf("unsupported YAML node kind %d", node.Kind())
	}
}

type canonicalYAMLScalarKind int

const (
	canonicalYAMLString canonicalYAMLScalarKind = iota
	canonicalYAMLBool
	canonicalYAMLNull
	canonicalYAMLNumber
)

func canonicalBundleHashYAMLScalar(node yamlsource.Node) (any, error) {
	explicit := node.Style()&yamlsource.TaggedStyle != 0
	if explicit {
		if node.Tag() != "!!str" && canonicalBundleHashYAMLQuotedScalar(node.Style()) {
			return nil, fmt.Errorf("explicit %s tag widens quoted scalar %q", node.Tag(), node.Value())
		}
		implicitValue, implicitKind, err := canonicalBundleHashImplicitScalar(node.Value())
		if err != nil {
			return nil, err
		}
		switch node.Tag() {
		case "!!str":
			return norm.NFC.String(node.Value()), nil
		case "!!bool":
			if implicitKind != canonicalYAMLBool {
				return nil, fmt.Errorf("explicit bool tag widens scalar %q", node.Value())
			}
			return implicitValue, nil
		case "!!int", "!!float":
			if implicitKind != canonicalYAMLNumber {
				return nil, fmt.Errorf("explicit numeric tag widens scalar %q", node.Value())
			}
			return implicitValue, nil
		case "!!null":
			if implicitKind != canonicalYAMLNull {
				return nil, fmt.Errorf("explicit null tag widens scalar %q", node.Value())
			}
			return nil, nil
		default:
			return nil, fmt.Errorf("unsupported YAML scalar tag %q", node.Tag())
		}
	}
	if node.Tag() == "!!str" {
		return norm.NFC.String(node.Value()), nil
	}
	value, _, err := canonicalBundleHashImplicitScalar(node.Value())
	return value, err
}

func canonicalBundleHashYAMLQuotedScalar(style yamlsource.Style) bool {
	return style&yamlsource.DoubleQuotedStyle != 0 || style&yamlsource.SingleQuotedStyle != 0
}

func canonicalBundleHashImplicitScalar(raw string) (any, canonicalYAMLScalarKind, error) {
	switch raw {
	case "true":
		return true, canonicalYAMLBool, nil
	case "false":
		return false, canonicalYAMLBool, nil
	case "null":
		return nil, canonicalYAMLNull, nil
	}
	switch strings.ToLower(raw) {
	case ".nan", "+.nan", "-.nan", ".inf", "+.inf", "-.inf":
		return nil, canonicalYAMLNumber, fmt.Errorf("non-finite number %q", raw)
	}
	if yamlJSONNumberPattern.MatchString(raw) {
		num, err := parseBundleHashJSONNumber(raw)
		if err != nil {
			return nil, canonicalYAMLNumber, err
		}
		return num, canonicalYAMLNumber, nil
	}
	return norm.NFC.String(raw), canonicalYAMLString, nil
}

func parseBundleHashJSONNumber(raw string) (canonicalJSONNumber, error) {
	if strings.HasPrefix(raw, "-") {
		unsigned := strings.TrimPrefix(raw, "-")
		unsigned = strings.TrimSuffix(unsigned, ".0")
		if unsigned == "0" || strings.HasPrefix(raw, "-0.") || strings.HasPrefix(raw, "-0e") || strings.HasPrefix(raw, "-0E") {
			f, err := strconv.ParseFloat(raw, 64)
			if err == nil && f == 0 {
				return 0, fmt.Errorf("negative zero is not allowed")
			}
		}
	}
	f, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, err
	}
	if math.IsInf(f, 0) || math.IsNaN(f) {
		return 0, fmt.Errorf("non-finite number %q", raw)
	}
	if math.Trunc(f) == f && math.Abs(f) > 9007199254740991 {
		return 0, fmt.Errorf("integer number %q is outside I-JSON safe range", raw)
	}
	return canonicalJSONNumber(f), nil
}

func appendCanonicalBundleHashJSON(out []byte, value any) ([]byte, error) {
	switch typed := value.(type) {
	case nil:
		return append(out, "null"...), nil
	case bool:
		if typed {
			return append(out, "true"...), nil
		}
		return append(out, "false"...), nil
	case string:
		return appendJSONString(out, typed), nil
	case canonicalJSONNumber:
		return append(out, formatBundleHashJSONNumber(float64(typed))...), nil
	case []any:
		out = append(out, '[')
		for i, item := range typed {
			if i > 0 {
				out = append(out, ',')
			}
			var err error
			out, err = appendCanonicalBundleHashJSON(out, item)
			if err != nil {
				return nil, err
			}
		}
		return append(out, ']'), nil
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool {
			return lessJCSString(keys[i], keys[j])
		})
		out = append(out, '{')
		for i, key := range keys {
			if i > 0 {
				out = append(out, ',')
			}
			out = appendJSONString(out, key)
			out = append(out, ':')
			var err error
			out, err = appendCanonicalBundleHashJSON(out, typed[key])
			if err != nil {
				return nil, err
			}
		}
		return append(out, '}'), nil
	default:
		return nil, fmt.Errorf("unsupported canonical JSON value %T", value)
	}
}

func appendJSONString(out []byte, value string) []byte {
	out = append(out, '"')
	for _, r := range value {
		switch r {
		case '\\':
			out = append(out, `\\`...)
		case '"':
			out = append(out, `\"`...)
		case '\b':
			out = append(out, `\b`...)
		case '\t':
			out = append(out, `\t`...)
		case '\n':
			out = append(out, `\n`...)
		case '\f':
			out = append(out, `\f`...)
		case '\r':
			out = append(out, `\r`...)
		default:
			if r < 0x20 {
				out = append(out, `\u00`...)
				out = append(out, "0123456789abcdef"[byte(r)>>4])
				out = append(out, "0123456789abcdef"[byte(r)&0x0f])
				continue
			}
			out = utf8.AppendRune(out, r)
		}
	}
	return append(out, '"')
}

func lessJCSString(left, right string) bool {
	l := utf16.Encode([]rune(left))
	r := utf16.Encode([]rune(right))
	for i := 0; i < len(l) && i < len(r); i++ {
		if l[i] != r[i] {
			return l[i] < r[i]
		}
	}
	return len(l) < len(r)
}

func formatBundleHashJSONNumber(f float64) string {
	if f == 0 {
		return "0"
	}
	abs := math.Abs(f)
	if abs >= 1e-6 && abs < 1e21 {
		return strconv.FormatFloat(f, 'f', -1, 64)
	}
	s := strconv.FormatFloat(f, 'e', -1, 64)
	parts := strings.SplitN(s, "e", 2)
	mantissa := strings.TrimRight(strings.TrimRight(parts[0], "0"), ".")
	exp := parts[1]
	sign := "+"
	if strings.HasPrefix(exp, "-") {
		sign = "-"
		exp = strings.TrimPrefix(exp, "-")
	} else {
		exp = strings.TrimPrefix(exp, "+")
	}
	exp = strings.TrimLeft(exp, "0")
	if exp == "" {
		exp = "0"
	}
	return mantissa + "e" + sign + exp
}
