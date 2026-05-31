package contracts

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"swarm/internal/platform"
	"swarm/internal/runtime/core/eventidentity"
)

const maxDiscoveredPackageDepth = 99

func contractScopeKey(source ContractItemSource, localID string) string {
	localID = strings.TrimSpace(localID)
	parts := make([]string, 0, 3)
	if pkg := strings.TrimSpace(source.PackageKey); pkg != "" {
		parts = append(parts, pkg)
	}
	if flowID := strings.TrimSpace(source.FlowID); flowID != "" {
		parts = append(parts, flowID)
	}
	if localID != "" {
		parts = append(parts, localID)
	}
	return strings.Join(parts, "::")
}
func contractSameScope(a, b ContractItemSource) bool {
	return strings.TrimSpace(a.PackageKey) == strings.TrimSpace(b.PackageKey) &&
		strings.TrimSpace(a.FlowID) == strings.TrimSpace(b.FlowID) &&
		strings.TrimSpace(a.Layer) == strings.TrimSpace(b.Layer)
}
func ResolveWorkflowContractPaths(repoRoot string) ContractPaths {
	return ResolveWorkflowContractPathsWithOverrides(repoRoot, "", "")
}
func DefaultPlatformSpecFile(repoRoot string) string {
	return platform.DefaultPlatformSpecFile(repoRoot)
}
func defaultAuxFile(repoRoot, envKey string, pathParts ...string) string {
	if env := strings.TrimSpace(os.Getenv(envKey)); env != "" {
		return env
	}
	return filepath.Join(append([]string{repoRoot}, pathParts...)...)
}
func DefaultWorkflowContractsDir(repoRoot string) string {
	if env := strings.TrimSpace(os.Getenv("SWARM_CONTRACTS_DIR")); env != "" {
		return env
	}
	dir := filepath.Join(repoRoot, "contracts")
	if existingFile(filepath.Join(dir, "package.yaml")) != "" {
		return dir
	}
	return ""
}
func RepoRootHasSwarmContracts(repoRoot string) bool {
	return existingFile(filepath.Join(DefaultWorkflowContractsDir(repoRoot), "package.yaml")) != ""
}
func ResolveWorkflowContractPathsWithOverrides(repoRoot, workflowDirOverride, platformSpecFileOverride string) ContractPaths {
	workflowDir := DefaultWorkflowContractsDir(repoRoot)
	overrideActive := strings.TrimSpace(workflowDirOverride) != ""
	if overrideActive {
		workflowDir = workflowDirOverride
	}
	platformSpecFile := DefaultPlatformSpecFile(repoRoot)
	if strings.TrimSpace(platformSpecFileOverride) != "" {
		platformSpecFile = platformSpecFileOverride
	}
	paths := ContractPaths{
		ContractsRoot:         workflowDir,
		WorkflowDir:           workflowDir,
		RootSchemaFile:        existingFile(filepath.Join(workflowDir, "schema.yaml")),
		RootTypesFile:         existingFile(filepath.Join(workflowDir, "types.yaml")),
		RootEntitiesFile:      existingFile(filepath.Join(workflowDir, "entities.yaml")),
		ProjectPackageFile:    existingFile(filepath.Join(workflowDir, "package.yaml")),
		ProjectNodesFile:      existingFile(filepath.Join(workflowDir, "nodes.yaml")),
		ProjectEventsFile:     existingFile(filepath.Join(workflowDir, "events.yaml")),
		ProjectAgentsFile:     existingFile(filepath.Join(workflowDir, "agents.yaml")),
		ProjectToolsFile:      existingFile(filepath.Join(workflowDir, "tools.yaml")),
		ProjectPolicyFile:     existingFile(filepath.Join(workflowDir, "policy.yaml")),
		ProjectPromptsDir:     existingDir(filepath.Join(workflowDir, "prompts")),
		PlatformSpecFile:      platformSpecFile,
		VerificationGatesFile: defaultAuxFile(repoRoot, "SWARM_VERIFICATION_GATES_FILE", "docs", "specs", "swarm-platform", "verification-gates.yaml"),
		ToolingLockFile:       defaultAuxFile(repoRoot, "SWARM_TOOLING_LOCK_FILE", "docs", "specs", "swarm-platform", "tooling.lock"),
		DDLFile:               "",
		AgentConfigMapFile:    defaultAuxFile(repoRoot, "SWARM_AGENT_CONFIG_MAP_FILE", "docs", "specs", "swarm-platform", "agent-config-map.yaml"),
	}
	if paths.ProjectPackageFile != "" {
		paths.ProjectPackages = discoverProjectPackagePaths(paths.ProjectPackageFile, workflowDir)
		for _, pkg := range paths.ProjectPackages {
			paths.Flows = append(paths.Flows, pkg.Flows...)
		}
		sort.Slice(paths.Flows, func(i, j int) bool {
			if paths.Flows[i].ID == paths.Flows[j].ID {
				if paths.Flows[i].PackageKey == paths.Flows[j].PackageKey {
					return strings.Compare(paths.Flows[i].Flow, paths.Flows[j].Flow) < 0
				}
				return strings.Compare(paths.Flows[i].PackageKey, paths.Flows[j].PackageKey) < 0
			}
			return strings.Compare(paths.Flows[i].ID, paths.Flows[j].ID) < 0
		})
	}
	return paths
}
func ContractFilesExist(repoRoot string) []string {
	paths := ResolveWorkflowContractPaths(repoRoot)
	files := []string{
		paths.PlatformSpecFile,
		paths.VerificationGatesFile,
		paths.ToolingLockFile,
		paths.DDLFile,
		paths.RootSchemaFile,
	}
	if paths.ProjectPackageFile != "" {
		files = append(files,
			paths.ProjectPackageFile,
			paths.RootTypesFile,
			paths.RootEntitiesFile,
			paths.ProjectNodesFile,
			paths.ProjectEventsFile,
			paths.ProjectAgentsFile,
		)
		for _, pkg := range paths.ProjectPackages {
			files = append(files,
				pkg.PackageFile,
				pkg.ProjectNodesFile,
				pkg.ProjectEventsFile,
				pkg.ProjectAgentsFile,
				pkg.ProjectToolsFile,
				pkg.ProjectPolicyFile,
			)
		}
		for _, flow := range paths.Flows {
			files = append(files, flow.SchemaFile, flow.TypesFile, flow.EntitiesFile, flow.NodesFile, flow.EventsFile, flow.AgentsFile)
		}
	}
	missing := make([]string, 0)
	for _, path := range files {
		if strings.TrimSpace(path) == "" {
			continue
		}
		if _, err := os.Stat(path); err != nil {
			missing = append(missing, path)
		}
	}
	sort.Strings(missing)
	return missing
}
func existingFile(path string) string {
	if path == "" {
		return ""
	}
	if stat, err := os.Stat(path); err == nil && !stat.IsDir() {
		return path
	}
	return ""
}
func existingDir(path string) string {
	if path == "" {
		return ""
	}
	if stat, err := os.Stat(path); err == nil && stat.IsDir() {
		return path
	}
	return ""
}
func (p ProjectPackageDocument) ChildPackages() []ProjectPackageRef {
	out := make([]ProjectPackageRef, 0, len(p.Packages)+len(p.Children)+len(p.Subpackages))
	out = append(out, p.Packages...)
	out = append(out, p.Children...)
	out = append(out, p.Subpackages...)
	return out
}
func (p ProjectPackageRef) ResolveLocation() string {
	for _, candidate := range []string{p.Path, p.Package, p.Dir} {
		if resolved := strings.TrimSpace(candidate); resolved != "" {
			return resolved
		}
	}
	return ""
}
func discoverProjectPackagePaths(packageFile, workflowDir string) []ProjectPackagePaths {
	rootFile := existingFile(packageFile)
	rootDir := filepath.Dir(rootFile)
	if strings.TrimSpace(rootFile) == "" || strings.TrimSpace(workflowDir) == "" {
		return nil
	}
	visited := map[string]bool{}
	var out []ProjectPackagePaths
	var walk func(packageFile, parentKey string, depth int)
	walk = func(packageFile, parentKey string, depth int) {
		packageFile = existingFile(packageFile)
		if packageFile == "" || visited[packageFile] {
			return
		}
		visited[packageFile] = true

		var manifest ProjectPackageDocument
		manifestErr := loadYAMLFile(packageFile, &manifest)

		packageDir := filepath.Dir(packageFile)
		key := "."
		if rel, err := filepath.Rel(rootDir, packageDir); err == nil && strings.TrimSpace(rel) != "" {
			key = filepath.Clean(rel)
		}
		pkg := ProjectPackagePaths{
			Key:               key,
			ParentKey:         parentKey,
			Depth:             depth,
			Dir:               packageDir,
			PackageFile:       packageFile,
			ProjectNodesFile:  existingFile(filepath.Join(packageDir, "nodes.yaml")),
			ProjectEventsFile: existingFile(filepath.Join(packageDir, "events.yaml")),
			ProjectAgentsFile: existingFile(filepath.Join(packageDir, "agents.yaml")),
			ProjectToolsFile:  existingFile(filepath.Join(packageDir, "tools.yaml")),
			ProjectPolicyFile: existingFile(filepath.Join(packageDir, "policy.yaml")),
			ProjectPromptsDir: existingDir(filepath.Join(packageDir, "prompts")),
		}
		if manifestErr != nil {
			out = append(out, pkg)
			return
		}
		for _, flow := range manifest.Flows {
			flowDirName := strings.TrimSpace(flow.Flow)
			if flowDirName == "" {
				continue
			}
			dir := filepath.Join(packageDir, "flows", flowDirName)
			pkg.Flows = append(pkg.Flows, FlowContractPaths{
				ID:           strings.TrimSpace(flow.ID),
				Flow:         flowDirName,
				Mode:         strings.TrimSpace(flow.Mode),
				Namespace:    strings.TrimSpace(flow.Namespace),
				PackageKey:   pkg.Key,
				PackageDir:   packageDir,
				Dir:          dir,
				DataDir:      existingDir(filepath.Join(dir, "data")),
				SchemaFile:   existingFile(filepath.Join(dir, "schema.yaml")),
				TypesFile:    existingFile(filepath.Join(dir, "types.yaml")),
				EntitiesFile: existingFile(filepath.Join(dir, "entities.yaml")),
				NodesFile:    existingFile(filepath.Join(dir, "nodes.yaml")),
				EventsFile:   existingFile(filepath.Join(dir, "events.yaml")),
				AgentsFile:   existingFile(filepath.Join(dir, "agents.yaml")),
				ToolsFile:    existingFile(filepath.Join(dir, "tools.yaml")),
				PolicyFile:   existingFile(filepath.Join(dir, "policy.yaml")),
				PromptsDir:   existingDir(filepath.Join(dir, "prompts")),
			})
		}
		sort.Slice(pkg.Flows, func(i, j int) bool {
			return strings.Compare(pkg.Flows[i].ID, pkg.Flows[j].ID) < 0
		})
		out = append(out, pkg)

		var flowPackageFiles []string
		for _, flow := range pkg.Flows {
			if flowPackage := existingFile(filepath.Join(flow.Dir, "package.yaml")); flowPackage != "" {
				flowPackageFiles = append(flowPackageFiles, flowPackage)
			}
		}
		sort.Strings(flowPackageFiles)
		for _, flowPackage := range flowPackageFiles {
			walk(flowPackage, pkg.Key, depth+1)
		}

		for _, child := range manifest.ChildPackages() {
			location := child.ResolveLocation()
			if strings.TrimSpace(location) == "" {
				continue
			}
			childPath := filepath.Join(packageDir, location)
			if strings.HasSuffix(strings.ToLower(location), ".yaml") {
				walk(childPath, pkg.Key, depth+1)
				continue
			}
			walk(filepath.Join(childPath, "package.yaml"), pkg.Key, depth+1)
		}
	}
	walk(rootFile, "", 0)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Depth == out[j].Depth {
			return strings.Compare(out[i].Key, out[j].Key) < 0
		}
		return out[i].Depth < out[j].Depth
	})
	return out
}
func validateDiscoveredPackageTree(pkgs []LoadedProjectPackage) error {
	seenPackages := map[string]struct{}{}
	seenFlows := map[string]string{}
	for _, pkg := range pkgs {
		if pkg.Depth > maxDiscoveredPackageDepth {
			return fmt.Errorf("package tree depth %d exceeds max depth %d at %s", pkg.Depth, maxDiscoveredPackageDepth, pkg.Key)
		}
		if _, exists := seenPackages[pkg.Key]; exists {
			return fmt.Errorf("duplicate package key %q discovered in package tree", pkg.Key)
		}
		seenPackages[pkg.Key] = struct{}{}
		for _, flow := range pkg.Paths.Flows {
			flowID := strings.TrimSpace(flow.ID)
			if flowID == "" {
				continue
			}
			if existing, exists := seenFlows[flowID]; exists {
				return fmt.Errorf("duplicate flow id %q discovered in package tree (%s, %s)", flowID, existing, pkg.Key)
			}
			seenFlows[flowID] = pkg.Key
		}
	}
	return nil
}
func cloneSystemNodeContractMap(in map[string]SystemNodeContract) map[string]SystemNodeContract {
	if len(in) == 0 {
		return map[string]SystemNodeContract{}
	}
	out := make(map[string]SystemNodeContract, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
func cloneEventCatalogEntryMap(in map[string]EventCatalogEntry) map[string]EventCatalogEntry {
	if len(in) == 0 {
		return map[string]EventCatalogEntry{}
	}
	out := make(map[string]EventCatalogEntry, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
func cloneAgentRegistryEntryMap(in map[string]AgentRegistryEntry) map[string]AgentRegistryEntry {
	if len(in) == 0 {
		return map[string]AgentRegistryEntry{}
	}
	out := make(map[string]AgentRegistryEntry, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
func cloneToolSchemaEntryMap(in map[string]ToolSchemaEntry) map[string]ToolSchemaEntry {
	if len(in) == 0 {
		return map[string]ToolSchemaEntry{}
	}
	out := make(map[string]ToolSchemaEntry, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
func normalizeStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, item := range in {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out
}
func appendIfMissingString(items []string, value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return items
	}
	for _, item := range items {
		if strings.TrimSpace(item) == value {
			return items
		}
	}
	return append(items, value)
}
func handlerPatternMatches(pattern, eventType string) bool {
	pattern = strings.TrimSpace(pattern)
	eventType = strings.TrimSpace(eventType)
	if pattern == "" || eventType == "" {
		return false
	}
	if pattern == eventType {
		return true
	}
	if !strings.Contains(pattern, "*") {
		return false
	}
	return contractRouteMatches(pattern, eventType)
}

func contractRouteMatches(pattern, eventType string) bool {
	return eventidentity.MatchPattern(pattern, eventType)
}
func sortedContractKeys[T any](m map[string]T) []string {
	if len(m) == 0 {
		return nil
	}
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
func loadYAMLFile(path string, target any) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	if err := yaml.Unmarshal(raw, target); err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}
	return nil
}
func loadOptionalYAMLMap(path string, target any) error {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	return loadYAMLFile(path, target)
}
