package cliapp

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/division-sh/swarm/internal/config"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/yamlsource"
	"gopkg.in/yaml.v3"
)

const unifiedConfigOwner = "platform-spec.yaml#configuration_source_authority.unified_swarm_config"

type unifiedConfigLayerName string

const (
	unifiedLayerExplicit      unifiedConfigLayerName = "explicit_config"
	unifiedLayerLocalOperator unifiedConfigLayerName = "local_operator_config"
	unifiedLayerProject       unifiedConfigLayerName = "project_config"
	unifiedLayerUserGlobal    unifiedConfigLayerName = "user_global_config"
)

type unifiedConfigLoadOptions struct {
	RepoRoot        string
	ExplicitPath    string
	BackendOverride string
}

type unifiedConfigLoadResult struct {
	Config      *config.Config
	CLI         cliCommandConfig
	Source      string
	Path        string
	Layers      []unifiedConfigLayer
	KeyOrigins  map[string]unifiedConfigKeyOrigin
	Diagnostics []unifiedConfigDiagnostic
}

type unifiedConfigKeyOrigin struct {
	Layer unifiedConfigLayerName
	Path  string
}

type unifiedConfigLayer struct {
	Name     unifiedConfigLayerName `json:"name"`
	Path     string                 `json:"path"`
	Explicit bool                   `json:"explicit"`
}

type unifiedConfigDiagnosticKind string

const (
	unifiedConfigDiagnosticLoaded            unifiedConfigDiagnosticKind = "loaded"
	unifiedConfigDiagnosticUnknownKey        unifiedConfigDiagnosticKind = "unknown_key"
	unifiedConfigDiagnosticTrustRejected     unifiedConfigDiagnosticKind = "trust_rejected"
	unifiedConfigDiagnosticSplitUnsupported  unifiedConfigDiagnosticKind = "split_unsupported"
	unifiedConfigDiagnosticPathViolation     unifiedConfigDiagnosticKind = "path_violation"
	unifiedConfigDiagnosticReadFailed        unifiedConfigDiagnosticKind = "read_failed"
	unifiedConfigDiagnosticParseFailed       unifiedConfigDiagnosticKind = "parse_failed"
	unifiedConfigDiagnosticUnsupportedSource unifiedConfigDiagnosticKind = "unsupported_source"
	unifiedConfigDiagnosticValidationFailed  unifiedConfigDiagnosticKind = "validation_failed"
)

type unifiedConfigDiagnostic struct {
	Kind         unifiedConfigDiagnosticKind `json:"kind"`
	Layer        unifiedConfigLayerName      `json:"layer,omitempty"`
	Path         string                      `json:"path,omitempty"`
	Key          string                      `json:"key,omitempty"`
	Message      string                      `json:"message"`
	Remediation  string                      `json:"remediation,omitempty"`
	Line         int                         `json:"line,omitempty"`
	Column       int                         `json:"column,omitempty"`
	ValidOptions []string                    `json:"valid_options,omitempty"`
}

func (d unifiedConfigDiagnostic) blocker() bool {
	return d.Kind != unifiedConfigDiagnosticLoaded
}

type unifiedConfigError struct {
	Diagnostics []unifiedConfigDiagnostic
}

func (e unifiedConfigError) Error() string {
	if len(e.Diagnostics) == 0 {
		return ""
	}
	lines := []string{"swarm.yaml config blockers:"}
	for _, d := range e.Diagnostics {
		if !d.blocker() {
			continue
		}
		line := strings.TrimSpace(d.Message)
		if d.Remediation != "" {
			line += "; remediation: " + d.Remediation
		}
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

func loadUnifiedConfig(opts unifiedConfigLoadOptions) (unifiedConfigLoadResult, error) {
	result, err := loadUnifiedConfigAllowDiagnostics(opts)
	if err != nil {
		return result, err
	}
	if blockers := unifiedConfigBlockers(result.Diagnostics); len(blockers) > 0 {
		return result, unifiedConfigError{Diagnostics: blockers}
	}
	return result, nil
}

func loadUnifiedConfigAllowDiagnostics(opts unifiedConfigLoadOptions) (unifiedConfigLoadResult, error) {
	RepoRoot, err := requireInvocationRootPath(opts.RepoRoot)
	if err != nil {
		return unifiedConfigLoadResult{}, err
	}
	layers, diagnostics := discoverUnifiedConfigLayers(RepoRoot, opts.ExplicitPath)
	merged, keyOrigins, layerDiagnostics := composeUnifiedConfigLayers(layers, RepoRoot)
	diagnostics = append(diagnostics, layerDiagnostics...)
	if err := validateSwarmEnvSources(swarmEnvGuardContext{RepoRoot: RepoRoot, RuntimeConfigPath: opts.ExplicitPath, DelegatedSources: unifiedConfigDelegation(&merged)}); err != nil {
		diagnostics = append(diagnostics, unifiedConfigDiagnostic{Kind: unifiedConfigDiagnosticValidationFailed, Message: err.Error()})
	}
	if unsupported := executableAdjacentRuntimeConfigDiagnostic(); unsupported != nil {
		diagnostics = append(diagnostics, *unsupported)
	}
	if unsupported := userGlobalUnsupportedConfigDiagnostic(); unsupported != nil {
		diagnostics = append(diagnostics, *unsupported)
	}
	cfg, err := defaultRuntimeConfig()
	if err != nil {
		return unifiedConfigLoadResult{}, err
	}
	if len(merged.Content) > 0 {
		if err := merged.Decode(cfg); err != nil {
			diagnostics = append(diagnostics, unifiedConfigDiagnostic{
				Kind:        unifiedConfigDiagnosticParseFailed,
				Message:     fmt.Sprintf("decode swarm.yaml config: %v", err),
				Remediation: "fix value types in swarm.yaml",
			})
		}
	}
	backendOverride := strings.TrimSpace(opts.BackendOverride)
	if len(unifiedConfigBlockers(diagnostics)) == 0 {
		if _, err := llmselection.ResolvePersistedBackend(cfg.LLM.Backend); err != nil {
			diagnostics = append(diagnostics, unifiedConfigDiagnostic{
				Kind:        unifiedConfigDiagnosticValidationFailed,
				Message:     err.Error(),
				Remediation: "set llm.backend to a supported backend profile before applying command-line overrides",
			})
		}
	}
	if backendOverride != "" {
		cfg.LLM.Backend = backendOverride
	}
	if len(unifiedConfigBlockers(diagnostics)) == 0 {
		if err := cfg.Validate(); err != nil {
			diagnostics = append(diagnostics, unifiedConfigDiagnostic{
				Kind:        unifiedConfigDiagnosticValidationFailed,
				Message:     err.Error(),
				Remediation: "fix the referenced swarm.yaml key or command flag",
			})
		}
	}
	cli, err := decodeUnifiedCLIConfig(&merged)
	if err != nil {
		diagnostics = append(diagnostics, unifiedConfigDiagnostic{
			Kind:        unifiedConfigDiagnosticParseFailed,
			Message:     fmt.Sprintf("decode swarm.yaml CLI config: %v", err),
			Remediation: "fix connection, serve, or paths value types in swarm.yaml",
		})
	}
	source, path := unifiedConfigPrimarySource(layers)
	result := unifiedConfigLoadResult{
		Config:      cfg,
		CLI:         cli,
		Source:      source,
		Path:        path,
		Layers:      layers,
		KeyOrigins:  keyOrigins,
		Diagnostics: diagnostics,
	}
	if blockers := unifiedConfigBlockers(diagnostics); len(blockers) > 0 {
		return result, unifiedConfigError{Diagnostics: blockers}
	}
	return result, nil
}

func composeUnifiedConfigLayers(layers []unifiedConfigLayer, RepoRoot string) (yaml.Node, map[string]unifiedConfigKeyOrigin, []unifiedConfigDiagnostic) {
	keyOrigins := map[string]unifiedConfigKeyOrigin{}
	var diagnostics []unifiedConfigDiagnostic
	merged := yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	for _, layer := range layers {
		raw, err := os.ReadFile(layer.Path)
		if err != nil {
			diagnostics = append(diagnostics, unifiedConfigDiagnostic{
				Kind:        unifiedConfigDiagnosticReadFailed,
				Layer:       layer.Name,
				Path:        layer.Path,
				Message:     fmt.Sprintf("read swarm.yaml config %s: %v", layer.Path, err),
				Remediation: unifiedConfigReadRemediation(layer),
			})
			continue
		}
		var doc yaml.Node
		if err := yaml.Unmarshal(raw, &doc); err != nil {
			diagnostics = append(diagnostics, unifiedConfigDiagnostic{
				Kind:        unifiedConfigDiagnosticParseFailed,
				Layer:       layer.Name,
				Path:        layer.Path,
				Message:     fmt.Sprintf("parse swarm.yaml config %s: %v", layer.Path, err),
				Remediation: "fix YAML syntax in this config file",
			})
			continue
		}
		root := yamlDocumentRoot(&doc)
		if root == nil || root.Kind == 0 {
			diagnostics = append(diagnostics, unifiedConfigDiagnostic{Kind: unifiedConfigDiagnosticLoaded, Layer: layer.Name, Path: layer.Path, Message: "loaded empty swarm.yaml config"})
			continue
		}
		if root.Kind != yaml.MappingNode {
			diagnostics = append(diagnostics, unifiedConfigDiagnostic{
				Kind:        unifiedConfigDiagnosticParseFailed,
				Layer:       layer.Name,
				Path:        layer.Path,
				Message:     fmt.Sprintf("swarm.yaml config %s must be a YAML mapping", layer.Path),
				Remediation: "use sectioned swarm.yaml keys such as runtime, workspace, connection, serve, paths, llm, store, or database",
			})
			continue
		}
		layerDiagnostics := validateUnifiedConfigNode(root, layer, RepoRoot)
		diagnostics = append(diagnostics, layerDiagnostics...)
		if len(unifiedConfigBlockers(layerDiagnostics)) != 0 {
			continue
		}
		expanded, err := expandUnifiedConfigValue(yamlsource.ValueFromNode(root))
		if err != nil {
			diagnostics = append(diagnostics, unifiedConfigDiagnostic{Kind: unifiedConfigDiagnosticParseFailed, Layer: layer.Name, Path: layer.Path, Message: err.Error()})
			continue
		}
		mergeUnifiedConfigLayer(&merged, expanded, nil, layer, keyOrigins)
		diagnostics = append(diagnostics, unifiedConfigDiagnostic{Kind: unifiedConfigDiagnosticLoaded, Layer: layer.Name, Path: layer.Path, Message: "loaded swarm.yaml config"})
	}
	return merged, keyOrigins, diagnostics
}

func recordUnifiedConfigKeyOrigins(node *yaml.Node, prefix []string, layer unifiedConfigLayer, origins map[string]unifiedConfigKeyOrigin) {
	if node == nil || node.Kind != yaml.MappingNode || origins == nil {
		return
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" {
			continue
		}
		pathParts := append(append([]string{}, prefix...), key)
		value := node.Content[i+1]
		if value.Kind == yaml.MappingNode {
			recordUnifiedConfigKeyOrigins(value, pathParts, layer, origins)
			continue
		}
		origins[strings.Join(pathParts, ".")] = unifiedConfigKeyOrigin{Layer: layer.Name, Path: layer.Path}
	}
}

func unifiedConfigBlockers(diagnostics []unifiedConfigDiagnostic) []unifiedConfigDiagnostic {
	blockers := make([]unifiedConfigDiagnostic, 0, len(diagnostics))
	for _, d := range diagnostics {
		if d.blocker() {
			blockers = append(blockers, d)
		}
	}
	return blockers
}

func discoverUnifiedConfigLayers(RepoRoot, explicitPath string) ([]unifiedConfigLayer, []unifiedConfigDiagnostic) {
	layers := []unifiedConfigLayer{}
	diagnostics := []unifiedConfigDiagnostic{}
	userPath := userGlobalUnifiedConfigPath()
	if fileExists(userPath) {
		layers = append(layers, unifiedConfigLayer{Name: unifiedLayerUserGlobal, Path: userPath})
	}
	projectPath := filepath.Join(RepoRoot, "swarm.yaml")
	if fileExists(projectPath) {
		layers = append(layers, unifiedConfigLayer{Name: unifiedLayerProject, Path: projectPath})
	}
	localPath := filepath.Join(RepoRoot, ".swarm", "swarm.yaml")
	if fileExists(localPath) {
		layers = append(layers, unifiedConfigLayer{Name: unifiedLayerLocalOperator, Path: localPath})
	}
	if path := strings.TrimSpace(explicitPath); path != "" {
		explicit := ResolvePath(RepoRoot, path)
		layers = removeUnifiedConfigLayerPath(layers, explicit)
		layers = append(layers, unifiedConfigLayer{Name: unifiedLayerExplicit, Path: explicit, Explicit: true})
	} else if raw, ok := os.LookupEnv("SWARM_CONFIG"); ok && strings.TrimSpace(raw) != "" {
		explicit := ResolvePath(RepoRoot, raw)
		layers = removeUnifiedConfigLayerPath(layers, explicit)
		layers = append(layers, unifiedConfigLayer{Name: unifiedLayerExplicit, Path: explicit, Explicit: true})
	}
	if len(layers) == 0 {
		diagnostics = append(diagnostics, unifiedConfigDiagnostic{Kind: unifiedConfigDiagnosticLoaded, Message: "no swarm.yaml config files loaded; using built-in defaults"})
	}
	return layers, diagnostics
}

func removeUnifiedConfigLayerPath(layers []unifiedConfigLayer, path string) []unifiedConfigLayer {
	canonical := canonicalConfigPath(path)
	out := layers[:0]
	for _, layer := range layers {
		if canonicalConfigPath(layer.Path) == canonical {
			continue
		}
		out = append(out, layer)
	}
	return out
}

func canonicalConfigPath(path string) string {
	if strings.TrimSpace(path) == "" {
		return ""
	}
	return filepath.Clean(path)
}

func userGlobalUnifiedConfigPath() string {
	dir, err := os.UserConfigDir()
	if err != nil {
		return ""
	}
	return filepath.Join(dir, "swarm", "swarm.yaml")
}

func userGlobalUnsupportedConfigDiagnostic() *unifiedConfigDiagnostic {
	dir, err := os.UserConfigDir()
	if err != nil {
		return nil
	}
	path := filepath.Join(dir, "swarm", "config.yaml")
	if !fileExists(path) {
		return nil
	}
	return &unifiedConfigDiagnostic{
		Kind:        unifiedConfigDiagnosticUnsupportedSource,
		Layer:       unifiedLayerUserGlobal,
		Path:        path,
		Message:     fmt.Sprintf("config source %s is not a supported configuration source", path),
		Remediation: "Only declared swarm.yaml layers and explicit config sources are admitted.",
	}
}

func executableAdjacentRuntimeConfigDiagnostic() *unifiedConfigDiagnostic {
	path, ok, err := executableAdjacentRuntimeConfigPath()
	if err != nil {
		return &unifiedConfigDiagnostic{
			Kind:        unifiedConfigDiagnosticUnsupportedSource,
			Path:        "",
			Message:     err.Error(),
			Remediation: "Only declared swarm.yaml layers and explicit config sources are admitted.",
		}
	}
	if !ok {
		return nil
	}
	return &unifiedConfigDiagnostic{
		Kind:        unifiedConfigDiagnosticUnsupportedSource,
		Path:        path,
		Message:     fmt.Sprintf("executable-adjacent runtime config %s is not an admitted config source", path),
		Remediation: "Only declared swarm.yaml layers and explicit config sources are admitted.",
	}
}

func fileExists(path string) bool {
	if strings.TrimSpace(path) == "" {
		return false
	}
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

func unifiedConfigPrimarySource(layers []unifiedConfigLayer) (string, string) {
	if len(layers) == 0 {
		return "built-in default", ""
	}
	layer := layers[len(layers)-1]
	return string(layer.Name), layer.Path
}

func unifiedConfigReadRemediation(layer unifiedConfigLayer) string {
	if layer.Explicit {
		return "fix --config or SWARM_CONFIG to point at a readable swarm.yaml file"
	}
	return "fix or remove this discovered swarm.yaml file"
}

func yamlDocumentRoot(doc *yaml.Node) *yaml.Node {
	if doc == nil {
		return nil
	}
	if doc.Kind == yaml.DocumentNode && len(doc.Content) > 0 {
		return doc.Content[0]
	}
	return doc
}

// Expand each admitted layer before precedence, origins and env delegation consume it.
func expandUnifiedConfigValue(value yamlsource.Value) (*yaml.Node, error) {
	var node yaml.Node
	if err := value.Project(&node); err != nil {
		return nil, err
	}
	node.Anchor, node.Alias = "", nil
	switch node.Kind {
	case yaml.MappingNode:
		fields, err := value.Mapping()
		if err != nil {
			return nil, err
		}
		node.Content = nil
		for _, field := range fields {
			child, err := expandUnifiedConfigValue(field.Value)
			if err != nil {
				return nil, err
			}
			key := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: field.Name, Line: field.KeyLocation.Line, Column: field.KeyLocation.Column}
			node.Content = append(node.Content, key, child)
		}
	case yaml.SequenceNode:
		items, err := value.Sequence()
		if err != nil {
			return nil, err
		}
		node.Content = nil
		for _, item := range items {
			child, err := expandUnifiedConfigValue(item)
			if err != nil {
				return nil, err
			}
			node.Content = append(node.Content, child)
		}
	}
	return &node, nil
}

func mergeUnifiedConfigLayer(dst, src *yaml.Node, prefix []string, layer unifiedConfigLayer, origins map[string]unifiedConfigKeyOrigin) {
	if dst == nil || src == nil || src.Kind != yaml.MappingNode {
		return
	}
	if dst.Kind == 0 {
		dst.Kind = yaml.MappingNode
	}
	for i := 0; i+1 < len(src.Content); i += 2 {
		key, value := src.Content[i], src.Content[i+1]
		pathParts := append(append([]string{}, prefix...), key.Value)
		path := strings.Join(pathParts, ".")
		if value.Kind == yaml.MappingNode {
			if existing := yamlMappingValue(dst, key.Value); existing != nil && existing.Kind == yaml.MappingNode {
				mergeUnifiedConfigLayer(existing, value, pathParts, layer, origins)
				continue
			}
		}
		for oldPath := range origins {
			if oldPath == path || strings.HasPrefix(oldPath, path+".") {
				delete(origins, oldPath)
			}
		}
		yamlSetMappingValue(dst, key, value)
		if value.Kind == yaml.MappingNode {
			recordUnifiedConfigKeyOrigins(value, pathParts, layer, origins)
		} else {
			origins[path] = unifiedConfigKeyOrigin{Layer: layer.Name, Path: layer.Path}
		}
	}
}

func yamlMappingValue(node *yaml.Node, key string) *yaml.Node {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		if node.Content[i].Value == key {
			return node.Content[i+1]
		}
	}
	return nil
}

func yamlSetMappingValue(node, key, value *yaml.Node) {
	for i := 0; i+1 < len(node.Content); i += 2 {
		if node.Content[i].Value == key.Value {
			node.Content[i] = key
			node.Content[i+1] = value
			return
		}
	}
	node.Content = append(node.Content, key, value)
}

type unifiedCLIYAML = config.CLISourceConfig

func decodeUnifiedCLIConfig(node *yaml.Node) (cliCommandConfig, error) {
	if node == nil || len(node.Content) == 0 {
		return cliCommandConfig{}, nil
	}
	var decoded unifiedCLIYAML
	if err := node.Decode(&decoded); err != nil {
		return cliCommandConfig{}, err
	}
	return cliCommandConfig{
		Connection: cliConnectionConfig{
			APIServer:    decoded.Connection.APIServer,
			APITokenFile: decoded.Connection.APITokenFile,
		},
		Serve: cliServeConfig{
			APIListenAddr: decoded.Serve.APIListenAddr,
			MCPListenAddr: decoded.Serve.MCPListenAddr,
			APITokenFile:  decoded.Serve.APITokenFile,
		},
		Paths: cliPathsConfig{
			SwarmDir:         decoded.Paths.SwarmDir,
			SwarmDirSet:      yamlPathExists(node, "paths", "swarm_dir"),
			PlatformSpecPath: decoded.Paths.PlatformSpecPath,
		},
	}, nil
}

func yamlPathExists(node *yaml.Node, path ...string) bool {
	cur := node
	for _, key := range path {
		cur = yamlMappingValue(cur, key)
		if cur == nil {
			return false
		}
	}
	return true
}

func validateUnifiedConfigNode(root *yaml.Node, layer unifiedConfigLayer, RepoRoot string) []unifiedConfigDiagnostic {
	var diagnostics []unifiedConfigDiagnostic
	value := yamlsource.ValueFromNode(root)
	if err := value.ValidateExpansion(); err != nil {
		return []unifiedConfigDiagnostic{{Kind: unifiedConfigDiagnosticParseFailed, Layer: layer.Name, Path: layer.Path, Message: err.Error()}}
	}
	if err := value.ValidateUniqueMappings(); err != nil {
		return []unifiedConfigDiagnostic{{Kind: unifiedConfigDiagnosticUnknownKey, Layer: layer.Name, Path: layer.Path, Message: err.Error()}}
	}
	walkUnifiedMapping(value, nil, layer, RepoRoot, &diagnostics)
	return diagnostics
}

func walkUnifiedMapping(value yamlsource.Value, prefix []string, layer unifiedConfigLayer, RepoRoot string, diagnostics *[]unifiedConfigDiagnostic) {
	entries, err := value.Mapping()
	if err != nil {
		*diagnostics = append(*diagnostics, unifiedConfigDiagnostic{Kind: unifiedConfigDiagnosticParseFailed, Layer: layer.Name, Path: layer.Path, Message: err.Error()})
		return
	}
	for _, entry := range entries {
		pathParts := append(append([]string{}, prefix...), entry.Name)
		path := strings.Join(pathParts, ".")
		scalar, _ := entry.Value.Scalar() // Containers have no scalar text.
		rule, ok := unifiedConfigRule(pathParts)
		if !ok {
			*diagnostics = append(*diagnostics, unknownUnifiedConfigDiagnostic(path, layer, entry.IntroductionLocation()))
			continue
		}
		if path == "llm.backend" && llmselection.NormalizeBackendID(scalar.Value) == llmselection.BackendMock {
			*diagnostics = append(*diagnostics, unifiedConfigDiagnostic{
				Kind: unifiedConfigDiagnosticValidationFailed, Layer: layer.Name, Path: layer.Path, Key: path,
				Message: fmt.Sprintf("config key %q in %s: backend mock is unsupported as a public selector", path, layer.Path), Remediation: "use swarm test to execute authored doubles",
			})
			continue
		}
		if rule.Split != "" {
			*diagnostics = append(*diagnostics, unifiedConfigDiagnostic{
				Kind:        unifiedConfigDiagnosticSplitUnsupported,
				Layer:       layer.Name,
				Path:        layer.Path,
				Key:         path,
				Message:     fmt.Sprintf("config key %q is recognized but not yet supported", path),
				Remediation: rule.Split,
			})
			continue
		}
		if rule.InlineSecret && strings.TrimSpace(scalar.Value) != "" {
			*diagnostics = append(*diagnostics, unifiedConfigDiagnostic{
				Kind:        unifiedConfigDiagnosticTrustRejected,
				Layer:       layer.Name,
				Path:        layer.Path,
				Key:         path,
				Message:     fmt.Sprintf("config key %q stores unsupported plaintext secret material", path),
				Remediation: "declare a file, secret key, or explicit env delegation field instead",
			})
		}
		if remediation := trustViolationRemediation(path, rule, scalar.Value, layer); remediation != "" {
			*diagnostics = append(*diagnostics, unifiedConfigDiagnostic{
				Kind:        unifiedConfigDiagnosticTrustRejected,
				Layer:       layer.Name,
				Path:        layer.Path,
				Key:         path,
				Message:     fmt.Sprintf("config key %q is not allowed in %s", path, layer.Name),
				Remediation: remediation,
			})
			continue
		}
		if rule.ProjectContainedPath && layer.Name == unifiedLayerProject {
			*diagnostics = append(*diagnostics, validateProjectContainedConfigPath(path, entry.Value, layer, RepoRoot)...)
		}
		if rule.Container && (entry.Value.Presence() == yamlsource.PresenceMapping || entry.Value.Presence() == yamlsource.PresenceEmptyMapping) {
			walkUnifiedMapping(entry.Value, pathParts, layer, RepoRoot, diagnostics)
		}
	}
}

type unifiedConfigKeyRule = config.SourceKeyRule

func unifiedConfigRule(pathParts []string) (unifiedConfigKeyRule, bool) {
	path := strings.Join(pathParts, ".")
	rules := config.SourceKeyRules()
	if rule, ok := rules[path]; ok {
		return rule, true
	}
	if len(pathParts) > 2 && pathParts[0] == "llm" && pathParts[1] == "models" {
		return unifiedConfigKeyRule{}, true
	}
	if rule, ok := unifiedConfigProviderLimitRule(pathParts); ok {
		return rule, true
	}
	if rule, ok := unifiedConfigChannelRule(pathParts); ok {
		return rule, true
	}
	if path == "sharding" || strings.HasPrefix(path, "sharding.") {
		return rules["sharding"], true
	}
	return unifiedConfigKeyRule{}, false
}

func unifiedConfigChannelRule(pathParts []string) (unifiedConfigKeyRule, bool) {
	if len(pathParts) < 3 || pathParts[0] != "channels" || pathParts[1] != "bindings" {
		return unifiedConfigKeyRule{}, false
	}
	elevatedSection := unifiedConfigKeyRule{Container: true, Elevated: true}
	switch len(pathParts) {
	case 3:
		return elevatedSection, true
	case 4:
		switch pathParts[3] {
		case "pack", "destination":
			return unifiedConfigKeyRule{Elevated: true}, true
		}
	}
	return unifiedConfigKeyRule{}, false
}

func unifiedConfigProviderLimitRule(pathParts []string) (unifiedConfigKeyRule, bool) {
	if len(pathParts) < 3 || pathParts[0] != "llm" || pathParts[1] != "provider_limits" {
		return unifiedConfigKeyRule{}, false
	}
	section := unifiedConfigKeyRule{Container: true}
	switch len(pathParts) {
	case 3:
		// Provider profile ids are dynamic, but their policy leaves are finite.
		return section, true
	case 4:
		if pathParts[3] == "models" {
			return section, true
		}
		_, ok := unifiedConfigProviderLimitPolicyLeaves()[pathParts[3]]
		return unifiedConfigKeyRule{}, ok
	case 5:
		if pathParts[3] == "models" {
			// Model keys under a provider profile are dynamic.
			return section, true
		}
	case 6:
		if pathParts[3] == "models" {
			_, ok := unifiedConfigProviderLimitPolicyLeaves()[pathParts[5]]
			return unifiedConfigKeyRule{}, ok
		}
	}
	return unifiedConfigKeyRule{}, false
}

func unifiedConfigProviderLimitPolicyLeaves() map[string]struct{} {
	return map[string]struct{}{
		"rate_limit":               {},
		"rate_limit_max_wait":      {},
		"max_concurrency":          {},
		"max_concurrency_max_wait": {},
	}
}

func trustViolationRemediation(path string, rule unifiedConfigKeyRule, value string, layer unifiedConfigLayer) string {
	switch layer.Name {
	case unifiedLayerProject:
		if rule.Elevated || rule.SecretReference {
			return "move this key to .swarm/swarm.yaml, user-global swarm.yaml, explicit --config, or a flag"
		}
		if (path == "serve.api_listen_addr" || path == "serve.mcp_listen_addr") && !listenAddrIsProjectSafe(value) {
			return "project config may only set loopback listener addresses; move public/wildcard binds to local-operator, user-global, explicit --config, or flags"
		}
	case unifiedLayerLocalOperator:
		if path == "connection" || strings.HasPrefix(path, "connection.") || path == "serve.api_token_file" {
			return "connection/auth keys require user-global config, explicit --config, or flags"
		}
	}
	return ""
}

func listenAddrIsProjectSafe(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return true
	}
	host, _, err := net.SplitHostPort(raw)
	if err != nil {
		return false
	}
	host = strings.Trim(host, "[]")
	return host == "127.0.0.1" || host == "::1" || strings.HasPrefix(host, "127.")
}

func validateProjectContainedConfigPath(path string, value yamlsource.Value, layer unifiedConfigLayer, RepoRoot string) []unifiedConfigDiagnostic {
	if value.Presence() == yamlsource.PresenceSequence || value.Presence() == yamlsource.PresenceEmptySequence {
		var diagnostics []unifiedConfigDiagnostic
		items, err := value.Sequence()
		if err != nil {
			return []unifiedConfigDiagnostic{{Kind: unifiedConfigDiagnosticParseFailed, Layer: layer.Name, Path: layer.Path, Key: path, Message: err.Error()}}
		}
		for i, item := range items {
			scalar, _ := item.Scalar()
			diagnostics = append(diagnostics, validateProjectContainedPathValue(fmt.Sprintf("%s[%d]", path, i), scalar.Value, layer, RepoRoot)...)
		}
		return diagnostics
	}
	scalar, _ := value.Scalar()
	return validateProjectContainedPathValue(path, scalar.Value, layer, RepoRoot)
}

func validateProjectContainedPathValue(path, raw string, layer unifiedConfigLayer, RepoRoot string) []unifiedConfigDiagnostic {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	if filepath.IsAbs(raw) {
		return []unifiedConfigDiagnostic{projectPathDiagnostic(path, raw, layer, "absolute paths are not allowed in project config")}
	}
	clean := filepath.Clean(raw)
	if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return []unifiedConfigDiagnostic{projectPathDiagnostic(path, raw, layer, "parent-directory escapes are not allowed in project config")}
	}
	root := strings.TrimSpace(RepoRoot)
	if root == "" {
		return nil
	}
	candidate := filepath.Join(root, clean)
	canonicalRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		canonicalRoot = filepath.Clean(root)
	}
	checkPath := nearestExistingPath(candidate)
	canonicalCandidate, err := filepath.EvalSymlinks(checkPath)
	if err != nil {
		canonicalCandidate = filepath.Clean(checkPath)
	}
	if !unifiedConfigPathWithin(canonicalCandidate, canonicalRoot) {
		return []unifiedConfigDiagnostic{projectPathDiagnostic(path, raw, layer, fmt.Sprintf("path escapes project root %s", canonicalRoot))}
	}
	return nil
}

func projectPathDiagnostic(path, raw string, layer unifiedConfigLayer, message string) unifiedConfigDiagnostic {
	return unifiedConfigDiagnostic{
		Kind:        unifiedConfigDiagnosticPathViolation,
		Layer:       layer.Name,
		Path:        layer.Path,
		Key:         path,
		Message:     fmt.Sprintf("config key %q value %q violates project-contained path rule: %s", path, raw, message),
		Remediation: "use a relative path under the project or move this setting to .swarm/swarm.yaml, user-global config, explicit --config, or a flag",
	}
}

func nearestExistingPath(path string) string {
	path = filepath.Clean(path)
	for {
		if _, err := os.Stat(path); err == nil {
			return path
		}
		parent := filepath.Dir(path)
		if parent == path {
			return path
		}
		path = parent
	}
}

func unifiedConfigPathWithin(path, root string) bool {
	path = filepath.Clean(path)
	root = filepath.Clean(root)
	if path == root {
		return true
	}
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	return rel != "." && !strings.HasPrefix(rel, "..") && !filepath.IsAbs(rel)
}

func unknownUnifiedConfigDiagnostic(path string, layer unifiedConfigLayer, location yamlsource.Location) unifiedConfigDiagnostic {
	parts := strings.Split(path, ".")
	parent, key := parts[:len(parts)-1], parts[len(parts)-1]
	allowed := map[string]struct{}{}
	for candidate, rule := range config.SourceKeyRules() {
		if !rule.Supported() {
			continue
		}
		fields := strings.Split(candidate, ".")
		if len(fields) == len(parts) && strings.Join(fields[:len(fields)-1], ".") == strings.Join(parent, ".") {
			allowed[fields[len(fields)-1]] = struct{}{}
		}
	}
	if len(parent) >= 3 && parent[0] == "llm" && parent[1] == "provider_limits" {
		for field := range unifiedConfigProviderLimitPolicyLeaves() {
			allowed[field] = struct{}{}
		}
		if len(parent) == 3 {
			allowed["models"] = struct{}{}
		}
	}
	if len(parent) == 3 && parent[0] == "channels" && parent[1] == "bindings" {
		allowed["pack"], allowed["destination"] = struct{}{}, struct{}{}
	}
	diagnostic := runtimecontracts.NewUndefinedFieldDiagnostic("config", key, allowed)
	diagnostic.Problem = fmt.Sprintf("unknown config key %q", path)
	diagnostic.Location = runtimecontracts.LoaderDiagnosticLocation{File: layer.Path, YAMLPath: path, Line: location.Line, Column: location.Column}
	return unifiedConfigDiagnostic{
		Kind: unifiedConfigDiagnosticUnknownKey, Layer: layer.Name, Path: layer.Path, Key: path,
		Message: diagnostic.Error(), Remediation: diagnostic.Remediation,
		Line: location.Line, Column: location.Column, ValidOptions: diagnostic.ValidOptions,
	}
}

func addUnifiedConfigDiagnosticsToReport(report *LocalPreflightReport, diagnostics []unifiedConfigDiagnostic) {
	for _, d := range diagnostics {
		if d.Kind == unifiedConfigDiagnosticLoaded {
			report.addWithOwner(localPreflightConfigPrerequisite, "config_loaded", LocalPreflightSeverityInfo, LocalPreflightStatusOK, d.Message, "", unifiedConfigOwner)
			continue
		}
		report.addWithOwner(localPreflightConfigPrerequisite, "config_"+string(d.Kind), LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, d.Message, d.Remediation, unifiedConfigOwner)
	}
}

func unifiedConfigDiagnosticsFromError(err error) []unifiedConfigDiagnostic {
	var configErr unifiedConfigError
	if errors.As(err, &configErr) {
		return configErr.Diagnostics
	}
	return nil
}

func unifiedConfigDelegatedSwarmEnvSources(RepoRoot, explicitPath string) map[string]string {
	RepoRoot, err := requireInvocationRootPath(RepoRoot)
	if err != nil {
		return map[string]string{}
	}
	layers, _ := discoverUnifiedConfigLayers(RepoRoot, explicitPath)
	merged, _, _ := composeUnifiedConfigLayers(layers, RepoRoot)
	return unifiedConfigDelegation(&merged)
}

func unifiedConfigDelegation(merged *yaml.Node) map[string]string {
	out := map[string]string{}
	name := strings.TrimSpace(yamlScalarPath(merged, "database", "password_env"))
	if strings.HasPrefix(name, "SWARM_") {
		out[name] = "database.password_env"
	}
	return out
}

func yamlScalarPath(node *yaml.Node, path ...string) string {
	cur := node
	for _, key := range path {
		cur = yamlMappingValue(cur, key)
		if cur == nil {
			return ""
		}
	}
	if cur.Kind != yaml.ScalarNode {
		return ""
	}
	return cur.Value
}
