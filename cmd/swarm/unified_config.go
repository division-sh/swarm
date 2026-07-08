package main

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/config"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
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
	CLI         cliAPIConfigFile
	Source      string
	Path        string
	Layers      []unifiedConfigLayer
	Diagnostics []unifiedConfigDiagnostic
}

type unifiedConfigLayer struct {
	Name     unifiedConfigLayerName `json:"name"`
	Path     string                 `json:"path"`
	Explicit bool                   `json:"explicit"`
}

type unifiedConfigDiagnosticKind string

const (
	unifiedConfigDiagnosticLoaded           unifiedConfigDiagnosticKind = "loaded"
	unifiedConfigDiagnosticUnknownKey       unifiedConfigDiagnosticKind = "unknown_key"
	unifiedConfigDiagnosticOldShape         unifiedConfigDiagnosticKind = "old_shape"
	unifiedConfigDiagnosticTrustRejected    unifiedConfigDiagnosticKind = "trust_rejected"
	unifiedConfigDiagnosticSplitUnsupported unifiedConfigDiagnosticKind = "split_unsupported"
	unifiedConfigDiagnosticPathViolation    unifiedConfigDiagnosticKind = "path_violation"
	unifiedConfigDiagnosticReadFailed       unifiedConfigDiagnosticKind = "read_failed"
	unifiedConfigDiagnosticParseFailed      unifiedConfigDiagnosticKind = "parse_failed"
	unifiedConfigDiagnosticLegacyDiscovery  unifiedConfigDiagnosticKind = "legacy_discovery"
	unifiedConfigDiagnosticValidationFailed unifiedConfigDiagnosticKind = "validation_failed"
)

type unifiedConfigDiagnostic struct {
	Kind        unifiedConfigDiagnosticKind `json:"kind"`
	Layer       unifiedConfigLayerName      `json:"layer,omitempty"`
	Path        string                      `json:"path,omitempty"`
	Key         string                      `json:"key,omitempty"`
	Message     string                      `json:"message"`
	Remediation string                      `json:"remediation,omitempty"`
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
	lines := []string{"unified config source authority blockers:"}
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
	if err := rejectUnsupportedRuntimeControlEnv(); err != nil {
		return unifiedConfigLoadResult{}, err
	}
	if err := rejectRetiredLLMEnvSelectors(); err != nil {
		return unifiedConfigLoadResult{}, err
	}
	repoRoot := unifiedConfigRepoRoot(opts.RepoRoot)
	layers, diagnostics := discoverUnifiedConfigLayers(repoRoot, opts.ExplicitPath)
	var merged yaml.Node
	merged.Kind = yaml.MappingNode
	for _, layer := range layers {
		raw, err := os.ReadFile(layer.Path)
		if err != nil {
			diagnostics = append(diagnostics, unifiedConfigDiagnostic{
				Kind:        unifiedConfigDiagnosticReadFailed,
				Layer:       layer.Name,
				Path:        layer.Path,
				Message:     fmt.Sprintf("read unified config %s: %v", layer.Path, err),
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
				Message:     fmt.Sprintf("parse unified config %s: %v", layer.Path, err),
				Remediation: "fix YAML syntax in this config file",
			})
			continue
		}
		root := yamlDocumentRoot(&doc)
		if root == nil || root.Kind == 0 {
			diagnostics = append(diagnostics, unifiedConfigDiagnostic{Kind: unifiedConfigDiagnosticLoaded, Layer: layer.Name, Path: layer.Path, Message: "loaded empty unified config"})
			continue
		}
		if root.Kind != yaml.MappingNode {
			diagnostics = append(diagnostics, unifiedConfigDiagnostic{
				Kind:        unifiedConfigDiagnosticParseFailed,
				Layer:       layer.Name,
				Path:        layer.Path,
				Message:     fmt.Sprintf("unified config %s must be a YAML mapping", layer.Path),
				Remediation: "use sectioned swarm.yaml keys such as runtime, workspace, connection, serve, paths, llm, store, or database",
			})
			continue
		}
		diagnostics = append(diagnostics, validateUnifiedConfigNode(root, layer, repoRoot)...)
		if len(unifiedConfigBlockers(diagnostics)) == 0 {
			mergeYAMLMapping(&merged, root)
		} else {
			// Continue scanning later files so doctor can render a complete blocker set.
			mergeYAMLMapping(&merged, root)
		}
		diagnostics = append(diagnostics, unifiedConfigDiagnostic{Kind: unifiedConfigDiagnosticLoaded, Layer: layer.Name, Path: layer.Path, Message: "loaded unified config"})
	}
	if legacy := executableAdjacentRuntimeConfigDiagnostic(); legacy != nil {
		diagnostics = append(diagnostics, *legacy)
	}
	if legacy := userGlobalLegacyCLIConfigDiagnostic(); legacy != nil {
		diagnostics = append(diagnostics, *legacy)
	}
	cfg, err := defaultRuntimeConfig()
	if err != nil {
		return unifiedConfigLoadResult{}, err
	}
	if len(merged.Content) > 0 {
		if err := merged.Decode(cfg); err != nil {
			diagnostics = append(diagnostics, unifiedConfigDiagnostic{
				Kind:        unifiedConfigDiagnosticParseFailed,
				Message:     fmt.Sprintf("decode unified config: %v", err),
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
				Remediation: "fix the referenced unified swarm.yaml key or command flag",
			})
		}
	}
	cli, err := decodeUnifiedCLIConfig(&merged)
	if err != nil {
		diagnostics = append(diagnostics, unifiedConfigDiagnostic{
			Kind:        unifiedConfigDiagnosticParseFailed,
			Message:     fmt.Sprintf("decode unified CLI config: %v", err),
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
		Diagnostics: diagnostics,
	}
	if blockers := unifiedConfigBlockers(diagnostics); len(blockers) > 0 {
		return result, unifiedConfigError{Diagnostics: blockers}
	}
	return result, nil
}

func rejectRetiredLLMEnvSelectors() error {
	if err := llmselection.RejectRetiredEnvBackend(os.LookupEnv); err != nil {
		return err
	}
	if err := llmselection.RejectRetiredEnvRuntimeMode(os.LookupEnv); err != nil {
		return err
	}
	if err := llmselection.RejectRetiredOpenAICompatibleBaseURLEnv(os.LookupEnv); err != nil {
		return err
	}
	return llmselection.RejectRetiredModelEnv(os.LookupEnv)
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

func unifiedConfigRepoRoot(repoRoot string) string {
	repoRoot = strings.TrimSpace(repoRoot)
	if repoRoot == "" {
		repoRoot = discoverRepoRoot()
	}
	if repoRoot == "" {
		if cwd, err := os.Getwd(); err == nil {
			repoRoot = cwd
		}
	}
	return filepath.Clean(repoRoot)
}

func discoverUnifiedConfigLayers(repoRoot, explicitPath string) ([]unifiedConfigLayer, []unifiedConfigDiagnostic) {
	layers := []unifiedConfigLayer{}
	diagnostics := []unifiedConfigDiagnostic{}
	userPath := userGlobalUnifiedConfigPath()
	if fileExists(userPath) {
		layers = append(layers, unifiedConfigLayer{Name: unifiedLayerUserGlobal, Path: userPath})
	}
	if repoRoot != "" {
		projectPath := filepath.Join(repoRoot, "swarm.yaml")
		if fileExists(projectPath) {
			layers = append(layers, unifiedConfigLayer{Name: unifiedLayerProject, Path: projectPath})
		}
		localPath := filepath.Join(repoRoot, ".swarm", "swarm.yaml")
		if fileExists(localPath) {
			layers = append(layers, unifiedConfigLayer{Name: unifiedLayerLocalOperator, Path: localPath})
		}
	}
	if path := strings.TrimSpace(explicitPath); path != "" {
		explicit := resolvePath(repoRoot, path)
		layers = removeUnifiedConfigLayerPath(layers, explicit)
		layers = append(layers, unifiedConfigLayer{Name: unifiedLayerExplicit, Path: explicit, Explicit: true})
	} else if raw, ok := os.LookupEnv("SWARM_CONFIG"); ok && strings.TrimSpace(raw) != "" {
		explicit := resolvePath(repoRoot, raw)
		layers = removeUnifiedConfigLayerPath(layers, explicit)
		layers = append(layers, unifiedConfigLayer{Name: unifiedLayerExplicit, Path: explicit, Explicit: true})
	}
	if len(layers) == 0 {
		diagnostics = append(diagnostics, unifiedConfigDiagnostic{Kind: unifiedConfigDiagnosticLoaded, Message: "no unified swarm.yaml config files loaded; using built-in defaults"})
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
	abs, err := filepath.Abs(path)
	if err != nil {
		return filepath.Clean(path)
	}
	return filepath.Clean(abs)
}

func userGlobalUnifiedConfigPath() string {
	dir, err := os.UserConfigDir()
	if err != nil {
		return ""
	}
	return filepath.Join(dir, "swarm", "swarm.yaml")
}

func userGlobalLegacyCLIConfigDiagnostic() *unifiedConfigDiagnostic {
	dir, err := os.UserConfigDir()
	if err != nil {
		return nil
	}
	path := filepath.Join(dir, "swarm", "config.yaml")
	if !fileExists(path) {
		return nil
	}
	return &unifiedConfigDiagnostic{
		Kind:        unifiedConfigDiagnosticLegacyDiscovery,
		Layer:       unifiedLayerUserGlobal,
		Path:        path,
		Message:     fmt.Sprintf("legacy flat CLI config %s is no longer a config source", path),
		Remediation: "move values to user-global swarm.yaml under connection, serve, or paths, then remove config.yaml",
	}
}

func executableAdjacentRuntimeConfigDiagnostic() *unifiedConfigDiagnostic {
	path, ok, err := executableAdjacentRuntimeConfigPath()
	if err != nil {
		return &unifiedConfigDiagnostic{
			Kind:        unifiedConfigDiagnosticLegacyDiscovery,
			Path:        "",
			Message:     err.Error(),
			Remediation: "remove executable-adjacent config.yaml; use --config, SWARM_CONFIG, .swarm/swarm.yaml, ./swarm.yaml, or user-global swarm.yaml",
		}
	}
	if !ok {
		return nil
	}
	return &unifiedConfigDiagnostic{
		Kind:        unifiedConfigDiagnosticLegacyDiscovery,
		Path:        path,
		Message:     fmt.Sprintf("executable-adjacent runtime config %s is no longer a config source", path),
		Remediation: "move this file to an explicit unified swarm.yaml source and remove executable-adjacent config.yaml",
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
		return "fix --config or SWARM_CONFIG to point at a readable unified swarm.yaml file"
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

func mergeYAMLMapping(dst, src *yaml.Node) {
	if dst == nil || src == nil || src.Kind != yaml.MappingNode {
		return
	}
	if dst.Kind == 0 {
		dst.Kind = yaml.MappingNode
	}
	for i := 0; i+1 < len(src.Content); i += 2 {
		key := cloneYAMLNode(src.Content[i])
		value := cloneYAMLNode(src.Content[i+1])
		if value.Kind == yaml.MappingNode {
			if existing := yamlMappingValue(dst, key.Value); existing != nil && existing.Kind == yaml.MappingNode {
				mergeYAMLMapping(existing, value)
				continue
			}
		}
		yamlSetMappingValue(dst, key, value)
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

func cloneYAMLNode(node *yaml.Node) *yaml.Node {
	if node == nil {
		return nil
	}
	out := *node
	out.Content = make([]*yaml.Node, len(node.Content))
	for i, child := range node.Content {
		out.Content[i] = cloneYAMLNode(child)
	}
	return &out
}

type unifiedCLIYAML struct {
	Connection struct {
		APIServer    string `yaml:"api_server"`
		APITokenFile string `yaml:"api_token_file"`
	} `yaml:"connection"`
	Serve struct {
		APIListenAddr string `yaml:"api_listen_addr"`
		MCPListenAddr string `yaml:"mcp_listen_addr"`
		APITokenFile  string `yaml:"api_token_file"`
	} `yaml:"serve"`
	Paths struct {
		SwarmDir         string `yaml:"swarm_dir"`
		ContractsPath    string `yaml:"contracts_path"`
		PlatformSpecPath string `yaml:"platform_spec_path"`
	} `yaml:"paths"`
}

func decodeUnifiedCLIConfig(node *yaml.Node) (cliAPIConfigFile, error) {
	if node == nil || len(node.Content) == 0 {
		return cliAPIConfigFile{}, nil
	}
	var decoded unifiedCLIYAML
	if err := node.Decode(&decoded); err != nil {
		return cliAPIConfigFile{}, err
	}
	return cliAPIConfigFile{
		APIServer:          decoded.Connection.APIServer,
		APITokenFile:       decoded.Connection.APITokenFile,
		SwarmDir:           decoded.Paths.SwarmDir,
		SwarmDirSet:        yamlPathExists(node, "paths", "swarm_dir"),
		ContractsPath:      decoded.Paths.ContractsPath,
		PlatformSpecPath:   decoded.Paths.PlatformSpecPath,
		ServeAPIListenAddr: decoded.Serve.APIListenAddr,
		ServeMCPListenAddr: decoded.Serve.MCPListenAddr,
		ServeAPITokenFile:  decoded.Serve.APITokenFile,
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

func validateUnifiedConfigNode(root *yaml.Node, layer unifiedConfigLayer, repoRoot string) []unifiedConfigDiagnostic {
	var diagnostics []unifiedConfigDiagnostic
	walkUnifiedMapping(root, nil, layer, repoRoot, &diagnostics)
	return diagnostics
}

func walkUnifiedMapping(node *yaml.Node, prefix []string, layer unifiedConfigLayer, repoRoot string, diagnostics *[]unifiedConfigDiagnostic) {
	if node == nil || node.Kind != yaml.MappingNode {
		return
	}
	seen := map[string]struct{}{}
	for i := 0; i+1 < len(node.Content); i += 2 {
		keyNode := node.Content[i]
		valueNode := node.Content[i+1]
		key := strings.TrimSpace(keyNode.Value)
		pathParts := append(append([]string{}, prefix...), key)
		path := strings.Join(pathParts, ".")
		if _, ok := seen[key]; ok {
			*diagnostics = append(*diagnostics, unifiedConfigDiagnostic{
				Kind:        unifiedConfigDiagnosticUnknownKey,
				Layer:       layer.Name,
				Path:        layer.Path,
				Key:         path,
				Message:     fmt.Sprintf("duplicate config key %q in %s", path, layer.Path),
				Remediation: "keep one value for this key",
			})
			continue
		}
		seen[key] = struct{}{}
		rule, ok := unifiedConfigRule(pathParts)
		if !ok {
			*diagnostics = append(*diagnostics, unknownUnifiedConfigDiagnostic(path, layer))
			continue
		}
		if rule.OldShape != "" {
			*diagnostics = append(*diagnostics, unifiedConfigDiagnostic{
				Kind:        unifiedConfigDiagnosticOldShape,
				Layer:       layer.Name,
				Path:        layer.Path,
				Key:         path,
				Message:     fmt.Sprintf("old flat config key %q in %s is no longer accepted", path, layer.Path),
				Remediation: rule.OldShape,
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
		if rule.InlineSecret && strings.TrimSpace(valueNode.Value) != "" {
			*diagnostics = append(*diagnostics, unifiedConfigDiagnostic{
				Kind:        unifiedConfigDiagnosticTrustRejected,
				Layer:       layer.Name,
				Path:        layer.Path,
				Key:         path,
				Message:     fmt.Sprintf("config key %q stores unsupported plaintext secret material", path),
				Remediation: "declare a file, secret key, or explicit env delegation field instead",
			})
		}
		if remediation := trustViolationRemediation(path, rule, valueNode, layer); remediation != "" {
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
			*diagnostics = append(*diagnostics, validateProjectContainedConfigPath(path, valueNode, layer, repoRoot)...)
		}
		if rule.Container && valueNode.Kind == yaml.MappingNode {
			walkUnifiedMapping(valueNode, pathParts, layer, repoRoot, diagnostics)
		}
	}
}

type unifiedConfigKeyRule struct {
	Container            bool
	Elevated             bool
	SecretReference      bool
	InlineSecret         bool
	ProjectContainedPath bool
	Split                string
	OldShape             string
}

func (r unifiedConfigKeyRule) supportedExampleLeaf() bool {
	return !r.Container && r.Split == "" && r.OldShape == "" && !r.InlineSecret
}

func unifiedConfigRule(pathParts []string) (unifiedConfigKeyRule, bool) {
	path := strings.Join(pathParts, ".")
	if remediation, ok := unifiedOldFlatKeyRemediation()[path]; ok {
		return unifiedConfigKeyRule{OldShape: remediation}, true
	}
	rules := unifiedConfigRules()
	if rule, ok := rules[path]; ok {
		return rule, true
	}
	if len(pathParts) > 2 && pathParts[0] == "llm" && pathParts[1] == "models" {
		return unifiedConfigKeyRule{}, true
	}
	if rule, ok := unifiedConfigProviderLimitRule(pathParts); ok {
		return rule, true
	}
	if path == "sharding" || strings.HasPrefix(path, "sharding.") {
		return unifiedConfigKeyRule{Split: "tracked split: runtime sharding has no supported production consumer; no supported replacement"}, true
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

func unifiedConfigRules() map[string]unifiedConfigKeyRule {
	section := unifiedConfigKeyRule{Container: true}
	elevatedSection := unifiedConfigKeyRule{Container: true, Elevated: true}
	pathSection := unifiedConfigKeyRule{Container: true, ProjectContainedPath: true}
	return map[string]unifiedConfigKeyRule{
		"connection":                            elevatedSection,
		"connection.api_server":                 {Elevated: true},
		"connection.api_token_file":             {Elevated: true, SecretReference: true},
		"serve":                                 section,
		"serve.api_listen_addr":                 {},
		"serve.mcp_listen_addr":                 {},
		"serve.api_token_file":                  {Elevated: true, SecretReference: true},
		"runtime":                               section,
		"runtime.recovery_on_startup":           {},
		"runtime.max_concurrent_agents":         {Split: "tracked split: runtime.max_concurrent_agents is not wired to runtime enforcement; no supported replacement"},
		"runtime.event_poll_interval":           {Split: "tracked split: runtime.event_poll_interval is not wired to runtime polling; no supported replacement"},
		"store":                                 elevatedSection,
		"store.backend":                         {Elevated: true},
		"store.sqlite":                          elevatedSection,
		"store.sqlite.path":                     {Elevated: true, ProjectContainedPath: true},
		"database":                              elevatedSection,
		"database.host":                         {Elevated: true},
		"database.port":                         {Elevated: true},
		"database.name":                         {Elevated: true},
		"database.user":                         {Elevated: true},
		"database.password":                     {Elevated: true, InlineSecret: true},
		"database.password_secret_key":          {Elevated: true, SecretReference: true},
		"database.password_file":                {Elevated: true, SecretReference: true},
		"database.password_env":                 {Elevated: true, SecretReference: true},
		"database.sslmode":                      {Elevated: true},
		"database.pool_size":                    {Elevated: true},
		"workspace":                             section,
		"workspace.data_source":                 {ProjectContainedPath: true},
		"workspace.backend":                     {},
		"workspace.allow_exec_on_host":          {Elevated: true},
		"workspace.image":                       {Elevated: true},
		"workspace.docker_bin":                  {Elevated: true},
		"workspace.host_root":                   {Elevated: true},
		"workspace.volumes_from":                {Elevated: true},
		"workspace.network":                     {Elevated: true},
		"llm":                                   section,
		"llm.backend":                           {},
		"llm.runtime_mode":                      {Split: "tracked split: llm.runtime_mode is retired; use llm.backend"},
		"llm.models":                            section,
		"llm.session":                           section,
		"llm.session.lock_ttl":                  {},
		"llm.session.rotate_after_turns":        {},
		"llm.session.rotate_on_parse_failures":  {},
		"llm.provider_limits":                   section,
		"llm.claude_api":                        section,
		"llm.claude_api.default_model":          {Split: "retired model-selection input; use llm.models"},
		"llm.claude_api.haiku_model":            {Split: "retired model-selection input; use llm.models"},
		"llm.claude_api.max_retries":            {},
		"llm.claude_api.retry_backoff":          {},
		"llm.claude_cli":                        section,
		"llm.claude_cli.command":                {Elevated: true},
		"llm.claude_cli.timeout":                {},
		"llm.claude_cli.output_format":          {},
		"llm.claude_cli.retries":                {Split: "tracked split: llm.claude_cli.retries remains unsupported/inert until #1803 promotes a production runtime owner; no supported replacement"},
		"llm.claude_cli.no_session_persistence": {Split: "tracked split: llm.claude_cli.no_session_persistence remains unsupported/inert until #1803 promotes a production runtime owner; no supported replacement"},
		"llm.claude_cli.use_tmux":               {Split: "tracked split: llm.claude_cli.use_tmux remains unsupported/inert until #1803 promotes a production runtime owner; no supported replacement"},
		"llm.openai_compatible":                 section,
		"llm.openai_compatible.base_url":        {Elevated: true},
		"llm.openai_compatible.default_model":   {Split: "retired model-selection input; use llm.models"},
		"llm.openai_compatible.low_cost_model":  {Split: "retired model-selection input; use llm.models"},
		"llm.openai_responses":                  section,
		"llm.openai_responses.base_url":         {Elevated: true},
		"provider_triggers":                     pathSection,
		"provider_triggers.packs":               pathSection,
		"provider_triggers.packs.external_dirs": {ProjectContainedPath: true},
		"budget":                                section,
		"budget.global_monthly_cap":             {},
		"budget.per_entity_monthly_cap":         {},
		"budget.system_monthly_cap":             {},
		"budget.human_tasks":                    section,
		"budget.human_tasks.max_tasks_per_week": {},
		"budget.human_tasks.budget_reset":       {},
		"budget.human_tasks.auto_expire_hours":  {},
		"budget.human_tasks.categories_enabled": {},
		"paths":                                 section,
		"paths.swarm_dir":                       {Elevated: true},
		"paths.contracts_path":                  {ProjectContainedPath: true},
		"paths.platform_spec_path":              {Elevated: true},
		"paths.prompts_dir":                     {ProjectContainedPath: true},
		"paths.artifact_root":                   {Elevated: true},
		"paths.monitor_dir":                     {Elevated: true},
		"paths.agent_config_map_file":           {ProjectContainedPath: true},
		"paths.verification_gates_file":         {ProjectContainedPath: true},
		"paths.tooling_lock_file":               {ProjectContainedPath: true},
	}
}

func unifiedOldFlatKeyRemediation() map[string]string {
	return map[string]string{
		"api_server":            "move to connection.api_server in unified swarm.yaml",
		"api_token_file":        "move to connection.api_token_file in unified swarm.yaml",
		"swarm_dir":             "move to paths.swarm_dir in unified swarm.yaml",
		"contracts_path":        "move to paths.contracts_path in unified swarm.yaml",
		"platform_spec_path":    "move to paths.platform_spec_path in unified swarm.yaml",
		"serve_api_listen_addr": "move to serve.api_listen_addr in unified swarm.yaml",
		"serve_mcp_listen_addr": "move to serve.mcp_listen_addr in unified swarm.yaml",
		"serve_api_token_file":  "move to serve.api_token_file in unified swarm.yaml",
	}
}

func trustViolationRemediation(path string, rule unifiedConfigKeyRule, value *yaml.Node, layer unifiedConfigLayer) string {
	switch layer.Name {
	case unifiedLayerProject:
		if rule.Elevated || rule.SecretReference {
			return "move this key to .swarm/swarm.yaml, user-global swarm.yaml, explicit --config, or a flag"
		}
		if (path == "serve.api_listen_addr" || path == "serve.mcp_listen_addr") && !listenAddrIsProjectSafe(value.Value) {
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

func validateProjectContainedConfigPath(path string, node *yaml.Node, layer unifiedConfigLayer, repoRoot string) []unifiedConfigDiagnostic {
	if node == nil {
		return nil
	}
	if node.Kind == yaml.SequenceNode {
		var diagnostics []unifiedConfigDiagnostic
		for i, item := range node.Content {
			diagnostics = append(diagnostics, validateProjectContainedPathValue(fmt.Sprintf("%s[%d]", path, i), item.Value, layer, repoRoot)...)
		}
		return diagnostics
	}
	return validateProjectContainedPathValue(path, node.Value, layer, repoRoot)
}

func validateProjectContainedPathValue(path, raw string, layer unifiedConfigLayer, repoRoot string) []unifiedConfigDiagnostic {
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
	root := strings.TrimSpace(repoRoot)
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

func unknownUnifiedConfigDiagnostic(path string, layer unifiedConfigLayer) unifiedConfigDiagnostic {
	known := knownUnifiedConfigKeys()
	suggestion := nearestUnifiedConfigKey(path, known)
	message := fmt.Sprintf("unknown config key %q in %s", path, layer.Path)
	if suggestion != "" {
		message += "; did you mean " + suggestion + "?"
	}
	return unifiedConfigDiagnostic{
		Kind:        unifiedConfigDiagnosticUnknownKey,
		Layer:       layer.Name,
		Path:        layer.Path,
		Key:         path,
		Message:     message,
		Remediation: "fix the key name or remove it; unsupported future config must be tracked as split_unsupported before use",
	}
}

func knownUnifiedConfigKeys() []string {
	keys := make([]string, 0, len(unifiedConfigRules())+len(unifiedOldFlatKeyRemediation()))
	for key := range unifiedConfigRules() {
		keys = append(keys, key)
	}
	for key := range unifiedOldFlatKeyRemediation() {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func nearestUnifiedConfigKey(path string, keys []string) string {
	best := ""
	bestDistance := 3
	for _, key := range keys {
		dist := editDistance(path, key)
		if dist < bestDistance {
			bestDistance = dist
			best = key
		}
	}
	return best
}

func addUnifiedConfigDiagnosticsToReport(report *localPreflightReport, diagnostics []unifiedConfigDiagnostic) {
	for _, d := range diagnostics {
		if d.Kind == unifiedConfigDiagnosticLoaded {
			report.addWithOwner(localPreflightConfigPrerequisite, "config_loaded", localPreflightSeverityInfo, localPreflightStatusOK, d.Message, "", unifiedConfigOwner)
			continue
		}
		report.addWithOwner(localPreflightConfigPrerequisite, "config_"+string(d.Kind), localPreflightSeverityBlocker, localPreflightStatusFailed, d.Message, d.Remediation, unifiedConfigOwner)
	}
}

func unifiedConfigDiagnosticsFromError(err error) []unifiedConfigDiagnostic {
	var configErr unifiedConfigError
	if errors.As(err, &configErr) {
		return configErr.Diagnostics
	}
	return nil
}

func unifiedConfigDelegatedSwarmEnvSources(repoRoot, explicitPath string) map[string]string {
	out := map[string]string{}
	repoRoot = unifiedConfigRepoRoot(repoRoot)
	layers, _ := discoverUnifiedConfigLayers(repoRoot, explicitPath)
	var merged yaml.Node
	merged.Kind = yaml.MappingNode
	for _, layer := range layers {
		raw, err := os.ReadFile(layer.Path)
		if err != nil {
			continue
		}
		var doc yaml.Node
		if err := yaml.Unmarshal(raw, &doc); err != nil {
			continue
		}
		root := yamlDocumentRoot(&doc)
		if root == nil || root.Kind != yaml.MappingNode {
			continue
		}
		if diagnostics := validateUnifiedConfigNode(root, layer, repoRoot); len(unifiedConfigBlockers(diagnostics)) > 0 {
			continue
		}
		mergeYAMLMapping(&merged, root)
	}
	delegatedEnv := strings.TrimSpace(yamlScalarPath(&merged, "database", "password_env"))
	if delegatedEnv == "" {
		return out
	}
	if name := delegatedEnv; strings.HasPrefix(name, "SWARM_") {
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
