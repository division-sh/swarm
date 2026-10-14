package cliapp

import (
	"errors"
	"fmt"
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

const (
	unifiedConfigSourceExplicit   = "explicit_config"
	unifiedConfigSourceDeployment = "deployment_config"
	unifiedConfigSourceDefaults   = "built-in default"
)

type unifiedConfigLoadOptions struct {
	RepoRoot        string
	ExplicitPath    string
	BackendOverride string
}

type unifiedConfigLoadResult struct {
	Config        *config.Config
	CLI           cliCommandConfig
	Source        string
	Path          string
	Diagnostics   []unifiedConfigDiagnostic
	sourcePresent *bool
	validated     bool
}

type unifiedConfigSource struct {
	Name    string
	Path    string
	Present *bool
}

type unifiedConfigDiagnosticKind string

const (
	unifiedConfigDiagnosticLoaded           unifiedConfigDiagnosticKind = "loaded"
	unifiedConfigDiagnosticUnknownKey       unifiedConfigDiagnosticKind = "unknown_key"
	unifiedConfigDiagnosticSplitUnsupported unifiedConfigDiagnosticKind = "split_unsupported"
	unifiedConfigDiagnosticReadFailed       unifiedConfigDiagnosticKind = "read_failed"
	unifiedConfigDiagnosticParseFailed      unifiedConfigDiagnosticKind = "parse_failed"
	unifiedConfigDiagnosticValidationFailed unifiedConfigDiagnosticKind = "validation_failed"
)

type unifiedConfigDiagnostic struct {
	Kind         unifiedConfigDiagnosticKind `json:"kind"`
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
	source, diagnostics := selectUnifiedConfigSource(RepoRoot, opts.ExplicitPath)
	document, sourceDiagnostics := readUnifiedConfigSource(source)
	diagnostics = append(diagnostics, sourceDiagnostics...)
	cfg, err := defaultRuntimeConfig()
	if err != nil {
		return unifiedConfigLoadResult{}, err
	}
	if len(document.Content) > 0 {
		if err := config.DecodeSource(yamlsource.ValueFromNode(&document), cfg); err != nil {
			diagnostics = append(diagnostics, unifiedConfigProjectionDiagnostic(err, source.Path))
		}
	}
	backendOverride := strings.TrimSpace(opts.BackendOverride)
	if len(unifiedConfigBlockers(diagnostics)) == 0 {
		if _, err := llmselection.ResolvePersistedBackend(cfg.LLM.Backend); err != nil {
			diagnostics = append(diagnostics, unifiedConfigValidationDiagnostic(err, source.Path, &document, "set llm.backend to a supported backend profile before applying command-line overrides"))
		}
	}
	if backendOverride != "" {
		cfg.LLM.Backend = backendOverride
	}
	if len(unifiedConfigBlockers(diagnostics)) == 0 {
		if err := cfg.Validate(); err != nil {
			diagnostics = append(diagnostics, unifiedConfigValidationDiagnostic(err, source.Path, &document, "fix the referenced swarm.yaml key or command flag"))
		}
	}
	cli, err := decodeUnifiedCLIConfig(&document)
	if err != nil {
		diagnostics = append(diagnostics, unifiedConfigProjectionDiagnostic(err, source.Path))
	}
	validated := len(unifiedConfigBlockers(diagnostics)) == 0
	delegated := map[string]string{}
	if validated {
		delegated = unifiedConfigDelegation(cfg)
	} else {
		diagnostics = unifiedConfigBlockers(diagnostics)
	}
	if err := validateSwarmEnvSources(swarmEnvGuardContext{RepoRoot: RepoRoot, RuntimeConfigPath: opts.ExplicitPath, DelegatedSources: delegated}); err != nil {
		diagnostics = append(diagnostics, unifiedConfigDiagnostic{Kind: unifiedConfigDiagnosticValidationFailed, Path: source.Path, Message: err.Error()})
	}
	result := unifiedConfigLoadResult{
		Config:        cfg,
		CLI:           cli,
		Source:        source.Name,
		Path:          source.Path,
		Diagnostics:   diagnostics,
		sourcePresent: source.Present,
		validated:     validated,
	}
	if blockers := unifiedConfigBlockers(diagnostics); len(blockers) > 0 {
		return result, unifiedConfigError{Diagnostics: blockers}
	}
	return result, nil
}

func unifiedConfigValidationDiagnostic(err error, path string, document *yaml.Node, remediation string) unifiedConfigDiagnostic {
	diagnostic := unifiedConfigDiagnostic{Kind: unifiedConfigDiagnosticValidationFailed, Path: path, Message: err.Error(), Remediation: remediation}
	if path != "" {
		// Runtime validation has no key coordinate; locate the selected document,
		// rather than inferring a field from the validator's human-readable error.
		diagnostic.Line, diagnostic.Column = 1, 1
		if root := yamlDocumentRoot(document); root != nil {
			diagnostic.Line, diagnostic.Column = max(1, root.Line), max(1, root.Column)
		}
		diagnostic.Message = fmt.Sprintf("%s:%d:%d: %v", path, diagnostic.Line, diagnostic.Column, err)
	}
	return diagnostic
}

func unifiedConfigProjectionDiagnostic(err error, path string) unifiedConfigDiagnostic {
	diagnostic := unifiedConfigDiagnostic{
		Kind: unifiedConfigDiagnosticParseFailed, Path: path,
		Message:     fmt.Sprintf("decode swarm.yaml config %s: %v", path, err),
		Remediation: "fix value types in swarm.yaml",
	}
	if path != "" {
		diagnostic.Line, diagnostic.Column = 1, 1
		diagnostic.Message = fmt.Sprintf("%s:1:1: decode swarm.yaml config: %v", path, err)
	}
	if typed, ok := runtimecontracts.AsLoaderDiagnostic(err); ok {
		located := *typed
		located.Location.File = path
		diagnostic.Key = strings.TrimPrefix(located.Location.YAMLPath, "config.")
		diagnostic.Line, diagnostic.Column = located.Location.Line, located.Location.Column
		diagnostic.ValidOptions = located.ValidOptions
		diagnostic.Message = fmt.Sprintf("decode swarm.yaml config %s: %v", path, &located)
		if located.Location.Line > 0 && located.Code != "contract_loader.undefined_field" {
			diagnostic.Message += fmt.Sprintf(" (line %d, column %d)", located.Location.Line, located.Location.Column)
		}
		diagnostic.Remediation = located.Remediation
	}
	return diagnostic
}

func readUnifiedConfigSource(source unifiedConfigSource) (yaml.Node, []unifiedConfigDiagnostic) {
	empty := yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	if source.Path == "" {
		return empty, nil
	}
	raw, err := os.ReadFile(source.Path)
	if err != nil {
		return empty, []unifiedConfigDiagnostic{{
			Kind: unifiedConfigDiagnosticReadFailed, Path: source.Path,
			Message:     fmt.Sprintf("read swarm.yaml config %s: %v", source.Path, err),
			Remediation: unifiedConfigReadRemediation(source),
		}}
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return empty, []unifiedConfigDiagnostic{{
			Kind: unifiedConfigDiagnosticParseFailed, Path: source.Path,
			Message:     fmt.Sprintf("parse swarm.yaml config %s: %v", source.Path, err),
			Remediation: "fix YAML syntax in this config file",
		}}
	}
	root := yamlDocumentRoot(&doc)
	if root == nil || root.Kind == 0 {
		return empty, []unifiedConfigDiagnostic{{Kind: unifiedConfigDiagnosticLoaded, Path: source.Path, Message: "loaded empty swarm.yaml config"}}
	}
	if root.Kind != yaml.MappingNode {
		return empty, []unifiedConfigDiagnostic{{
			Kind: unifiedConfigDiagnosticParseFailed, Path: source.Path,
			Message:     fmt.Sprintf("swarm.yaml config %s must be a YAML mapping", source.Path),
			Remediation: "use sectioned swarm.yaml keys such as runtime, workspace, connection, serve, paths, llm, store, or database",
		}}
	}
	diagnostics := validateUnifiedConfigNode(root, source.Path)
	if len(unifiedConfigBlockers(diagnostics)) != 0 {
		return empty, diagnostics
	}
	expanded, err := expandUnifiedConfigValue(yamlsource.ValueFromNode(root))
	if err != nil {
		return empty, []unifiedConfigDiagnostic{{Kind: unifiedConfigDiagnosticParseFailed, Path: source.Path, Message: err.Error()}}
	}
	return *expanded, []unifiedConfigDiagnostic{{Kind: unifiedConfigDiagnosticLoaded, Path: source.Path, Message: "loaded swarm.yaml config"}}
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

func selectUnifiedConfigSource(RepoRoot, explicitPath string) (unifiedConfigSource, []unifiedConfigDiagnostic) {
	path := strings.TrimSpace(explicitPath)
	if path == "" {
		path = strings.TrimSpace(os.Getenv("SWARM_CONFIG"))
	}
	if path != "" {
		return inspectUnifiedConfigSource(unifiedConfigSource{Name: unifiedConfigSourceExplicit, Path: ResolvePath(RepoRoot, path)})
	}
	path = filepath.Join(RepoRoot, ".swarm", "swarm.yaml")
	return inspectUnifiedConfigSource(unifiedConfigSource{Name: unifiedConfigSourceDeployment, Path: path})
}

func inspectUnifiedConfigSource(source unifiedConfigSource) (unifiedConfigSource, []unifiedConfigDiagnostic) {
	present := false
	source.Present = &present
	if _, err := os.Lstat(source.Path); os.IsNotExist(err) {
		if source.Name == unifiedConfigSourceDeployment {
			return unifiedConfigSource{Name: unifiedConfigSourceDefaults, Present: &present}, []unifiedConfigDiagnostic{{Kind: unifiedConfigDiagnosticLoaded, Message: "using built-in defaults"}}
		}
		return source, nil
	} else if err != nil {
		source.Present = nil
		return source, []unifiedConfigDiagnostic{{
			Kind: unifiedConfigDiagnosticReadFailed, Path: source.Path,
			Message: fmt.Sprintf("inspect selected config %s: %v", source.Path, err),
		}}
	}
	present = true
	return source, nil
}

func unifiedConfigReadRemediation(source unifiedConfigSource) string {
	if source.Name == unifiedConfigSourceExplicit {
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

// Expand aliases and merge keys within the single selected document.
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

func validateUnifiedConfigNode(root *yaml.Node, file string) []unifiedConfigDiagnostic {
	var diagnostics []unifiedConfigDiagnostic
	value := yamlsource.ValueFromNode(root)
	if err := value.ValidateExpansion(); err != nil {
		return []unifiedConfigDiagnostic{{Kind: unifiedConfigDiagnosticParseFailed, Path: file, Message: err.Error()}}
	}
	if err := value.ValidateUniqueMappings(); err != nil {
		return []unifiedConfigDiagnostic{{Kind: unifiedConfigDiagnosticUnknownKey, Path: file, Message: err.Error()}}
	}
	walkUnifiedMapping(value, nil, file, &diagnostics)
	return diagnostics
}

func walkUnifiedMapping(value yamlsource.Value, prefix []string, file string, diagnostics *[]unifiedConfigDiagnostic) {
	entries, err := value.Mapping()
	if err != nil {
		*diagnostics = append(*diagnostics, unifiedConfigDiagnostic{Kind: unifiedConfigDiagnosticParseFailed, Path: file, Message: err.Error()})
		return
	}
	for _, entry := range entries {
		pathParts := append(append([]string{}, prefix...), entry.Name)
		path := strings.Join(pathParts, ".")
		scalar, _ := entry.Value.Scalar() // Containers have no scalar text.
		rule, ok := unifiedConfigRule(pathParts)
		if !ok {
			*diagnostics = append(*diagnostics, unknownUnifiedConfigDiagnostic(path, file, entry.IntroductionLocation()))
			continue
		}
		if path == "llm.backend" && llmselection.NormalizeBackendID(scalar.Value) == llmselection.BackendMock {
			*diagnostics = append(*diagnostics, unifiedConfigDiagnostic{
				Kind: unifiedConfigDiagnosticValidationFailed, Path: file, Key: path,
				Message: fmt.Sprintf("config key %q in %s: backend mock is unsupported as a public selector", path, file), Remediation: "use swarm test to execute authored doubles",
			})
			continue
		}
		if rule.Split != "" {
			*diagnostics = append(*diagnostics, unifiedConfigDiagnostic{
				Kind:        unifiedConfigDiagnosticSplitUnsupported,
				Path:        file,
				Key:         path,
				Message:     fmt.Sprintf("config key %q is recognized but not yet supported", path),
				Remediation: rule.Split,
			})
			continue
		}
		if rule.InlineSecret && strings.TrimSpace(scalar.Value) != "" {
			*diagnostics = append(*diagnostics, unifiedConfigDiagnostic{
				Kind:        unifiedConfigDiagnosticValidationFailed,
				Path:        file,
				Key:         path,
				Message:     fmt.Sprintf("config key %q stores unsupported plaintext secret material", path),
				Remediation: "declare a file, secret key, or explicit env delegation field instead",
			})
		}
		if rule.Container && (entry.Value.Presence() == yamlsource.PresenceMapping || entry.Value.Presence() == yamlsource.PresenceEmptyMapping) {
			walkUnifiedMapping(entry.Value, pathParts, file, diagnostics)
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
	section := unifiedConfigKeyRule{Container: true}
	switch len(pathParts) {
	case 3:
		return section, true
	case 4:
		switch pathParts[3] {
		case "pack", "destination":
			return unifiedConfigKeyRule{}, true
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

func unknownUnifiedConfigDiagnostic(path, file string, location yamlsource.Location) unifiedConfigDiagnostic {
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
	diagnostic.Location = runtimecontracts.LoaderDiagnosticLocation{File: file, YAMLPath: path, Line: location.Line, Column: location.Column}
	return unifiedConfigDiagnostic{
		Kind: unifiedConfigDiagnosticUnknownKey, Path: file, Key: path,
		Message: diagnostic.Error(), Remediation: diagnostic.Remediation,
		Line: location.Line, Column: location.Column, ValidOptions: diagnostic.ValidOptions,
	}
}

func addUnifiedConfigDiagnosticsToReport(report *LocalPreflightReport, diagnostics []unifiedConfigDiagnostic) {
	for _, d := range diagnostics {
		if d.Kind == unifiedConfigDiagnosticLoaded {
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

// The early env guard admits reference declarations, not the deployment.
// Doctor consumes the completed selected-config admission instead.
func unifiedConfigDelegatedSwarmEnvSources(RepoRoot, explicitPath string) map[string]string {
	RepoRoot, err := requireInvocationRootPath(RepoRoot)
	if err != nil {
		return map[string]string{}
	}
	source, diagnostics := selectUnifiedConfigSource(RepoRoot, explicitPath)
	if len(unifiedConfigBlockers(diagnostics)) != 0 {
		return map[string]string{}
	}
	document, diagnostics := readUnifiedConfigSource(source)
	if len(unifiedConfigBlockers(diagnostics)) != 0 {
		return map[string]string{}
	}
	var cfg config.Config
	if err := config.DecodeSource(yamlsource.ValueFromNode(&document), &cfg); err != nil {
		return map[string]string{}
	}
	return unifiedConfigDelegation(&cfg)
}

func unifiedConfigDelegation(cfg *config.Config) map[string]string {
	out := map[string]string{}
	name := strings.TrimSpace(cfg.Database.PasswordEnv)
	if strings.HasPrefix(name, "SWARM_") {
		out[name] = "database.password_env"
	}
	return out
}
