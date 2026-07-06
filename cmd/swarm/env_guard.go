package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	swarmEnvAuthorityOwner = "platform-spec.yaml#environment_source_authority.repo_wide_swarm_env_accepted_set"
	swarmTestHarnessEnv    = "SWARM_TEST_HARNESS"
)

type swarmEnvCategory string

const (
	swarmEnvCategoryBootstrap         swarmEnvCategory = "bootstrap"
	swarmEnvCategoryTypedDelegation   swarmEnvCategory = "typed_delegation"
	swarmEnvCategoryGeneratedBoundary swarmEnvCategory = "generated_boundary"
	swarmEnvCategoryTestQuarantine    swarmEnvCategory = "test_quarantine"
	swarmEnvCategorySeededLegacy      swarmEnvCategory = "seeded_legacy"
	swarmEnvCategoryKnownRetired      swarmEnvCategory = "known_retired"
	swarmEnvCategoryUnknownStale      swarmEnvCategory = "unknown_stale"
)

type swarmEnvCatalogEntry struct {
	Name        string
	Prefix      string
	Category    swarmEnvCategory
	Owner       string
	Migration   string
	Message     string
	Remediation string
}

type swarmEnvFinding struct {
	Name        string           `json:"name"`
	Category    swarmEnvCategory `json:"category"`
	Severity    string           `json:"severity"`
	AcceptedBy  string           `json:"accepted_by,omitempty"`
	Message     string           `json:"message"`
	Remediation string           `json:"remediation,omitempty"`
	Owner       string           `json:"owner"`
}

type swarmEnvGuardError struct {
	findings []swarmEnvFinding
}

func (e swarmEnvGuardError) Error() string {
	if len(e.findings) == 0 {
		return ""
	}
	lines := []string{"environment source authority blockers:"}
	for _, finding := range e.findings {
		lines = append(lines, formatSwarmEnvFinding(finding))
	}
	return strings.Join(lines, "\n")
}

func formatSwarmEnvFinding(finding swarmEnvFinding) string {
	severity := strings.ToUpper(strings.TrimSpace(finding.Severity))
	if severity == "" {
		severity = "INFO"
	}
	line := fmt.Sprintf("[%s] env/%s: %s - %s", severity, finding.Category, finding.Name, finding.Message)
	if remediation := strings.TrimSpace(finding.Remediation); remediation != "" {
		line += "\n  remediation: " + remediation
	}
	return line
}

func validateSwarmEnvForCommand(args []string, repoRoot string) error {
	if shouldSkipSwarmEnvGuard(args) {
		return nil
	}
	findings := collectSwarmEnvFindings(swarmEnvGuardContext{
		RepoRoot:          repoRoot,
		Args:              args,
		RuntimeConfigPath: runtimeConfigPathFromArgs(args),
	})
	blockers := swarmEnvBlockers(findings)
	if len(blockers) == 0 {
		return nil
	}
	return swarmEnvGuardError{findings: blockers}
}

func shouldSkipSwarmEnvGuard(args []string) bool {
	if len(args) == 0 {
		return true
	}
	if isPureHelpFlagRequest(args) {
		return true
	}
	command := firstSwarmCommandArg(args)
	switch command {
	case "", "help", "completion", "doctor":
		return true
	case "version":
		return !versionServerRequested(args)
	default:
		return false
	}
}

func isPureHelpFlagRequest(args []string) bool {
	consumeNext := false
	for _, raw := range args {
		arg := strings.TrimSpace(raw)
		if arg == "" {
			continue
		}
		if arg == "--" {
			return false
		}
		if consumeNext {
			consumeNext = false
			continue
		}
		switch arg {
		case "-h", "--help":
			return true
		}
		if swarmEnvGuardFlagConsumesNext(arg) {
			consumeNext = true
		}
	}
	return false
}

func swarmEnvGuardFlagConsumesNext(arg string) bool {
	arg = strings.TrimSpace(arg)
	if arg == "" || arg == "-" || arg == "--" || !strings.HasPrefix(arg, "-") || strings.Contains(arg, "=") {
		return false
	}
	if strings.HasPrefix(arg, "--") {
		name := strings.TrimPrefix(arg, "--")
		return !swarmEnvGuardKnownBoolLongFlags()[name]
	}
	if strings.HasPrefix(arg, "-") {
		return strings.TrimPrefix(arg, "-") == "m"
	}
	return false
}

func swarmEnvGuardKnownBoolLongFlags() map[string]bool {
	return map[string]bool{
		"abandon-active-runs":     true,
		"all":                     true,
		"client-secret-stdin":     true,
		"code-stdin":              true,
		"delivery-detail":         true,
		"delivery-summary":        true,
		"detach":                  true,
		"dev":                     true,
		"dry-run":                 true,
		"follow":                  true,
		"force":                   true,
		"has-dead-letter":         true,
		"help":                    true,
		"json":                    true,
		"mcp-only":                true,
		"missing":                 true,
		"no-color":                true,
		"no-diagnose":             true,
		"no-follow":               true,
		"no-require-bundle-match": true,
		"no-retry":                true,
		"present":                 true,
		"quiet":                   true,
		"require-bundle-match":    true,
		"self-check":              true,
		"server":                  true,
		"stdin":                   true,
		"target":                  true,
		"verbose":                 true,
		"yes":                     true,
	}
}

func firstSwarmCommandArg(args []string) string {
	for i := 0; i < len(args); i++ {
		arg := strings.TrimSpace(args[i])
		if arg == "" {
			continue
		}
		if arg == "--" {
			return ""
		}
		if arg == "--swarm-dir" {
			i++
			continue
		}
		if strings.HasPrefix(arg, "--swarm-dir=") {
			continue
		}
		if strings.HasPrefix(arg, "-") {
			continue
		}
		return arg
	}
	return ""
}

func versionServerRequested(args []string) bool {
	afterVersion := false
	for _, arg := range args {
		arg = strings.TrimSpace(arg)
		if arg == "" {
			continue
		}
		if arg == "--" {
			return false
		}
		if !afterVersion {
			if arg == "version" {
				afterVersion = true
			}
			continue
		}
		if arg == "--server" {
			return true
		}
		if value, ok := strings.CutPrefix(arg, "--server="); ok {
			value = strings.TrimSpace(strings.ToLower(value))
			return value == "" || value == "1" || value == "t" || value == "true" || value == "yes" || value == "y"
		}
	}
	return false
}

func runtimeConfigPathFromArgs(args []string) string {
	for i := 0; i < len(args); i++ {
		arg := strings.TrimSpace(args[i])
		if arg == "--" {
			return ""
		}
		if arg == "--config" && i+1 < len(args) {
			return strings.TrimSpace(args[i+1])
		}
		if strings.HasPrefix(arg, "--config=") {
			return strings.TrimSpace(strings.TrimPrefix(arg, "--config="))
		}
	}
	return ""
}

type swarmEnvGuardContext struct {
	RepoRoot          string
	Args              []string
	RuntimeConfigPath string
}

func doctorSwarmEnvFindings(repoRoot, runtimeConfigPath string) []swarmEnvFinding {
	return collectSwarmEnvFindings(swarmEnvGuardContext{
		RepoRoot:          repoRoot,
		RuntimeConfigPath: runtimeConfigPath,
	})
}

func collectSwarmEnvFindings(ctx swarmEnvGuardContext) []swarmEnvFinding {
	delegated := delegatedSwarmEnvSources(ctx.RepoRoot, ctx.RuntimeConfigPath)
	entries := swarmEnvCatalogByName()
	prefixes := swarmEnvCatalogPrefixes()
	names := visibleSwarmEnvNames()
	findings := make([]swarmEnvFinding, 0, len(names))
	for _, name := range names {
		if source := delegated[name]; source != "" {
			findings = append(findings, swarmEnvFinding{
				Name:       name,
				Category:   swarmEnvCategoryTypedDelegation,
				Severity:   "info",
				AcceptedBy: source,
				Message:    "accepted by explicit typed config delegation " + source,
				Owner:      swarmEnvAuthorityOwner,
			})
			continue
		}
		entry, ok := entries[name]
		if !ok {
			for _, prefixEntry := range prefixes {
				if strings.HasPrefix(name, prefixEntry.Prefix) {
					entry, ok = prefixEntry, true
					break
				}
			}
		}
		if !ok {
			findings = append(findings, unknownSwarmEnvFinding(name, entries))
			continue
		}
		findings = append(findings, findingForSwarmEnvEntry(name, entry))
	}
	return findings
}

func delegatedSwarmEnvSources(repoRoot, runtimeConfigPath string) map[string]string {
	out := map[string]string{}
	path := runtimeConfigPathForEnvDelegation(repoRoot, runtimeConfigPath)
	if path == "" {
		return out
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return out
	}
	var cfg struct {
		Database struct {
			PasswordEnv string `yaml:"password_env"`
		} `yaml:"database"`
	}
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return out
	}
	if name := strings.TrimSpace(cfg.Database.PasswordEnv); strings.HasPrefix(name, "SWARM_") {
		out[name] = "database.password_env"
	}
	return out
}

func runtimeConfigPathForEnvDelegation(repoRoot, explicitPath string) string {
	if path := strings.TrimSpace(explicitPath); path != "" {
		return resolvePath(repoRoot, path)
	}
	path, ok, err := executableAdjacentRuntimeConfigPath()
	if err != nil || !ok {
		return ""
	}
	return path
}

func visibleSwarmEnvNames() []string {
	seen := map[string]struct{}{}
	for _, item := range os.Environ() {
		name, value, ok := strings.Cut(item, "=")
		name = strings.TrimSpace(name)
		if !ok || !strings.HasPrefix(name, "SWARM_") || strings.TrimSpace(value) == "" {
			continue
		}
		seen[name] = struct{}{}
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func findingForSwarmEnvEntry(name string, entry swarmEnvCatalogEntry) swarmEnvFinding {
	finding := swarmEnvFinding{
		Name:        name,
		Category:    entry.Category,
		Owner:       nonEmpty(entry.Owner, swarmEnvAuthorityOwner),
		Message:     nonEmpty(entry.Message, defaultSwarmEnvMessage(entry)),
		Remediation: entry.Remediation,
	}
	switch entry.Category {
	case swarmEnvCategoryBootstrap, swarmEnvCategorySeededLegacy, swarmEnvCategoryTypedDelegation:
		finding.Severity = "info"
		finding.AcceptedBy = entry.Owner
	case swarmEnvCategoryTestQuarantine:
		if isSwarmEnvTestContext() {
			finding.Severity = "info"
			finding.AcceptedBy = entry.Owner
		} else {
			finding.Severity = "blocker"
			if finding.Remediation == "" {
				finding.Remediation = "unset " + name + "; test-quarantined SWARM_* env is accepted only under the Swarm test harness"
			}
		}
	case swarmEnvCategoryGeneratedBoundary, swarmEnvCategoryKnownRetired:
		finding.Severity = "blocker"
	default:
		finding.Severity = "blocker"
	}
	if finding.Remediation == "" && finding.Severity == "blocker" {
		finding.Remediation = "unset " + name
	}
	return finding
}

func defaultSwarmEnvMessage(entry swarmEnvCatalogEntry) string {
	switch entry.Category {
	case swarmEnvCategorySeededLegacy:
		return "accepted temporarily as seeded legacy env pending migration"
	case swarmEnvCategoryBootstrap:
		return "accepted as bootstrap config locator"
	case swarmEnvCategoryTestQuarantine:
		return "test-quarantined env is not production configuration"
	case swarmEnvCategoryGeneratedBoundary:
		return "generated final-boundary env must be injected by Swarm, not set in the parent process"
	case swarmEnvCategoryKnownRetired:
		return "known retired env source is no longer accepted"
	default:
		return "classified by the repo-wide SWARM env accepted-set"
	}
}

func unknownSwarmEnvFinding(name string, entries map[string]swarmEnvCatalogEntry) swarmEnvFinding {
	message := "unknown SWARM_* env is not accepted; this is usually a stale export or typo"
	if suggestion := nearestSwarmEnvName(name, entries); suggestion != "" {
		message += "; did you mean " + suggestion + "?"
	}
	return swarmEnvFinding{
		Name:        name,
		Category:    swarmEnvCategoryUnknownStale,
		Severity:    "blocker",
		Message:     message,
		Remediation: "unset " + name + " or declare an explicit typed config delegation if this env is intentional",
		Owner:       swarmEnvAuthorityOwner,
	}
}

func swarmEnvBlockers(findings []swarmEnvFinding) []swarmEnvFinding {
	out := []swarmEnvFinding{}
	for _, finding := range findings {
		if strings.EqualFold(finding.Severity, "blocker") {
			out = append(out, finding)
		}
	}
	return out
}

func addSwarmEnvFindingsToLocalPreflightReport(report *localPreflightReport, findings []swarmEnvFinding) {
	if report == nil {
		return
	}
	for _, finding := range findings {
		severity := localPreflightSeverityInfo
		status := localPreflightStatusOK
		if strings.EqualFold(finding.Severity, "blocker") {
			severity = localPreflightSeverityBlocker
			status = localPreflightStatusFailed
		}
		message := strings.TrimSpace(finding.Name)
		if msg := strings.TrimSpace(finding.Message); msg != "" {
			message += " - " + msg
		}
		if acceptedBy := strings.TrimSpace(finding.AcceptedBy); acceptedBy != "" {
			message += " (accepted by " + acceptedBy + ")"
		}
		report.addWithOwner(
			localPreflightEnvPrerequisite,
			string(finding.Category),
			severity,
			status,
			message,
			finding.Remediation,
			finding.Owner,
		)
	}
}

func isSwarmEnvTestContext() bool {
	base := filepath.Base(os.Args[0])
	return strings.HasSuffix(base, ".test")
}

func nearestSwarmEnvName(name string, entries map[string]swarmEnvCatalogEntry) string {
	best := ""
	bestDistance := 4
	for candidate := range entries {
		distance := editDistance(name, candidate)
		if distance < bestDistance {
			bestDistance = distance
			best = candidate
		}
	}
	return best
}

func editDistance(a, b string) int {
	ar := []rune(a)
	br := []rune(b)
	prev := make([]int, len(br)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(ar); i++ {
		curr := make([]int, len(br)+1)
		curr[0] = i
		for j := 1; j <= len(br); j++ {
			cost := 0
			if ar[i-1] != br[j-1] {
				cost = 1
			}
			curr[j] = minInt(curr[j-1]+1, prev[j]+1, prev[j-1]+cost)
		}
		prev = curr
	}
	return prev[len(br)]
}

func minInt(values ...int) int {
	if len(values) == 0 {
		return 0
	}
	min := values[0]
	for _, value := range values[1:] {
		if value < min {
			min = value
		}
	}
	return min
}

func nonEmpty(value, fallback string) string {
	if strings.TrimSpace(value) != "" {
		return value
	}
	return fallback
}

func swarmEnvCatalogByName() map[string]swarmEnvCatalogEntry {
	entries := map[string]swarmEnvCatalogEntry{}
	for _, entry := range swarmEnvCatalogEntries() {
		if entry.Name == "" {
			continue
		}
		entries[entry.Name] = entry
	}
	return entries
}

func swarmEnvCatalogPrefixes() []swarmEnvCatalogEntry {
	prefixes := []swarmEnvCatalogEntry{}
	for _, entry := range swarmEnvCatalogEntries() {
		if entry.Prefix != "" {
			prefixes = append(prefixes, entry)
		}
	}
	sort.Slice(prefixes, func(i, j int) bool {
		return len(prefixes[i].Prefix) > len(prefixes[j].Prefix)
	})
	return prefixes
}

func swarmEnvCatalogEntries() []swarmEnvCatalogEntry {
	e := func(name string, category swarmEnvCategory, owner, migration, message, remediation string) swarmEnvCatalogEntry {
		return swarmEnvCatalogEntry{Name: name, Category: category, Owner: owner, Migration: migration, Message: message, Remediation: remediation}
	}
	p := func(prefix string, category swarmEnvCategory, owner, migration, message, remediation string) swarmEnvCatalogEntry {
		return swarmEnvCatalogEntry{Prefix: prefix, Category: category, Owner: owner, Migration: migration, Message: message, Remediation: remediation}
	}
	seeded := func(name, owner, migration string) swarmEnvCatalogEntry {
		return e(name, swarmEnvCategorySeededLegacy, owner, migration, "", "migrate "+name+" through "+migration+" and then remove it from the seeded accepted set")
	}
	retired := func(name, message, remediation string) swarmEnvCatalogEntry {
		return e(name, swarmEnvCategoryKnownRetired, swarmEnvAuthorityOwner, "retired", message, remediation)
	}
	testOnly := func(name string) swarmEnvCatalogEntry {
		return e(name, swarmEnvCategoryTestQuarantine, swarmEnvAuthorityOwner, "test/debug quarantine", "", "")
	}
	return []swarmEnvCatalogEntry{
		e("SWARM_CONFIG", swarmEnvCategoryBootstrap, "platform-spec.yaml#cli_specification.foundations.api_connection_auth_config_precedence.cli_config_file", "keep bootstrap locator", "", ""),
		retired("SWARM_API_SERVER", "client-side API environment sources are no longer accepted: SWARM_API_SERVER", "use --api-server, --context, project/selected context, or config api_server"),
		retired("SWARM_API_TOKEN", "client-side API environment sources are no longer accepted: SWARM_API_TOKEN", "use --api-token-file, context descriptor auth, config api_token_file, or serve_api_token_file for server auth"),
		retired("SWARM_API_TOKEN_FILE", "client-side API environment sources are no longer accepted: SWARM_API_TOKEN_FILE", "use --api-token-file, context descriptor auth, or config api_token_file"),
		seeded("SWARM_API_LISTEN_ADDR", "platform-spec.yaml#cli_specification.command_catalog.serve.listener_topology_v2_1", "#1600 listener config migration"),
		seeded("SWARM_MCP_LISTEN_ADDR", "platform-spec.yaml#cli_specification.command_catalog.serve.listener_topology_v2_1", "#1600 listener config migration"),
		retired("SWARM_API_PORT", "SWARM_API_PORT is retired; final listener topology uses full listen addresses", "use --api-listen-addr or config serve_api_listen_addr"),
		retired("SWARM_MCP_PORT", "SWARM_MCP_PORT is retired; final listener topology uses full listen addresses", "use --mcp-listen-addr or config serve_mcp_listen_addr"),
		seeded("SWARM_CONTRACTS_PATH", "platform-spec.yaml#cli_specification.foundations.contract_platform_spec_path_resolution", "config contracts_path or --contracts"),
		retired("SWARM_CONTRACTS_DIR", "SWARM_CONTRACTS_DIR is not a promoted CLI source", "use --contracts or config contracts_path"),
		retired("SWARM_PLATFORM_SPEC_PATH", "SWARM_PLATFORM_SPEC_PATH is not promoted", "use --platform-spec or config platform_spec_path where supported"),
		retired("SWARM_DIR", "SWARM_DIR is not promoted as state directory authority", "use --swarm-dir or config swarm_dir"),
		retired("SWARM_HOME", "SWARM_HOME is not promoted as state directory authority", "use --swarm-dir or config swarm_dir"),
		seeded("SWARM_STORE_BACKEND", "platform-spec.yaml#runtime_storage.runtime_store_backend_selection", "#1637 store.backend"),
		seeded("SWARM_SQLITE_PATH", "platform-spec.yaml#runtime_storage.runtime_store_backend_selection.sqlite_path", "#1637 store.sqlite.path"),
		seeded("SWARM_DB_HOST", "platform-spec.yaml#runtime_storage.runtime_store_backend_selection.postgres_connection_details", "#1637 database.host"),
		seeded("SWARM_DB_PORT", "platform-spec.yaml#runtime_storage.runtime_store_backend_selection.postgres_connection_details", "#1637 database.port"),
		seeded("SWARM_DB_NAME", "platform-spec.yaml#runtime_storage.runtime_store_backend_selection.postgres_connection_details", "#1637 database.name"),
		seeded("SWARM_DB_USER", "platform-spec.yaml#runtime_storage.runtime_store_backend_selection.postgres_connection_details", "#1637 database.user"),
		seeded("SWARM_DB_SSLMODE", "platform-spec.yaml#runtime_storage.runtime_store_backend_selection.postgres_connection_details", "#1637 database.sslmode"),
		seeded("SWARM_DB_POOL_SIZE", "platform-spec.yaml#runtime_storage.runtime_store_backend_selection.postgres_connection_details", "#1637 database.pool_size"),
		retired("SWARM_DB_PASSWORD", "SWARM_DB_PASSWORD is not read implicitly; it is accepted only when explicitly named by database.password_env", "unset SWARM_DB_PASSWORD or declare database.password_env: SWARM_DB_PASSWORD in the runtime config"),
		seeded("SWARM_WORKSPACE_DATA_SOURCE", "platform-spec.yaml#workspace_model.data_source_authority", "#1600 workspace.data_source"),
		seeded("SWARM_WORKSPACE_BACKEND", "platform-spec.yaml#workspace_model.workspace_backend_selection", "#1600 workspace.backend"),
		seeded("SWARM_DOCKER_BIN", "platform-spec.yaml#environment_source_authority.workspace_monitor_artifact_debug_slice", "#1600 workspace/tooling config"),
		seeded("SWARM_WORKSPACE_IMAGE", "platform-spec.yaml#workspace_model.runtime_image_packaging", "#1600 workspace.image"),
		seeded("SWARM_WORKSPACE_HOST_ROOT", "platform-spec.yaml#workspace_model.workspace_backend_selection", "#1600 workspace.host_root"),
		seeded("SWARM_WORKSPACE_VOLUMES_FROM", "platform-spec.yaml#workspace_model.data_source_authority", "#1600 workspace volumes config"),
		seeded("SWARM_WORKSPACE_NETWORK", "platform-spec.yaml#environment_source_authority.workspace_monitor_artifact_debug_slice", "#1600 workspace network config"),
		seeded("SWARM_WORKSPACE_DATA_MOUNT", "platform-spec.yaml#environment_source_authority.workspace_monitor_artifact_debug_slice", "#1600 workspace mount config"),
		seeded("SWARM_WORKSPACE_CONTRACTS_SOURCE", "platform-spec.yaml#environment_source_authority.workspace_monitor_artifact_debug_slice", "#1600 workspace contracts source config"),
		seeded("SWARM_WORKSPACE_CONTRACTS_MOUNT", "platform-spec.yaml#environment_source_authority.workspace_monitor_artifact_debug_slice", "#1600 workspace contracts mount config"),
		seeded("SWARM_SCAFFOLD_CONTAINER", "platform-spec.yaml#environment_source_authority.workspace_monitor_artifact_debug_slice", "#1600 workspace lifecycle config"),
		seeded("SWARM_SCAFFOLD_WORKDIR", "platform-spec.yaml#environment_source_authority.workspace_monitor_artifact_debug_slice", "#1600 workspace lifecycle config"),
		seeded("SWARM_SCAFFOLD_VOLUME", "platform-spec.yaml#environment_source_authority.workspace_monitor_artifact_debug_slice", "#1600 workspace lifecycle config"),
		seeded("SWARM_SYSTEM_CONTAINER", "platform-spec.yaml#environment_source_authority.workspace_monitor_artifact_debug_slice", "#1600 workspace lifecycle config"),
		seeded("SWARM_SYSTEM_WORKDIR", "platform-spec.yaml#environment_source_authority.workspace_monitor_artifact_debug_slice", "#1600 workspace lifecycle config"),
		seeded("SWARM_SYSTEM_ENTITIES_VOLUME", "platform-spec.yaml#environment_source_authority.workspace_monitor_artifact_debug_slice", "#1600 workspace lifecycle config"),
		seeded("SWARM_SYSTEM_NGINX_VOLUME", "platform-spec.yaml#environment_source_authority.workspace_monitor_artifact_debug_slice", "#1600 workspace lifecycle config"),
		seeded("SWARM_SYSTEM_SYSTEMD_VOLUME", "platform-spec.yaml#environment_source_authority.workspace_monitor_artifact_debug_slice", "#1600 workspace lifecycle config"),
		seeded("SWARM_ENTITY_CONTAINER_PREFIX", "platform-spec.yaml#environment_source_authority.workspace_monitor_artifact_debug_slice", "#1600 workspace lifecycle config"),
		seeded("SWARM_ENTITY_WORKDIR", "platform-spec.yaml#environment_source_authority.workspace_monitor_artifact_debug_slice", "#1600 workspace lifecycle config"),
		seeded("SWARM_RUNTIME_RECOVERY_ON_STARTUP", "platform-spec.yaml#runtime_storage.runtime_store_backend_selection", "#1600 runtime.recovery_on_startup"),
		retired("SWARM_RUNTIME_MAX_CONCURRENT_AGENTS", "SWARM_RUNTIME_MAX_CONCURRENT_AGENTS is unsupported inert runtime control", "remove it; no runtime path enforces this control"),
		retired("SWARM_RUNTIME_EVENT_POLL_INTERVAL", "SWARM_RUNTIME_EVENT_POLL_INTERVAL is unsupported inert runtime control", "remove it; no runtime path enforces this control"),
		seeded("SWARM_LLM_SESSION_LOCK_TTL", "platform-spec.yaml#engine.agent_session_management.llm_provider_selection_config_authority", "#1600 llm.session.lock_ttl"),
		seeded("SWARM_LLM_SESSION_ROTATE_AFTER_TURNS", "platform-spec.yaml#engine.agent_session_management.llm_provider_selection_config_authority", "#1600 llm.session.rotate_after_turns"),
		seeded("SWARM_LLM_SESSION_ROTATE_ON_PARSE_FAILURES", "platform-spec.yaml#engine.agent_session_management.llm_provider_selection_config_authority", "#1600 llm.session.rotate_on_parse_failures"),
		seeded("SWARM_CLAUDE_API_MAX_RETRIES", "platform-spec.yaml#engine.agent_session_management.llm_provider_selection_config_authority", "#1600 llm.claude_api.max_retries"),
		seeded("SWARM_CLAUDE_API_RETRY_BACKOFF", "platform-spec.yaml#engine.agent_session_management.llm_provider_selection_config_authority", "#1600 llm.claude_api.retry_backoff"),
		seeded("SWARM_CLAUDE_CLI_COMMAND", "platform-spec.yaml#engine.agent_session_management.llm_provider_selection_config_authority", "#1600 llm.claude_cli.command"),
		seeded("SWARM_CLAUDE_CLI_TIMEOUT", "platform-spec.yaml#engine.agent_session_management.llm_provider_selection_config_authority", "#1600 llm.claude_cli.timeout"),
		seeded("SWARM_CLAUDE_CLI_OUTPUT_FORMAT", "platform-spec.yaml#engine.agent_session_management.llm_provider_selection_config_authority", "#1600 llm.claude_cli.output_format"),
		seeded("SWARM_CLAUDE_CLI_RETRIES", "platform-spec.yaml#engine.agent_session_management.llm_provider_selection_config_authority", "#1600 llm.claude_cli.retries"),
		seeded("SWARM_CLAUDE_CLI_NO_SESSION_PERSISTENCE", "platform-spec.yaml#engine.agent_session_management.llm_provider_selection_config_authority", "#1600 llm.claude_cli.no_session_persistence"),
		seeded("SWARM_CLAUDE_CLI_USE_TMUX", "platform-spec.yaml#engine.agent_session_management.llm_provider_selection_config_authority", "#1600 llm.claude_cli.use_tmux"),
		seeded("SWARM_CLAUDE_TIMEOUT_SECONDS", "platform-spec.yaml#engine.agent_session_management.llm_provider_selection_config_authority", "#1600 llm.claude_cli.timeout"),
		seeded("SWARM_CLAUDE_PERMISSION_MODE", "platform-spec.yaml#engine.agent_session_management.llm_provider_selection_config_authority", "#1600 llm.claude_cli.permission_mode"),
		seeded("SWARM_CLAUDE_BYPASS_PERMISSIONS", "platform-spec.yaml#engine.agent_session_management.llm_provider_selection_config_authority", "#1600 llm.claude_cli.permission_mode"),
		seeded("SWARM_CLAUDE_USE_MCP", "platform-spec.yaml#cli_specification.foundations.local_tool_gateway_binding", "#1600 typed Claude CLI transport config"),
		retired("SWARM_LLM_BACKEND", "SWARM_LLM_BACKEND is retired; use --backend or llm.backend", "use --backend or llm.backend"),
		retired("SWARM_LLM_RUNTIME_MODE", "SWARM_LLM_RUNTIME_MODE is retired; use llm.backend", "use llm.backend"),
		retired("SWARM_CLAUDE_DEFAULT_MODEL", "SWARM_CLAUDE_DEFAULT_MODEL is retired for model selection; use llm.models", "use llm.models"),
		retired("SWARM_CLAUDE_HAIKU_MODEL", "SWARM_CLAUDE_HAIKU_MODEL is retired for model selection; use llm.models", "use llm.models"),
		retired("SWARM_OPENAI_COMPATIBLE_BASE_URL", "SWARM_OPENAI_COMPATIBLE_BASE_URL is retired; use llm.openai_compatible.base_url", "use llm.openai_compatible.base_url"),
		retired("SWARM_OPENAI_COMPATIBLE_DEFAULT_MODEL", "SWARM_OPENAI_COMPATIBLE_DEFAULT_MODEL is retired for model selection; use llm.models", "use llm.models"),
		retired("SWARM_OPENAI_COMPATIBLE_LOW_COST_MODEL", "SWARM_OPENAI_COMPATIBLE_LOW_COST_MODEL is retired for model selection; use llm.models", "use llm.models"),
		seeded("SWARM_CREDENTIALS_FILE", "platform-spec.yaml#environment_source_authority.repo_wide_swarm_env_accepted_set", "#1600 secrets file config"),
		seeded("SWARM_MANAGED_CREDENTIALS_FILE", "platform-spec.yaml#environment_source_authority.repo_wide_swarm_env_accepted_set", "#1600 managed credentials file config"),
		seeded("SWARM_ARTIFACT_ROOT", "platform-spec.yaml#runtime_storage.artifact_root", "#1600 runtime.artifact_root"),
		seeded("SWARM_MONITOR_DIR", "platform-spec.yaml#environment_source_authority.workspace_monitor_artifact_debug_slice", "#1600 monitor config"),
		seeded("SWARM_PROMPTS_DIR", "platform-spec.yaml#environment_source_authority.workspace_monitor_artifact_debug_slice", "#1600 prompt helper config"),
		seeded("SWARM_AGENT_CONFIG_MAP_FILE", "platform-spec.yaml#environment_source_authority.workspace_monitor_artifact_debug_slice", "#1600 spec helper config"),
		seeded("SWARM_VERIFICATION_GATES_FILE", "platform-spec.yaml#environment_source_authority.workspace_monitor_artifact_debug_slice", "#1600 spec helper config"),
		seeded("SWARM_TOOLING_LOCK_FILE", "platform-spec.yaml#environment_source_authority.workspace_monitor_artifact_debug_slice", "#1600 spec helper config"),
		retired("SWARM_LOG_LEVEL", "SWARM_LOG_LEVEL is not promoted as CLI logging source", "use --log-level on supported commands"),
		testOnly("SWARM_SQL_DEBUG"),
		testOnly("SWARM_BOOT_WARNINGS_FATAL"),
		testOnly("SWARM_EMIT_SCHEMA_STRICT"),
		testOnly("SWARM_CATALOG_E2E_DEBUG"),
		testOnly("SWARM_FAKE_DOCKER_STATE"),
		testOnly("SWARM_LLM_FIRST_TURN_FAKE_DOCKER"),
		testOnly(swarmTestHarnessEnv),
		p("SWARM_TEST_", swarmEnvCategoryTestQuarantine, swarmEnvAuthorityOwner, "test/debug quarantine", "", ""),
		e("SWARM_TOOL_GATEWAY_URL", swarmEnvCategoryGeneratedBoundary, "platform-spec.yaml#cli_specification.foundations.local_tool_gateway_binding", "generated final-boundary only", "SWARM_TOOL_GATEWAY_URL is retired and not accepted as gateway endpoint configuration; generated final-boundary env must be injected by Swarm", "unset SWARM_TOOL_GATEWAY_URL; local serve/run derives the gateway binding from the bound MCP listener and ignores this retired URL"),
		e("SWARM_TOOL_GATEWAY_CONTAINER_URL", swarmEnvCategoryGeneratedBoundary, "platform-spec.yaml#cli_specification.foundations.local_tool_gateway_binding", "generated final-boundary only", "SWARM_TOOL_GATEWAY_CONTAINER_URL is retired and not accepted as gateway endpoint configuration; generated final-boundary env must be injected by Swarm", "unset SWARM_TOOL_GATEWAY_CONTAINER_URL; local serve/run derives the gateway binding from the bound MCP listener and ignores this retired URL"),
		retired("SWARM_TOOL_GATEWAY_TOKEN", "SWARM_TOOL_GATEWAY_TOKEN is retired; ToolGatewayBinding owns per-boot gateway auth", "unset SWARM_TOOL_GATEWAY_TOKEN; use the generated ToolGatewayBinding token path"),
		retired("SWARM_BUILDER_AUTH_TOKEN", "SWARM_BUILDER_AUTH_TOKEN is not accepted as API auth fallback", "use token files, context descriptor auth, or configured auth sources"),
		retired("SWARM_OPERATOR_AUTH_TOKEN", "SWARM_OPERATOR_AUTH_TOKEN is not accepted as API auth fallback", "use token files, context descriptor auth, or configured auth sources"),
	}
}
