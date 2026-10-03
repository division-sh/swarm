package cliapp

import (
	"os"
	"path/filepath"
	"sort"
	"strings"

	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
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
	swarmEnvCategoryUnknownStale      swarmEnvCategory = "unknown_stale"
)

type swarmEnvCatalogEntry struct {
	Name        string
	Prefix      string
	Category    swarmEnvCategory
	Owner       string
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
	severity := runtimebootverify.SeverityLintEvidence
	if strings.EqualFold(finding.Severity, "blocker") {
		severity = runtimebootverify.SeverityHardInvalidity
	} else if strings.EqualFold(finding.Severity, "warning") {
		severity = runtimebootverify.SeveritySemanticDriftWarn
	}
	return runtimebootverify.FormatTypedDiagnosticFinding(runtimebootverify.TypedDiagnosticFinding{
		CheckID:     "env/" + strings.TrimSpace(string(finding.Category)),
		Severity:    severity,
		Location:    strings.TrimSpace(finding.Name),
		Message:     strings.TrimSpace(finding.Message),
		Remediation: strings.TrimSpace(finding.Remediation),
	}, false)
}

func validateSwarmEnvForCommand(args []string, RepoRoot string) error {
	if shouldSkipSwarmEnvGuard(args) {
		return nil
	}
	return validateSwarmEnvSources(swarmEnvGuardContext{
		RepoRoot:          RepoRoot,
		Args:              args,
		RuntimeConfigPath: runtimeConfigPathFromArgs(args),
	})
}

func validateSwarmEnvSources(ctx swarmEnvGuardContext) error {
	blockers := swarmEnvBlockers(collectSwarmEnvFindings(ctx))
	if len(blockers) == 0 {
		return nil
	}
	return swarmEnvGuardError{findings: blockers}
}

// ValidateGeneratedBoundaryEnv preserves the parent-process boundary when a
// serve consumer receives an already-built config rather than a source file.
func ValidateGeneratedBoundaryEnv() error {
	var findings []swarmEnvFinding
	for _, entry := range swarmEnvCatalogEntries() {
		if entry.Category == swarmEnvCategoryGeneratedBoundary && strings.TrimSpace(os.Getenv(entry.Name)) != "" {
			findings = append(findings, findingForSwarmEnvEntry(entry.Name, entry))
		}
	}
	if len(findings) != 0 {
		return swarmEnvGuardError{findings: findings}
	}
	return nil
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
		"abandon-active-runs": true,
		"all":                 true,
		"client-secret-stdin": true,
		"code-stdin":          true,
		"delivery-detail":     true,
		"delivery-summary":    true,
		"detach":              true,
		"dev":                 true,
		"dry-run":             true,
		"follow":              true,
		"force":               true,
		"has-dead-letter":     true,
		"help":                true,
		"json":                true,
		"mcp-only":            true,
		"missing":             true,
		"no-color":            true,
		"no-diagnose":         true,
		"no-follow":           true,
		"no-retry":            true,
		"present":             true,
		"quiet":               true,
		"self-check":          true,
		"server":              true,
		"stdin":               true,
		"target":              true,
		"verbose":             true,
		"yes":                 true,
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
		if arg == "--swarm-dir" || arg == "--config" {
			i++
			continue
		}
		if strings.HasPrefix(arg, "--swarm-dir=") || strings.HasPrefix(arg, "--config=") {
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

func doctorSwarmEnvFindings(RepoRoot, runtimeConfigPath string) []swarmEnvFinding {
	return collectSwarmEnvFindings(swarmEnvGuardContext{
		RepoRoot:          RepoRoot,
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

func delegatedSwarmEnvSources(RepoRoot, runtimeConfigPath string) map[string]string {
	return unifiedConfigDelegatedSwarmEnvSources(RepoRoot, runtimeConfigPath)
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
	case swarmEnvCategoryGeneratedBoundary:
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
		return "accepted by the current runtime source"
	case swarmEnvCategoryBootstrap:
		return "accepted as bootstrap config locator"
	case swarmEnvCategoryTestQuarantine:
		return "test-quarantined env is not production configuration"
	case swarmEnvCategoryGeneratedBoundary:
		return "generated final-boundary env must be injected by Swarm, not set in the parent process"
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

func addSwarmEnvFindingsToLocalPreflightReport(report *LocalPreflightReport, findings []swarmEnvFinding) {
	if report == nil {
		return
	}
	for _, finding := range findings {
		severity := LocalPreflightSeverityInfo
		status := LocalPreflightStatusOK
		if strings.EqualFold(finding.Severity, "blocker") {
			severity = LocalPreflightSeverityBlocker
			status = LocalPreflightStatusFailed
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
	return []swarmEnvCatalogEntry{
		{Name: "SWARM_CONFIG", Category: swarmEnvCategoryBootstrap, Owner: unifiedConfigOwner},
		{Name: "SWARM_CLAUDE_PERMISSION_MODE", Category: swarmEnvCategorySeededLegacy, Owner: "platform-spec.yaml#engine.agent_session_management.llm_provider_selection_config_authority"},
		{Name: "SWARM_CLAUDE_BYPASS_PERMISSIONS", Category: swarmEnvCategorySeededLegacy, Owner: "platform-spec.yaml#engine.agent_session_management.llm_provider_selection_config_authority"},
		{Name: "SWARM_CLAUDE_USE_MCP", Category: swarmEnvCategorySeededLegacy, Owner: "platform-spec.yaml#cli_specification.foundations.local_tool_gateway_binding"},
		{Name: "SWARM_CREDENTIALS_FILE", Category: swarmEnvCategorySeededLegacy, Owner: swarmEnvAuthorityOwner},
		{Name: "SWARM_MANAGED_CREDENTIALS_FILE", Category: swarmEnvCategorySeededLegacy, Owner: swarmEnvAuthorityOwner},
		{Name: "SWARM_MONITOR_DIR", Category: swarmEnvCategorySeededLegacy, Owner: "platform-spec.yaml#environment_source_authority.workspace_monitor_artifact_debug_slice"},
		{Name: "SWARM_SQL_DEBUG", Category: swarmEnvCategoryTestQuarantine, Owner: swarmEnvAuthorityOwner},
		{Name: "SWARM_BOOT_WARNINGS_FATAL", Category: swarmEnvCategoryTestQuarantine, Owner: swarmEnvAuthorityOwner},
		{Name: "SWARM_EMIT_SCHEMA_STRICT", Category: swarmEnvCategoryTestQuarantine, Owner: swarmEnvAuthorityOwner},
		{Name: "SWARM_CATALOG_E2E_DEBUG", Category: swarmEnvCategoryTestQuarantine, Owner: swarmEnvAuthorityOwner},
		{Name: "SWARM_FAKE_DOCKER_STATE", Category: swarmEnvCategoryTestQuarantine, Owner: swarmEnvAuthorityOwner},
		{Name: "SWARM_LLM_FIRST_TURN_FAKE_DOCKER", Category: swarmEnvCategoryTestQuarantine, Owner: swarmEnvAuthorityOwner},
		{Name: swarmTestHarnessEnv, Category: swarmEnvCategoryTestQuarantine, Owner: swarmEnvAuthorityOwner},
		{Prefix: "SWARM_TEST_", Category: swarmEnvCategoryTestQuarantine, Owner: swarmEnvAuthorityOwner},
		{Name: "SWARM_TOOL_GATEWAY_URL", Category: swarmEnvCategoryGeneratedBoundary, Owner: "platform-spec.yaml#cli_specification.foundations.local_tool_gateway_binding"},
		{Name: "SWARM_TOOL_GATEWAY_CONTAINER_URL", Category: swarmEnvCategoryGeneratedBoundary, Owner: "platform-spec.yaml#cli_specification.foundations.local_tool_gateway_binding"},
	}
}
