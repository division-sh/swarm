package main

import (
	"fmt"
	"sort"
	"strings"
)

type unifiedConfigExampleTier string

const (
	unifiedConfigExampleTierProjectSafe          unifiedConfigExampleTier = "project_safe"
	unifiedConfigExampleTierProjectContainedPath unifiedConfigExampleTier = "project_contained_path"
	unifiedConfigExampleTierFullTrust            unifiedConfigExampleTier = "full_trust"
	unifiedConfigExampleTierElevated             unifiedConfigExampleTier = "elevated"
	unifiedConfigExampleTierSecretReference      unifiedConfigExampleTier = "secret_reference"
)

type unifiedConfigExampleEntry struct {
	Path               string
	Value              string
	Description        string
	Tier               unifiedConfigExampleTier
	ValidateInSample   bool
	RequiresRuleLookup bool
}

func unifiedConfigExampleEntries() []unifiedConfigExampleEntry {
	e := func(path, value, description string, tier unifiedConfigExampleTier) unifiedConfigExampleEntry {
		return unifiedConfigExampleEntry{Path: path, Value: value, Description: description, Tier: tier, ValidateInSample: true, RequiresRuleLookup: true}
	}
	dynamic := func(path, value, description string, tier unifiedConfigExampleTier) unifiedConfigExampleEntry {
		return unifiedConfigExampleEntry{Path: path, Value: value, Description: description, Tier: tier, ValidateInSample: true, RequiresRuleLookup: true}
	}
	skippedValidation := func(path, value, description string, tier unifiedConfigExampleTier) unifiedConfigExampleEntry {
		entry := e(path, value, description, tier)
		entry.ValidateInSample = false
		return entry
	}
	return []unifiedConfigExampleEntry{
		e("runtime.recovery_on_startup", "false", "Recover persisted runtime state on startup.", unifiedConfigExampleTierProjectSafe),
		e("runtime.decision_card_first_reminder", "4h", "Delay before the first pending decision-card reminder.", unifiedConfigExampleTierProjectSafe),
		e("runtime.decision_card_urgency", "24h", "Delay before a pending decision card becomes urgent.", unifiedConfigExampleTierProjectSafe),
		e("runtime.decision_card_reminder_interval", "24h", "Cadence for later pending decision-card reminders.", unifiedConfigExampleTierProjectSafe),
		e("runtime.decision_card_input_draft_ttl", "15m", "Lifetime of an incomplete actor-scoped decision input draft.", unifiedConfigExampleTierProjectSafe),
		e("serve.api_listen_addr", `"127.0.0.1:8081"`, "Loopback API listener. Public or wildcard binds require local/operator config or a flag.", unifiedConfigExampleTierProjectSafe),
		e("serve.mcp_listen_addr", `"127.0.0.1:8082"`, "Loopback MCP listener. Public or wildcard binds require local/operator config or a flag.", unifiedConfigExampleTierProjectSafe),
		e("workspace.backend", "docker", "Workspace backend preference; actual isolation still follows capability policy.", unifiedConfigExampleTierProjectSafe),
		e("llm.backend", "anthropic", "Active LLM backend profile.", unifiedConfigExampleTierProjectSafe),
		dynamic("llm.models.regular.anthropic", "claude-sonnet-4-5", "Anthropic model to use for the regular alias.", unifiedConfigExampleTierProjectSafe),
		e("llm.session.lock_ttl", "10s", "How long a session lock can be held before it expires.", unifiedConfigExampleTierProjectSafe),
		e("llm.session.rotate_after_turns", "40", "Rotate an LLM session after this many turns.", unifiedConfigExampleTierProjectSafe),
		e("llm.session.rotate_on_parse_failures", "3", "Rotate an LLM session after repeated parse failures.", unifiedConfigExampleTierProjectSafe),
		dynamic("llm.provider_limits.anthropic.rate_limit", "30/m", "Provider-wide request rate limit.", unifiedConfigExampleTierProjectSafe),
		dynamic("llm.provider_limits.anthropic.rate_limit_max_wait", "30s", "Maximum wait for provider-wide rate limiting.", unifiedConfigExampleTierProjectSafe),
		dynamic("llm.provider_limits.anthropic.max_concurrency", "2", "Provider-wide concurrency limit.", unifiedConfigExampleTierProjectSafe),
		dynamic("llm.provider_limits.anthropic.max_concurrency_max_wait", "30s", "Maximum wait for provider-wide concurrency limiting.", unifiedConfigExampleTierProjectSafe),
		dynamic("llm.provider_limits.anthropic.models.claude-sonnet-4-5.rate_limit", "10/m", "Model-specific request rate limit.", unifiedConfigExampleTierProjectSafe),
		dynamic("llm.provider_limits.anthropic.models.claude-sonnet-4-5.rate_limit_max_wait", "30s", "Maximum wait for model-specific rate limiting.", unifiedConfigExampleTierProjectSafe),
		e("llm.claude_cli.timeout", "1h", "Claude CLI request timeout.", unifiedConfigExampleTierProjectSafe),
		e("llm.claude_cli.output_format", "stream-json", "Claude CLI output format.", unifiedConfigExampleTierProjectSafe),
		e("budget.global_monthly_cap", "0", "Global monthly budget cap; zero leaves enforcement disabled.", unifiedConfigExampleTierProjectSafe),
		e("budget.per_entity_monthly_cap", "0", "Per-entity monthly budget cap; zero leaves enforcement disabled.", unifiedConfigExampleTierProjectSafe),
		e("budget.system_monthly_cap", "0", "System actor monthly budget cap; zero leaves enforcement disabled.", unifiedConfigExampleTierProjectSafe),
		e("budget.human_tasks.max_tasks_per_week", "0", "Human task weekly cap; zero leaves enforcement disabled.", unifiedConfigExampleTierProjectSafe),
		e("budget.human_tasks.budget_reset", "monday", "Weekly human-task budget reset day.", unifiedConfigExampleTierProjectSafe),
		e("budget.human_tasks.auto_expire_hours", "168", "Human task auto-expiration window in hours.", unifiedConfigExampleTierProjectSafe),
		e("budget.human_tasks.categories_enabled", `["ops"]`, "Human task categories covered by the budget.", unifiedConfigExampleTierProjectSafe),

		e("workspace.data_source", "./.swarm/data", "Project-contained workspace data source.", unifiedConfigExampleTierProjectContainedPath),
		e("provider_triggers.packs.external_dirs", `["./provider-packs"]`, "Project-contained provider trigger pack directories.", unifiedConfigExampleTierProjectContainedPath),
		e("paths.contracts_path", "./contracts", "Project-contained contract bundle root.", unifiedConfigExampleTierProjectContainedPath),
		e("paths.platform_spec_path", "platform-spec.yaml", "Project-contained platform spec path override.", unifiedConfigExampleTierProjectContainedPath),
		e("paths.prompts_dir", "./prompts", "Project-contained prompt directory.", unifiedConfigExampleTierProjectContainedPath),
		e("paths.agent_config_map_file", "./agent-config-map.yaml", "Project-contained agent config map file.", unifiedConfigExampleTierProjectContainedPath),
		e("paths.verification_gates_file", "./verification-gates.yaml", "Project-contained verification gates file.", unifiedConfigExampleTierProjectContainedPath),
		e("paths.tooling_lock_file", "./tooling.lock", "Project-contained tooling lock file.", unifiedConfigExampleTierProjectContainedPath),

		e("connection.api_server", "http://127.0.0.1:8081", "CLI API target. Keep out of shared project config and this-machine config.", unifiedConfigExampleTierFullTrust),
		e("connection.api_token_file", ".swarm/api-token", "CLI API bearer-token file reference; never inline the token.", unifiedConfigExampleTierFullTrust),
		e("serve.api_token_file", ".swarm/serve-api-token", "Serve API bearer-token file reference; never inline the token.", unifiedConfigExampleTierFullTrust),

		e("store.backend", "sqlite", "Runtime store backend.", unifiedConfigExampleTierElevated),
		e("store.sqlite.path", ".swarm/stores/dev.db", "SQLite runtime store path.", unifiedConfigExampleTierElevated),
		e("database.host", "127.0.0.1", "Postgres host for postgres store deployments.", unifiedConfigExampleTierElevated),
		e("database.port", "5432", "Postgres port for postgres store deployments.", unifiedConfigExampleTierElevated),
		e("database.name", "swarm", "Postgres database name.", unifiedConfigExampleTierElevated),
		e("database.user", "postgres", "Postgres user.", unifiedConfigExampleTierElevated),
		e("database.sslmode", "disable", "Postgres SSL mode.", unifiedConfigExampleTierElevated),
		e("database.pool_size", "5", "Postgres connection pool size.", unifiedConfigExampleTierElevated),
		e("workspace.allow_exec_on_host", "false", "Unsafe host execution opt-in; requires local/operator config or a flag.", unifiedConfigExampleTierElevated),
		e("workspace.image", "swarm-workspace:latest", "Workspace image for Docker-backed execution.", unifiedConfigExampleTierElevated),
		e("workspace.docker_bin", "docker", "Docker executable path/name.", unifiedConfigExampleTierElevated),
		e("workspace.host_root", "~/.swarm/workspaces", "Host workspace root for local/operator use.", unifiedConfigExampleTierElevated),
		e("workspace.volumes_from", "swarm-orchestrator", "Container volume source for orchestrated deployments.", unifiedConfigExampleTierElevated),
		e("workspace.network", "swarm", "Docker network for workspaces.", unifiedConfigExampleTierElevated),
		e("llm.claude_cli.command", "claude", "Claude CLI executable path/name.", unifiedConfigExampleTierElevated),
		e("llm.openai_compatible.base_url", "https://api.example.com/v1", "OpenAI-compatible endpoint base URL.", unifiedConfigExampleTierElevated),
		e("llm.openai_responses.base_url", "https://api.openai.com/v1", "OpenAI Responses endpoint base URL.", unifiedConfigExampleTierElevated),
		e("provider_triggers.packs.platform_dirs", `["packs/provider-triggers/github", "packs/provider-triggers/intercom", "packs/provider-triggers/shopify", "packs/provider-triggers/slack", "packs/provider-triggers/stripe", "packs/provider-triggers/telegram", "packs/provider-triggers/twilio", "packs/provider-triggers/typeform"]`, "Required first-party provider trigger pack directories; machine/operator config only.", unifiedConfigExampleTierElevated),
		e("paths.swarm_dir", ".swarm", "Swarm state directory.", unifiedConfigExampleTierElevated),
		e("paths.artifact_root", ".swarm/artifacts", "Runtime artifact root.", unifiedConfigExampleTierElevated),
		e("paths.monitor_dir", ".swarm/monitor", "Runtime monitor artifact directory.", unifiedConfigExampleTierElevated),

		e("database.password_secret_key", "postgres_password", "Database password key in the Swarm secrets store.", unifiedConfigExampleTierSecretReference),
		skippedValidation("database.password_file", "/run/secrets/postgres-password", "Database password file for orchestrator-mounted secrets.", unifiedConfigExampleTierSecretReference),
		skippedValidation("database.password_env", "DB_PASSWORD", "Explicit env delegation for orchestrator-injected database password.", unifiedConfigExampleTierSecretReference),
	}
}

func generatedUnifiedConfigExample() string {
	var b strings.Builder
	b.WriteString("# swarm.yaml - Swarm's config file; copy keys into ./swarm.yaml, .swarm/swarm.yaml, or --config <file>.\n")
	b.WriteString("# Everything is commented out; uncomment to override a default. Run `swarm doctor --target` to inspect the active source.\n")
	b.WriteString("# Inline secret values are absent; use secret keys, token files, secret files, or explicit env delegation.\n")
	b.WriteString("# Contributors: generated file - edit the generator metadata, not this file; see drift test.\n\n")

	tiers := []unifiedConfigExampleTier{
		unifiedConfigExampleTierProjectSafe,
		unifiedConfigExampleTierProjectContainedPath,
		unifiedConfigExampleTierFullTrust,
		unifiedConfigExampleTierElevated,
		unifiedConfigExampleTierSecretReference,
	}
	for i, tier := range tiers {
		b.WriteString("# " + unifiedConfigExampleTierTitle(tier) + "\n")
		b.WriteString("# " + unifiedConfigExampleTierDescription(tier) + "\n")
		writeCommentedYAMLTree(&b, entriesForUnifiedConfigExampleTier(tier, false))
		if i < len(tiers)-1 {
			b.WriteString("\n")
		}
	}

	return b.String()
}

func generatedUnifiedConfigValidationSample() string {
	var b strings.Builder
	writeActiveYAMLTree(&b, entriesForUnifiedConfigExampleTier("", true))
	return b.String()
}

func entriesForUnifiedConfigExampleTier(tier unifiedConfigExampleTier, validationSample bool) []unifiedConfigExampleEntry {
	var entries []unifiedConfigExampleEntry
	for _, entry := range unifiedConfigExampleEntries() {
		if tier != "" && entry.Tier != tier {
			continue
		}
		if validationSample && !entry.ValidateInSample {
			continue
		}
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Path < entries[j].Path
	})
	return entries
}

func unifiedConfigExampleTierTitle(tier unifiedConfigExampleTier) string {
	switch tier {
	case unifiedConfigExampleTierProjectSafe:
		return "Project-safe keys: safe in checked-in ./swarm.yaml"
	case unifiedConfigExampleTierProjectContainedPath:
		return "Project-contained paths: relative paths under the project when used in ./swarm.yaml"
	case unifiedConfigExampleTierFullTrust:
		return "Connection and auth settings: use user-global config, --config, or flags"
	case unifiedConfigExampleTierElevated:
		return "Settings for this machine only: put these in .swarm/swarm.yaml"
	case unifiedConfigExampleTierSecretReference:
		return "Secret references: never store plaintext secrets in config"
	default:
		return "Config keys"
	}
}

func unifiedConfigExampleTierDescription(tier unifiedConfigExampleTier) string {
	switch tier {
	case unifiedConfigExampleTierProjectSafe:
		return "These keys express portable runtime behavior."
	case unifiedConfigExampleTierProjectContainedPath:
		return "Project config may use these only as relative paths that stay inside the project root."
	case unifiedConfigExampleTierFullTrust:
		return "These keys can redirect clients or name auth material, so keep them in user-global config, an explicit --config file, or invocation flags."
	case unifiedConfigExampleTierElevated:
		return "These keys are machine-specific or unsafe if committed to a shared project file."
	case unifiedConfigExampleTierSecretReference:
		return "These keys point at secret material; the secret value itself belongs in the secrets store, a token file, a mounted secret file, or an explicitly delegated env var."
	default:
		return ""
	}
}

type unifiedConfigExampleNode struct {
	Children    map[string]*unifiedConfigExampleNode
	Value       string
	Description string
}

func writeCommentedYAMLTree(b *strings.Builder, entries []unifiedConfigExampleEntry) {
	var raw strings.Builder
	writeActiveYAMLTree(&raw, entries)
	for _, line := range strings.Split(strings.TrimRight(raw.String(), "\n"), "\n") {
		if strings.TrimSpace(line) == "" {
			b.WriteString("#\n")
			continue
		}
		b.WriteString("# ")
		b.WriteString(line)
		b.WriteString("\n")
	}
}

func writeActiveYAMLTree(b *strings.Builder, entries []unifiedConfigExampleEntry) {
	root := &unifiedConfigExampleNode{Children: map[string]*unifiedConfigExampleNode{}}
	for _, entry := range entries {
		insertUnifiedConfigExampleEntry(root, entry)
	}
	writeUnifiedConfigExampleNode(b, root, 0)
}

func insertUnifiedConfigExampleEntry(root *unifiedConfigExampleNode, entry unifiedConfigExampleEntry) {
	node := root
	for _, part := range strings.Split(entry.Path, ".") {
		if node.Children == nil {
			node.Children = map[string]*unifiedConfigExampleNode{}
		}
		if node.Children[part] == nil {
			node.Children[part] = &unifiedConfigExampleNode{Children: map[string]*unifiedConfigExampleNode{}}
		}
		node = node.Children[part]
	}
	node.Value = entry.Value
	node.Description = entry.Description
}

func writeUnifiedConfigExampleNode(b *strings.Builder, node *unifiedConfigExampleNode, indent int) {
	keys := make([]string, 0, len(node.Children))
	for key := range node.Children {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		child := node.Children[key]
		prefix := strings.Repeat(" ", indent)
		if len(child.Children) == 0 {
			fmt.Fprintf(b, "%s%s: %s", prefix, key, child.Value)
			if child.Description != "" {
				fmt.Fprintf(b, " # %s", child.Description)
			}
			b.WriteString("\n")
			continue
		}
		fmt.Fprintf(b, "%s%s:\n", prefix, key)
		writeUnifiedConfigExampleNode(b, child, indent+2)
	}
}
