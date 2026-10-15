package cliapp

import (
	"fmt"
	"sort"
	"strings"
)

type unifiedConfigExampleEntry struct {
	Path               string
	Value              string
	Description        string
	ValidateInSample   bool
	RequiresRuleLookup bool
}

func unifiedConfigExampleEntries() []unifiedConfigExampleEntry {
	e := func(path, value, description string) unifiedConfigExampleEntry {
		return unifiedConfigExampleEntry{Path: path, Value: value, Description: description, ValidateInSample: true, RequiresRuleLookup: true}
	}
	skippedValidation := func(path, value, description string) unifiedConfigExampleEntry {
		entry := e(path, value, description)
		entry.ValidateInSample = false
		return entry
	}
	return []unifiedConfigExampleEntry{
		e("runtime.recovery_on_startup", "true", "Recover retained runtime state on startup; explicit false disables replay."),
		e("runtime.fan_out_workers", "1", "Optional process-wide fan-out workers; defaults PostgreSQL 4 / SQLite 1. SQLite requires 1; PostgreSQL requires pool headroom."),
		e("runtime.decision_card_first_reminder", "4h", "Delay before the first pending decision-card reminder."),
		e("runtime.decision_card_urgency", "24h", "Delay before a pending decision card becomes urgent."),
		e("runtime.decision_card_reminder_interval", "24h", "Cadence for later pending decision-card reminders."),
		e("runtime.decision_card_input_draft_ttl", "15m", "Lifetime of an incomplete actor-scoped decision input draft."),
		e("serve.api_listen_addr", `"127.0.0.1:8081"`, "API listener address; defaults to loopback."),
		e("serve.mcp_listen_addr", `"127.0.0.1:8082"`, "MCP listener address; defaults to loopback."),
		e("workspace.backend", "docker", "Workspace backend preference; actual isolation still follows capability policy."),
		e("llm.backend", "anthropic", "Active LLM backend profile."),
		e("llm.models.regular.anthropic", "claude-sonnet-4-5", "Anthropic model to use for the regular alias."),
		e("llm.session.lock_ttl", "10s", "How long a session lock can be held before it expires."),
		e("llm.session.rotate_after_turns", "40", "Rotate an LLM session after this many turns."),
		e("llm.session.rotate_on_parse_failures", "3", "Rotate an LLM session after repeated parse failures."),
		e("llm.provider_limits.anthropic.max_concurrency", "2", "Profile-wide concurrency limit."),
		e("llm.claude_cli.timeout", "1h", "Claude CLI request timeout."),
		e("llm.claude_cli.output_format", "stream-json", "Claude CLI output format."),
		e("budget.global_monthly_cap", "0", "Global monthly budget cap; zero leaves enforcement disabled."),
		e("budget.per_entity_monthly_cap", "0", "Per-entity monthly budget cap; zero leaves enforcement disabled."),
		e("budget.system_monthly_cap", "0", "System actor monthly budget cap; zero leaves enforcement disabled."),
		e("budget.human_tasks.max_tasks_per_week", "0", "Human task weekly cap; zero leaves enforcement disabled."),
		e("budget.human_tasks.budget_reset", "monday", "Weekly human-task budget reset day."),
		e("budget.human_tasks.auto_expire_hours", "168", "Human task auto-expiration window in hours."),
		e("budget.human_tasks.categories_enabled", `["ops"]`, "Human task categories covered by the budget."),

		e("connection.api_server", "http://127.0.0.1:8081", "CLI API target for this deployment."),
		e("connection.api_token_file", ".swarm/api-token", "CLI API bearer-token file reference; never inline the token."),
		e("serve.api_token_file", ".swarm/serve-api-token", "Serve API bearer-token file reference; never inline the token."),

		e("store.backend", "sqlite", "Runtime store backend."),
		e("store.sqlite.path", ".swarm/stores/dev.db", "SQLite runtime store path."),
		e("database.host", "127.0.0.1", "Postgres host for postgres store deployments."),
		e("database.port", "5432", "Postgres port for postgres store deployments."),
		e("database.name", "swarm", "Postgres database name."),
		e("database.user", "postgres", "Postgres user."),
		e("database.sslmode", "disable", "Postgres SSL mode."),
		e("database.pool_size", "5", "Postgres connection pool size."),
		e("workspace.allow_exec_on_host", "false", "Unsafe host execution opt-in; disabled by default."),
		e("workspace.image", "swarm-workspace:latest", "Workspace image for Docker-backed execution."),
		e("workspace.docker_bin", "docker", "Docker executable path/name."),
		e("workspace.host_root", "~/.swarm/workspaces", "Host workspace root."),
		e("workspace.network", "swarm", "Docker network for workspaces."),
		e("llm.claude_cli.command", "claude", "Claude CLI executable path/name."),
		e("llm.openai_compatible.base_url", "https://api.example.com/v1", "OpenAI-compatible endpoint base URL."),
		e("llm.openai_responses.base_url", "https://api.openai.com/v1", "OpenAI Responses endpoint base URL."),
		e("platform.packs.platform_dirs", `["/path/to/complete/dev-pack/provider.telegram.trigger", "/path/to/complete/dev-pack/provider.telegram.connector", "/path/to/complete/dev-pack/provider.telegram.channel"]`, "Complete replacement for the embedded platform pack inventory during platform development."),
		e("channels.bindings.hitl.pack", "provider.telegram.hitl_channel", "Channel pack selected for the HITL outbound binding."),
		e("channels.bindings.hitl.destination", `"-1001234567890"`, "Deployment-owned opaque destination for the HITL outbound binding."),
		e("paths.swarm_dir", ".swarm", "Swarm state directory."),
		e("paths.monitor_dir", ".swarm/monitor", "Runtime monitor artifact directory."),

		e("database.password_secret_key", "postgres_password", "Database password key in the Swarm secrets store."),
		skippedValidation("database.password_file", "/run/secrets/postgres-password", "Database password file for orchestrator-mounted secrets."),
		skippedValidation("database.password_env", "DB_PASSWORD", "Explicit env delegation for orchestrator-injected database password."),
	}
}

func generatedUnifiedConfigExample() string {
	var b strings.Builder
	b.WriteString("# swarm.yaml - Swarm's config file; use .swarm/swarm.yaml or --config <file> (SWARM_CONFIG selects the same file).\n")
	b.WriteString("# No other config files are read. Uncomment needed keys; omitted keys use defaults. Run `swarm doctor --target`.\n")
	b.WriteString("# Inline secret values are absent; use secret keys, token files, secret files, or explicit env delegation.\n")
	b.WriteString("# Contributors: generated file - edit the generator metadata, not this file; see drift test.\n\n")

	var deployment, secrets []unifiedConfigExampleEntry
	for _, entry := range sortedUnifiedConfigExampleEntries(false) {
		rule, _ := unifiedConfigRule(strings.Split(entry.Path, "."))
		if rule.SecretReference {
			secrets = append(secrets, entry)
		} else {
			deployment = append(deployment, entry)
		}
	}
	b.WriteString("# Deployment keys\n")
	writeCommentedYAMLTree(&b, deployment)
	b.WriteString("\n# Secret references: never store plaintext secrets in config\n")
	writeCommentedYAMLTree(&b, secrets)
	return b.String()
}

func generatedUnifiedConfigValidationSample() string {
	var b strings.Builder
	writeActiveYAMLTree(&b, sortedUnifiedConfigExampleEntries(true))
	return b.String()
}

func sortedUnifiedConfigExampleEntries(validationSample bool) []unifiedConfigExampleEntry {
	var entries []unifiedConfigExampleEntry
	for _, entry := range unifiedConfigExampleEntries() {
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
