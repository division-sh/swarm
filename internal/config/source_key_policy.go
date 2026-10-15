package config

const PlatformSpecPathConfigRemediation = "the platform spec is embedded in the binary; to run against a different spec, pass it with the command's explicit flag"

// SourceKeyRule describes the existing swarm.yaml source policy.
type SourceKeyRule struct {
	Container       bool
	SecretReference bool
	InlineSecret    bool
	Split           string
}

func (r SourceKeyRule) Supported() bool {
	return r.Split == "" && !r.InlineSecret
}

func (r SourceKeyRule) SupportedExampleLeaf() bool {
	return !r.Container && r.Supported()
}

func SourceKeyRules() map[string]SourceKeyRule {
	section := SourceKeyRule{Container: true}
	return map[string]SourceKeyRule{
		"sharding":                                {Split: "tracked split: runtime sharding has no supported production consumer; no supported replacement"},
		"connection":                              section,
		"connection.api_server":                   {},
		"connection.api_token_file":               {SecretReference: true},
		"serve":                                   section,
		"serve.api_listen_addr":                   {},
		"serve.mcp_listen_addr":                   {},
		"serve.api_token_file":                    {SecretReference: true},
		"runtime":                                 section,
		"runtime.recovery_on_startup":             {},
		"runtime.fan_out_workers":                 {},
		"runtime.max_concurrent_agents":           {Split: "tracked split: runtime.max_concurrent_agents is not wired to runtime enforcement; no supported replacement"},
		"runtime.event_poll_interval":             {Split: "tracked split: runtime.event_poll_interval is not wired to runtime polling; no supported replacement"},
		"runtime.decision_card_first_reminder":    {},
		"runtime.decision_card_urgency":           {},
		"runtime.decision_card_reminder_interval": {},
		"runtime.decision_card_input_draft_ttl":   {},
		"store":                                   section,
		"store.backend":                           {},
		"store.sqlite":                            section,
		"store.sqlite.path":                       {},
		"database":                                section,
		"database.host":                           {},
		"database.port":                           {},
		"database.name":                           {},
		"database.user":                           {},
		"database.password":                       {InlineSecret: true},
		"database.password_secret_key":            {SecretReference: true},
		"database.password_file":                  {SecretReference: true},
		"database.password_env":                   {SecretReference: true},
		"database.sslmode":                        {},
		"database.pool_size":                      {},
		"workspace":                               section,
		"workspace.backend":                       {},
		"workspace.allow_exec_on_host":            {},
		"workspace.image":                         {},
		"workspace.docker_bin":                    {},
		"workspace.host_root":                     {},
		"workspace.network":                       {},
		"llm":                                     section,
		"llm.backend":                             {},
		"llm.models":                              section,
		"llm.session":                             section,
		"llm.session.lock_ttl":                    {},
		"llm.session.rotate_after_turns":          {},
		"llm.session.rotate_on_parse_failures":    {},
		"llm.provider_limits":                     section,
		"llm.claude_cli":                          section,
		"llm.claude_cli.command":                  {},
		"llm.claude_cli.timeout":                  {},
		"llm.claude_cli.output_format":            {},
		"llm.claude_cli.retries":                  {Split: "tracked split: llm.claude_cli.retries remains unsupported/inert until #1803 promotes a production runtime owner; no supported replacement"},
		"llm.claude_cli.no_session_persistence":   {Split: "tracked split: llm.claude_cli.no_session_persistence remains unsupported/inert until #1803 promotes a production runtime owner; no supported replacement"},
		"llm.claude_cli.use_tmux":                 {Split: "tracked split: llm.claude_cli.use_tmux remains unsupported/inert until #1803 promotes a production runtime owner; no supported replacement"},
		"llm.openai_compatible":                   section,
		"llm.openai_compatible.base_url":          {},
		"llm.openai_responses":                    section,
		"llm.openai_responses.base_url":           {},
		"platform":                                section,
		"platform.packs":                          section,
		"platform.packs.platform_dirs":            {},
		"channels":                                section,
		"channels.bindings":                       section,
		"budget":                                  section,
		"budget.global_monthly_cap":               {},
		"budget.per_entity_monthly_cap":           {},
		"budget.system_monthly_cap":               {},
		"budget.human_tasks":                      section,
		"budget.human_tasks.max_tasks_per_week":   {},
		"budget.human_tasks.budget_reset":         {},
		"budget.human_tasks.auto_expire_hours":    {},
		"budget.human_tasks.categories_enabled":   {},
		"paths":                                   section,
		"paths.swarm_dir":                         {},
		"paths.monitor_dir":                       {},
	}
}
