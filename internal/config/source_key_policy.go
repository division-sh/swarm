package config

// SourceKeyRule describes the existing swarm.yaml source policy.
type SourceKeyRule struct {
	Container            bool
	Elevated             bool
	SecretReference      bool
	InlineSecret         bool
	ProjectContainedPath bool
	Split                string
}

func (r SourceKeyRule) Supported() bool {
	return r.Split == "" && !r.InlineSecret
}

func (r SourceKeyRule) SupportedExampleLeaf() bool {
	return !r.Container && r.Supported()
}

func SourceKeyRules() map[string]SourceKeyRule {
	section := SourceKeyRule{Container: true}
	elevatedSection := SourceKeyRule{Container: true, Elevated: true}
	return map[string]SourceKeyRule{
		"connection":                              elevatedSection,
		"connection.api_server":                   {Elevated: true},
		"connection.api_token_file":               {Elevated: true, SecretReference: true},
		"serve":                                   section,
		"serve.api_listen_addr":                   {},
		"serve.mcp_listen_addr":                   {},
		"serve.api_token_file":                    {Elevated: true, SecretReference: true},
		"runtime":                                 section,
		"runtime.recovery_on_startup":             {},
		"runtime.fan_out_workers":                 {},
		"runtime.max_concurrent_agents":           {Split: "tracked split: runtime.max_concurrent_agents is not wired to runtime enforcement; no supported replacement"},
		"runtime.event_poll_interval":             {Split: "tracked split: runtime.event_poll_interval is not wired to runtime polling; no supported replacement"},
		"runtime.decision_card_first_reminder":    {},
		"runtime.decision_card_urgency":           {},
		"runtime.decision_card_reminder_interval": {},
		"runtime.decision_card_input_draft_ttl":   {},
		"store":                                   elevatedSection,
		"store.backend":                           {Elevated: true},
		"store.sqlite":                            elevatedSection,
		"store.sqlite.path":                       {Elevated: true, ProjectContainedPath: true},
		"database":                                elevatedSection,
		"database.host":                           {Elevated: true},
		"database.port":                           {Elevated: true},
		"database.name":                           {Elevated: true},
		"database.user":                           {Elevated: true},
		"database.password":                       {Elevated: true, InlineSecret: true},
		"database.password_secret_key":            {Elevated: true, SecretReference: true},
		"database.password_file":                  {Elevated: true, SecretReference: true},
		"database.password_env":                   {Elevated: true, SecretReference: true},
		"database.sslmode":                        {Elevated: true},
		"database.pool_size":                      {Elevated: true},
		"workspace":                               section,
		"workspace.backend":                       {},
		"workspace.allow_exec_on_host":            {Elevated: true},
		"workspace.image":                         {Elevated: true},
		"workspace.docker_bin":                    {Elevated: true},
		"workspace.host_root":                     {Elevated: true},
		"workspace.network":                       {Elevated: true},
		"llm":                                     section,
		"llm.backend":                             {},
		"llm.models":                              section,
		"llm.session":                             section,
		"llm.session.lock_ttl":                    {},
		"llm.session.rotate_after_turns":          {},
		"llm.session.rotate_on_parse_failures":    {},
		"llm.provider_limits":                     section,
		"llm.claude_cli":                          section,
		"llm.claude_cli.command":                  {Elevated: true},
		"llm.claude_cli.timeout":                  {},
		"llm.claude_cli.output_format":            {},
		"llm.claude_cli.retries":                  {Split: "tracked split: llm.claude_cli.retries remains unsupported/inert until #1803 promotes a production runtime owner; no supported replacement"},
		"llm.claude_cli.no_session_persistence":   {Split: "tracked split: llm.claude_cli.no_session_persistence remains unsupported/inert until #1803 promotes a production runtime owner; no supported replacement"},
		"llm.claude_cli.use_tmux":                 {Split: "tracked split: llm.claude_cli.use_tmux remains unsupported/inert until #1803 promotes a production runtime owner; no supported replacement"},
		"llm.openai_compatible":                   section,
		"llm.openai_compatible.base_url":          {Elevated: true},
		"llm.openai_responses":                    section,
		"llm.openai_responses.base_url":           {Elevated: true},
		"platform":                                section,
		"platform.packs":                          section,
		"platform.packs.platform_dirs":            {Elevated: true},
		"channels":                                section,
		"channels.bindings":                       elevatedSection,
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
		"paths.swarm_dir":                         {Elevated: true},
		"paths.platform_spec_path":                {ProjectContainedPath: true},
		"paths.monitor_dir":                       {Elevated: true},
		"paths.agent_config_map_file":             {ProjectContainedPath: true},
		"paths.verification_gates_file":           {ProjectContainedPath: true},
		"paths.tooling_lock_file":                 {ProjectContainedPath: true},
	}
}
