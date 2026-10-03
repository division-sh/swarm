package cliapp

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

// This negative proof ledger is not a source registry. None of these spellings
// may return to the production accepted set or acquire an implicit reader.
func TestRemovedEnvRowsUseCurrentUnknownAuthorityBeforeEffects(t *testing.T) {
	for _, name := range []string{
		"SWARM_AGENT_CONFIG_MAP_FILE",
		"SWARM_API_LISTEN_ADDR",
		"SWARM_API_PORT",
		"SWARM_API_SERVER",
		"SWARM_API_TOKEN",
		"SWARM_API_TOKEN_FILE",
		"SWARM_BUILDER_AUTH_TOKEN",
		"SWARM_CLAUDE_API_MAX_RETRIES",
		"SWARM_CLAUDE_API_RETRY_BACKOFF",
		"SWARM_CLAUDE_CLI_COMMAND",
		"SWARM_CLAUDE_CLI_NO_SESSION_PERSISTENCE",
		"SWARM_CLAUDE_CLI_OUTPUT_FORMAT",
		"SWARM_CLAUDE_CLI_RETRIES",
		"SWARM_CLAUDE_CLI_TIMEOUT",
		"SWARM_CLAUDE_CLI_USE_TMUX",
		"SWARM_CLAUDE_DEFAULT_MODEL",
		"SWARM_CLAUDE_HAIKU_MODEL",
		"SWARM_CLAUDE_TIMEOUT_SECONDS",
		"SWARM_CONTRACTS_DIR",
		"SWARM_CONTRACTS_PATH",
		"SWARM_DB_HOST",
		"SWARM_DB_NAME",
		"SWARM_DB_PASSWORD",
		"SWARM_DB_POOL_SIZE",
		"SWARM_DB_PORT",
		"SWARM_DB_SSLMODE",
		"SWARM_DB_USER",
		"SWARM_DIR",
		"SWARM_DOCKER_BIN",
		"SWARM_ENTITY_CONTAINER_PREFIX",
		"SWARM_ENTITY_WORKDIR",
		"SWARM_HOME",
		"SWARM_LLM_BACKEND",
		"SWARM_LLM_RUNTIME_MODE",
		"SWARM_LLM_SESSION_LOCK_TTL",
		"SWARM_LLM_SESSION_ROTATE_AFTER_TURNS",
		"SWARM_LLM_SESSION_ROTATE_ON_PARSE_FAILURES",
		"SWARM_LOG_LEVEL",
		"SWARM_MCP_LISTEN_ADDR",
		"SWARM_MCP_PORT",
		"SWARM_OPENAI_COMPATIBLE_BASE_URL",
		"SWARM_OPENAI_COMPATIBLE_DEFAULT_MODEL",
		"SWARM_OPENAI_COMPATIBLE_LOW_COST_MODEL",
		"SWARM_OPERATOR_AUTH_TOKEN",
		"SWARM_PLATFORM_SPEC_PATH",
		"SWARM_RUNTIME_EVENT_POLL_INTERVAL",
		"SWARM_RUNTIME_MAX_CONCURRENT_AGENTS",
		"SWARM_RUNTIME_RECOVERY_ON_STARTUP",
		"SWARM_SCAFFOLD_CONTAINER",
		"SWARM_SCAFFOLD_VOLUME",
		"SWARM_SCAFFOLD_WORKDIR",
		"SWARM_SQLITE_PATH",
		"SWARM_STORE_BACKEND",
		"SWARM_SYSTEM_CONTAINER",
		"SWARM_SYSTEM_ENTITIES_VOLUME",
		"SWARM_SYSTEM_NGINX_VOLUME",
		"SWARM_SYSTEM_SYSTEMD_VOLUME",
		"SWARM_SYSTEM_WORKDIR",
		"SWARM_TOOLING_LOCK_FILE",
		"SWARM_TOOL_GATEWAY_TOKEN",
		"SWARM_VERIFICATION_GATES_FILE",
		"SWARM_WORKSPACE_BACKEND",
		"SWARM_WORKSPACE_CONTRACTS_MOUNT",
		"SWARM_WORKSPACE_CONTRACTS_SOURCE",
		"SWARM_WORKSPACE_DATA_MOUNT",
		"SWARM_WORKSPACE_DATA_SOURCE",
		"SWARM_WORKSPACE_HOST_ROOT",
		"SWARM_WORKSPACE_IMAGE",
		"SWARM_WORKSPACE_NETWORK",
		"SWARM_WORKSPACE_VOLUMES_FROM",
	} {
		t.Run(name, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			if _, present := swarmEnvCatalogByName()[name]; present {
				t.Fatalf("obsolete entry returned to accepted set: %s", name)
			}
			root := t.TempDir()
			opts := defaultRootCommandOptions()
			calls := 0
			opts.runServe = func(context.Context, InvocationRoot, ServeOptions) int {
				calls++
				return 0
			}
			for _, value := range []string{"", "dummy"} {
				t.Setenv(name, value)
				var out, diagnostic bytes.Buffer
				code := executeRootCommandWithOptions(context.Background(), root, []string{"serve", root}, &out, &diagnostic, opts)
				if value == "" {
					if code != 0 || calls != 1 {
						t.Fatalf("empty source changed nonempty-env boundary: code=%d calls=%d err=%s", code, calls, diagnostic.String())
					}
					continue
				}
				if code != CLIExitValidation || calls != 1 || !strings.Contains(diagnostic.String(), "env/unknown_stale @ "+name) {
					t.Fatalf("unknown env reached effects: code=%d calls=%d err=%s", code, calls, diagnostic.String())
				}
				if strings.Contains(diagnostic.String(), "known_retired") || strings.Contains(diagnostic.String(), "migrate ") {
					t.Fatalf("old translation table survived: %s", diagnostic.String())
				}
			}
		})
	}
}
