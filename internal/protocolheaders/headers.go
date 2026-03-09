package protocolheaders

const (
	APIKeyHeader = "X-Empire-Key"

	ActorIDHeader      = "X-Empire-Agent-Id"
	ActorRoleHeader    = "X-Empire-Agent-Role"
	ActorModeHeader    = "X-Empire-Agent-Mode"
	VerticalIDHeader   = "X-Empire-Vertical-Id"
	AllowedToolsHeader = "X-Empire-Allowed-Tools"
	ContextTokenHeader = "X-Empire-Context-Token"
	TraceIDHeader      = "X-Empire-Trace-Id"

	ActorIDQuery      = "empire_agent_id"
	ActorRoleQuery    = "empire_agent_role"
	ActorModeQuery    = "empire_agent_mode"
	VerticalIDQuery   = "empire_vertical_id"
	AllowedToolsQuery = "empire_allowed_tools"
	ContextTokenQuery = "empire_ctx_token"
	TraceIDQuery      = "empire_trace_id"
)
