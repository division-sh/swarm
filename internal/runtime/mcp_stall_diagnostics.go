package runtime

import (
	"context"
	"database/sql"
	"sync"

	runtimemcp "empireai/internal/runtime/mcp"
)

type MCPStallDiagnosticConfig = runtimemcp.StallDiagnosticConfig

var mcpStallDiagSeen sync.Map

func DefaultMCPStallDiagnosticConfig() MCPStallDiagnosticConfig {
	return runtimemcp.DefaultStallDiagnosticConfig()
}

func StartMCPStallDiagnosticLoop(ctx context.Context, db *sql.DB, logger *RuntimeLogger, cfg MCPStallDiagnosticConfig) {
	if logger == nil {
		return
	}
	runtimemcp.StartStallDiagnosticLoop(ctx, db, runtimeMCPDiagnosticLogger(logger), cfg)
}

func runMCPStallDiagnosticsPass(ctx context.Context, db *sql.DB, logger *RuntimeLogger, cfg MCPStallDiagnosticConfig) {
	runtimemcp.ResetStallDiagnosticsForTest()
	mcpStallDiagSeen = sync.Map{}
	if logger == nil {
		return
	}
	runtimemcp.RunStallDiagnosticsPass(ctx, db, runtimeMCPDiagnosticLogger(logger), cfg)
}

func classifyMCPStallCause(code string) string {
	return runtimemcp.ClassifyStallCause(code)
}

func runtimeMCPDiagnosticLogger(logger *RuntimeLogger) runtimemcp.StallDiagnosticLogger {
	return func(ctx context.Context, level, component, action, agentID, verticalID string, detail map[string]any, errText string) {
		logger.Log(ctx, RuntimeLogEntry{
			Level:      level,
			Component:  component,
			Action:     action,
			AgentID:    agentID,
			VerticalID: verticalID,
			Detail:     detail,
			Error:      errText,
		})
	}
}
