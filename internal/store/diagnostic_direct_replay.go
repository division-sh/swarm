package store

import (
	"fmt"
	"strings"
)

// diagnosticDirectReplayEventNames mirrors the platform-spec
// diagnostic_direct_persisted catalog entries. These rows are persisted for
// queryability and causality, not executable pipeline replay.
func diagnosticDirectReplayEventNames() []string {
	return []string{
		"platform.runtime_log",
		"platform.agent_directive",
		"platform.inbound_recorded",
	}
}

func diagnosticDirectReplayEventArgs() []any {
	names := diagnosticDirectReplayEventNames()
	args := make([]any, 0, len(names))
	for _, name := range names {
		args = append(args, name)
	}
	return args
}

func sqliteDiagnosticDirectReplayExclusionSQL(alias string) string {
	return diagnosticDirectReplayColumn(alias) + " NOT IN (" + sqlitePlaceholders(len(diagnosticDirectReplayEventNames())) + ")"
}

func postgresDiagnosticDirectReplayExclusionSQL(alias string, startPlaceholder int) string {
	names := diagnosticDirectReplayEventNames()
	placeholders := make([]string, 0, len(names))
	for i := range names {
		placeholders = append(placeholders, fmt.Sprintf("$%d", startPlaceholder+i))
	}
	return diagnosticDirectReplayColumn(alias) + " NOT IN (" + strings.Join(placeholders, ", ") + ")"
}

func diagnosticDirectReplayColumn(alias string) string {
	alias = strings.TrimSpace(alias)
	if alias == "" {
		return "event_name"
	}
	return alias + ".event_name"
}
