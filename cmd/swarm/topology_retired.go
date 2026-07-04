package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

// Retired CLI v2.2 spellings. Dispositions, exit code, and exact messages are
// promoted in platform-spec.yaml#cli_specification.topology_revision_v2_2
// .superseded_spellings — fail closed, zero API calls, no aliases.
func newRetiredTopologySpellingCommands() []*cobra.Command {
	retired := []struct {
		use     string
		short   string
		message string
	}{
		{"runs", "Retired spelling; use swarm run list.",
			"ERROR: `swarm runs` was renamed. Use `swarm run list`."},
		{"status", "Retired spelling; use swarm run status.",
			"ERROR: `swarm status` was renamed. Use `swarm run status`."},
		{"trace", "Retired spelling; use swarm run trace.",
			"ERROR: `swarm trace` was renamed. Use `swarm run trace`."},
		{"fork", "Retired spelling; use swarm run fork.",
			"ERROR: `swarm fork` was renamed. Use `swarm run fork`."},
		{"agents", "Retired spelling; use swarm agent list.",
			"ERROR: `swarm agents` was renamed. Use `swarm agent list`."},
		{"events", "Retired spelling; use swarm event list or swarm event follow.",
			"ERROR: `swarm events` was renamed. Use `swarm event list` or `swarm event follow`."},
		{"entities", "Retired spelling; use swarm entity list.",
			"ERROR: `swarm entities` was renamed. Use `swarm entity list`."},
		{"conversations", "Retired spelling; use swarm conversation list.",
			"ERROR: `swarm conversations` was renamed. Use `swarm conversation list`."},
	}
	commands := make([]*cobra.Command, 0, len(retired))
	for _, entry := range retired {
		message := entry.message
		commands = append(commands, &cobra.Command{
			Use:                entry.use,
			Short:              entry.short,
			Hidden:             true,
			DisableFlagParsing: true,
			RunE: func(cmd *cobra.Command, args []string) error {
				fmt.Fprintln(cmd.ErrOrStderr(), message)
				return commandExitError{code: 2}
			},
		})
	}
	return commands
}

// runStartRetiredMessage is the promoted disposition for the bare `swarm run`
// start form (superseded_spellings.run_bare_start).
const runStartRetiredMessage = "ERROR: `swarm run` no longer starts a run. Use `swarm run start ...`. Bare `swarm run` prints the run command group."

// cliTopologyRetiredOrGroupPrefix reports whether pre-dispatch flag-placement
// validation should defer to cobra dispatch so retired v2.2 spellings and the
// bare run noun-group fail closed with their promoted pointer messages
// instead of a generic flag-placement error.
func cliTopologyRetiredOrGroupPrefix(prefix []string) bool {
	if len(prefix) == 0 {
		return false
	}
	switch prefix[0] {
	case "runs", "status", "trace", "fork", "agents", "events", "entities", "conversations":
		return true
	case "run":
		return len(prefix) == 1 // bare noun-group; deeper paths use leaf eligibility
	}
	return false
}
