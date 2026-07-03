package main

import (
	"strings"

	"github.com/spf13/cobra"
)

func bindCLIAPIConnectionFlags(cmd *cobra.Command, opts *rootCommandOptions) {
	bindCLIAPIConnectionFlagsWithClass(cmd, opts, cliAPICommandClassReadOnly, "")
}

func bindCLIAPIConnectionFlagsWithClass(cmd *cobra.Command, opts *rootCommandOptions, class cliAPICommandClass, commandName string) {
	if opts != nil {
		opts.apiCommandClass = class
		opts.apiCommandName = strings.TrimSpace(commandName)
	}
	cmd.Flags().StringVar(&opts.apiServer, "api-server", "", "Swarm API server base URL")
	cmd.Flags().StringVar(&opts.apiTokenFile, "api-token-file", "", "Path to file containing the Swarm API bearer token")
	cmd.Flags().StringVar(&opts.contextName, "context", "", "Local Swarm context descriptor name")
}

func cliAPIConnectionFlagsChanged(cmd *cobra.Command) bool {
	return cmd.Flags().Changed("api-server") || cmd.Flags().Changed("api-token-file") || cmd.Flags().Changed("context")
}

func validateCLIAPIConnectionFlagPlacement(args []string) error {
	index, flag := firstCLIAPIConnectionFlagIndex(args)
	if index < 0 || cliAPIConnectionFlagAfterLeafCommand(args[:index]) {
		return nil
	}
	return &cliAPIValidationError{message: "unknown flag: " + flag}
}

func firstCLIAPIConnectionFlagIndex(args []string) (int, string) {
	for i, arg := range args {
		switch {
		case arg == "--api-server" || arg == "--api-token-file":
			return i, arg
		case arg == "--context":
			return i, arg
		case strings.HasPrefix(arg, "--api-server="):
			return i, "--api-server"
		case strings.HasPrefix(arg, "--api-token-file="):
			return i, "--api-token-file"
		case strings.HasPrefix(arg, "--context="):
			return i, "--context"
		}
	}
	return -1, ""
}

func cliAPIConnectionFlagAfterLeafCommand(prefix []string) bool {
	prefix = stripRootPersistentFlagsForCLIAPIPlacement(prefix)
	leafCommands := [][]string{
		{"runs"},
		{"test"},
		{"status"},
		{"trace"},
		{"health"},
		{"logs"},
		{"incidents"},
		{"version"},
		{"events", "list"},
		{"events", "follow"},
		{"event", "view"},
		{"event", "publish"},
		{"event", "replay"},
		{"bundle", "list"},
		{"bundle", "show"},
		{"bundle", "agents"},
		{"bundle", "register"},
		{"bundle", "delete"},
		{"agents", "list"},
		{"agent", "deliveries"},
		{"agent", "diagnose"},
		{"agent", "view"},
		{"agent", "restart"},
		{"agent", "replay"},
		{"agent", "replay-backlog"},
		{"agent", "directive"},
		{"conversations", "list"},
		{"conversation", "view"},
		{"conversation", "turn"},
		{"entities", "list"},
		{"entity", "view"},
		{"entity", "aggregate"},
		{"mailbox", "list"},
		{"mailbox", "view"},
		{"mailbox", "approve"},
		{"mailbox", "reject"},
		{"mailbox", "defer"},
		{"control", "pause"},
		{"control", "continue"},
		{"control", "stop"},
		{"control", "nuke"},
		{"fork"},
		{"forkchat", "new"},
		{"forkchat", "resume"},
		{"forkchat", "list"},
		{"forkchat", "view"},
		{"forkchat", "delete"},
		{"doctor"},
		{"serve"},
	}
	for _, command := range leafCommands {
		if argsHaveCommandPrefix(prefix, command) {
			return true
		}
	}
	return false
}

func stripRootPersistentFlagsForCLIAPIPlacement(args []string) []string {
	if len(args) == 0 {
		return args
	}
	stripped := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "--swarm-dir" && i+1 < len(args):
			i++
			continue
		case strings.HasPrefix(arg, "--swarm-dir="):
			continue
		default:
			stripped = append(stripped, arg)
		}
	}
	return stripped
}

func argsHaveCommandPrefix(args, command []string) bool {
	if len(args) < len(command) {
		return false
	}
	for i, want := range command {
		if args[i] != want {
			return false
		}
	}
	return true
}
