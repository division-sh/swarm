package main

import (
	"strings"

	"github.com/spf13/cobra"
)

func bindCLIAPIConnectionFlags(cmd *cobra.Command, opts *rootCommandOptions) {
	cmd.Flags().StringVar(&opts.apiServer, "api-server", "", "Swarm API server base URL")
	cmd.Flags().StringVar(&opts.apiTokenFile, "api-token-file", "", "Path to file containing the Swarm API bearer token")
}

func cliAPIConnectionFlagsChanged(cmd *cobra.Command) bool {
	return cmd.Flags().Changed("api-server") || cmd.Flags().Changed("api-token-file")
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
		case strings.HasPrefix(arg, "--api-server="):
			return i, "--api-server"
		case strings.HasPrefix(arg, "--api-token-file="):
			return i, "--api-token-file"
		}
	}
	return -1, ""
}

func cliAPIConnectionFlagAfterLeafCommand(prefix []string) bool {
	leafCommands := [][]string{
		{"runs"},
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
		{"agents", "list"},
		{"agent", "view"},
		{"agent", "restart"},
		{"agent", "replay"},
		{"agent", "replay-backlog"},
		{"agent", "directive"},
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
		{"doctor"},
	}
	for _, command := range leafCommands {
		if argsHaveCommandPrefix(prefix, command) {
			return true
		}
	}
	return false
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
