package main

import (
	"flag"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

const (
	cliLogLevelFlag     = "log-level"
	cliLogLevelFlagHelp = "Set CLI diagnostic logging level: debug, info, warn, error"
	cliLogLevelDefault  = "info"
)

type cliLogLevel string

const (
	cliLogLevelDebug cliLogLevel = "debug"
	cliLogLevelInfo  cliLogLevel = "info"
	cliLogLevelWarn  cliLogLevel = "warn"
	cliLogLevelError cliLogLevel = "error"
)

type cliLoggingOptions struct {
	level string
}

type cliLoggingPolicy struct {
	level cliLogLevel
}

func defaultCLILoggingOptions() cliLoggingOptions {
	return cliLoggingOptions{level: cliLogLevelDefault}
}

func bindCLILoggingFlags(cmd *cobra.Command, opts *cliLoggingOptions) {
	*opts = defaultCLILoggingOptions()
	cmd.Flags().StringVar(&opts.level, cliLogLevelFlag, opts.level, cliLogLevelFlagHelp)
}

func bindCLILoggingFlagSet(fs *flag.FlagSet, opts *cliLoggingOptions) {
	*opts = defaultCLILoggingOptions()
	fs.StringVar(&opts.level, cliLogLevelFlag, opts.level, cliLogLevelFlagHelp)
}

func validateCLILoggingFlagPlacement(args []string) error {
	index, flag := firstCLILoggingFlagIndex(args)
	if index < 0 || cliLoggingFlagAfterSupportedLeafCommand(args[:index]) || cliLoggingFlagUnderRetiredNamespace(args[:index]) {
		return nil
	}
	return fmt.Errorf("unknown flag: %s", flag)
}

func firstCLILoggingFlagIndex(args []string) (int, string) {
	for i, arg := range args {
		switch {
		case arg == "--":
			return -1, ""
		case arg == "--"+cliLogLevelFlag:
			return i, "--" + cliLogLevelFlag
		case strings.HasPrefix(arg, "--"+cliLogLevelFlag+"="):
			return i, "--" + cliLogLevelFlag
		}
	}
	return -1, ""
}

func cliLoggingFlagAfterSupportedLeafCommand(prefix []string) bool {
	leafCommands := [][]string{
		{"version"},
		{"verify"},
		{"runs"},
		{"health"},
		{"status"},
		{"conversations", "list"},
		{"conversation", "view"},
		{"conversation", "turn"},
	}
	for _, command := range leafCommands {
		if argsHaveCommandPrefix(prefix, command) {
			return true
		}
	}
	return false
}

func cliLoggingFlagUnderRetiredNamespace(prefix []string) bool {
	return len(prefix) > 0 && prefix[0] == "investigate"
}

func (opts cliLoggingOptions) validate() error {
	_, err := opts.resolve()
	return err
}

func (opts cliLoggingOptions) resolve() (cliLoggingPolicy, error) {
	level, err := parseCLILogLevel(opts.level)
	if err != nil {
		return cliLoggingPolicy{}, err
	}
	return cliLoggingPolicy{level: level}, nil
}

func parseCLILogLevel(raw string) (cliLogLevel, error) {
	switch cliLogLevel(raw) {
	case cliLogLevelDebug:
		return cliLogLevelDebug, nil
	case cliLogLevelInfo:
		return cliLogLevelInfo, nil
	case cliLogLevelWarn:
		return cliLogLevelWarn, nil
	case cliLogLevelError:
		return cliLogLevelError, nil
	default:
		return "", fmt.Errorf("--log-level must be one of debug, info, warn, error")
	}
}
