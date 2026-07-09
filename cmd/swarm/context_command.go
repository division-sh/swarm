package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"
)

type contextCommandOptions struct {
	asJSON bool
}

func newContextCommand(ctx context.Context, opts rootCommandOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "context",
		Short: "Show which runtime and directories this CLI targets.",
	}
	cmd.AddCommand(
		newContextCurrentCommand(ctx, opts),
		newContextListCommand(ctx, opts),
		newContextPruneCommand(ctx, opts),
	)
	return cmd
}

func newContextCurrentCommand(ctx context.Context, opts rootCommandOptions) *cobra.Command {
	var commandOpts contextCommandOptions
	cmd := &cobra.Command{
		Use:   "current",
		Short: "Show the selected local Swarm context.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			report, err := inspectLocalContextRegistryForCommand(ctx, cmd, opts)
			if err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			if commandOpts.asJSON {
				return writeJSON(cmd.OutOrStdout(), report)
			}
			writeContextCurrentText(cmd.OutOrStdout(), report)
			return nil
		},
	}
	cmd.Flags().BoolVar(&commandOpts.asJSON, "json", false, "Render current context details as JSON")
	return cmd
}

func newContextListCommand(ctx context.Context, opts rootCommandOptions) *cobra.Command {
	var commandOpts contextCommandOptions
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List local Swarm context descriptors.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			report, err := inspectLocalContextRegistryForCommand(ctx, cmd, opts)
			if err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			if commandOpts.asJSON {
				return writeJSON(cmd.OutOrStdout(), report)
			}
			writeContextListText(cmd.OutOrStdout(), report)
			return nil
		},
	}
	cmd.Flags().BoolVar(&commandOpts.asJSON, "json", false, "Render context descriptors as JSON")
	return cmd
}

func newContextPruneCommand(ctx context.Context, opts rootCommandOptions) *cobra.Command {
	var commandOpts contextCommandOptions
	cmd := &cobra.Command{
		Use:   "prune",
		Short: "Remove stale, mismatched, corrupt, or unsupported local Swarm context descriptors.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			swarmDir, err := resolveSwarmDirForCommand(cmd)
			if err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			registry := newLocalContextRegistry(swarmDir.Path)
			result, err := registry.Prune(ctx, cliRuntimeIdentityCaller{httpClient: opts.httpClient})
			if err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			if commandOpts.asJSON {
				return writeJSON(cmd.OutOrStdout(), result)
			}
			writeContextPruneText(cmd.OutOrStdout(), result)
			return nil
		},
	}
	cmd.Flags().BoolVar(&commandOpts.asJSON, "json", false, "Render prune result as JSON")
	return cmd
}

func inspectLocalContextRegistryForCommand(ctx context.Context, cmd *cobra.Command, opts rootCommandOptions) (localContextRegistryReport, error) {
	swarmDir, err := resolveSwarmDirForCommand(cmd)
	if err != nil {
		return localContextRegistryReport{}, err
	}
	registry := newLocalContextRegistry(swarmDir.Path)
	return registry.Inspect(ctx, cliRuntimeIdentityCaller{httpClient: opts.httpClient})
}

func resolveSwarmDirForCommand(cmd *cobra.Command) (cliSwarmDirResolution, error) {
	swarmDirFlag, swarmDirFlagSet := rootSwarmDirFlag(cmd)
	if swarmDirFlagSet {
		path, err := normalizeCLISwarmDir(swarmDirFlag, "--swarm-dir")
		return cliSwarmDirResolution{Path: path, Source: "--swarm-dir"}, err
	}
	configPath, _, err := effectiveCommandConfigPath(cmd, "", false)
	if err != nil {
		return cliSwarmDirResolution{}, err
	}
	cfg, err := loadCLICommandConfigWithOptions(unifiedConfigLoadOptions{ExplicitPath: configPath})
	if err != nil {
		return cliSwarmDirResolution{}, err
	}
	return resolveCLISwarmDirFromConfig(cliSwarmDirOptions{}, cfg)
}

func writeContextCurrentText(out io.Writer, report localContextRegistryReport) {
	if out == nil {
		return
	}
	if report.Current == nil {
		fmt.Fprintf(out, "current context: none\n")
		fmt.Fprintf(out, "registry_status: %s\n", report.Status)
		if report.Detail != "" {
			fmt.Fprintf(out, "detail: %s\n", report.Detail)
		}
		return
	}
	writeContextEntryText(out, "current context", *report.Current)
}

func writeContextListText(out io.Writer, report localContextRegistryReport) {
	if out == nil {
		return
	}
	if len(report.Entries) == 0 {
		writeCLIEmptyState(out, "No contexts found. Create one by running a command with --api or --config.")
		if report.Status != "" && report.Status != "empty" {
			fmt.Fprintf(out, "registry_status: %s\n", report.Status)
			if report.Detail != "" {
				fmt.Fprintf(out, "detail: %s\n", report.Detail)
			}
		}
		return
	}
	rows := make([][]string, 0, len(report.Entries))
	for _, entry := range report.Entries {
		target := contextEntryTarget(entry.Descriptor)
		rows = append(rows, []string{entry.Descriptor.Name, entry.Status, entry.Descriptor.Transport, target})
	}
	writeCLITable(out, cliTable{
		Columns: []cliTableColumn{
			{Header: "NAME", KeyColumn: true, IdentifierFamily: cliIdentifierFamilyContext},
			{Header: "STATUS"},
			{Header: "TRANSPORT"},
			{Header: "TARGET"},
		},
		Rows: rows,
	})
}

func writeContextEntryText(out io.Writer, label string, entry localContextEntry) {
	fmt.Fprintf(out, "%s: %s\n", label, entry.Descriptor.Name)
	fmt.Fprintf(out, "status: %s\n", entry.Status)
	fmt.Fprintf(out, "runtime_instance_id: %s\n", entry.Descriptor.RuntimeInstanceID)
	fmt.Fprintf(out, "transport: %s\n", entry.Descriptor.Transport)
	fmt.Fprintf(out, "target: %s\n", contextEntryTarget(entry.Descriptor))
	if entry.Detail != "" {
		fmt.Fprintf(out, "detail: %s\n", entry.Detail)
	}
}

func writeContextPruneText(out io.Writer, result localContextPruneResult) {
	if out == nil {
		return
	}
	if len(result.Removed) == 0 {
		fmt.Fprintf(out, "removed contexts: 0\n")
		return
	}
	fmt.Fprintf(out, "removed contexts: %d\n", len(result.Removed))
	for _, entry := range result.Removed {
		detail := strings.TrimSpace(entry.Detail)
		if detail != "" {
			detail = " (" + detail + ")"
		}
		fmt.Fprintf(out, "- %s: %s%s\n", entry.Descriptor.Name, entry.Status, detail)
	}
}

func contextEntryTarget(desc localContextDescriptor) string {
	switch desc.Transport {
	case localContextTransportTCP:
		return desc.APIServer
	case localContextTransportUnix:
		return desc.SocketPath
	default:
		return ""
	}
}

func writeJSON(out io.Writer, value any) error {
	if out == nil {
		return nil
	}
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}
