package main

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/authoringview"
	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/spf13/cobra"
)

type describeCommandOptions struct {
	contractsPath    string
	platformSpecPath string
	output           cliOutputOptions
	logging          cliLoggingOptions
}

func defaultDescribeCommandOptions() describeCommandOptions {
	return describeCommandOptions{
		logging: defaultCLILoggingOptions(),
	}
}

func newDescribeCommand(ctx context.Context, repo string) *cobra.Command {
	opts := defaultDescribeCommandOptions()
	cmd := &cobra.Command{
		Use:   "describe",
		Short: "Render the expanded authoring view for local Swarm contracts.",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := opts.logging.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			if err := opts.output.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			if len(args) > 0 {
				return returnCLIValidationError(cmd.ErrOrStderr(), fmt.Errorf("unexpected argument %q", args[0]))
			}
			code := runDescribeCommandWithOutput(ctx, assetCommandRepoRoot(repo), opts, cmd.OutOrStdout(), cmd.ErrOrStderr())
			if code != 0 {
				return commandExitError{code: code}
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&opts.contractsPath, "contracts", opts.contractsPath, "Path to Swarm contract bundle root")
	cmd.Flags().StringVar(&opts.platformSpecPath, "platform-spec", opts.platformSpecPath, "Path to platform spec yaml")
	bindCLIOutputFlags(cmd, &opts.output)
	bindCLILoggingFlags(cmd, &opts.logging)
	return cmd
}

func runDescribeCommandWithOutput(ctx context.Context, repo string, opts describeCommandOptions, out, errOut io.Writer) int {
	if err := opts.logging.validate(); err != nil {
		if errOut != nil {
			fmt.Fprintf(errOut, "describe failed: %v\n", err)
		}
		return 2
	}
	if err := opts.output.validate(); err != nil {
		if errOut != nil {
			fmt.Fprintf(errOut, "describe failed: %v\n", err)
		}
		return 2
	}
	if err := loadRepoDotEnv(repo); err != nil {
		if errOut != nil {
			fmt.Fprintf(errOut, "describe failed: load .env: %v\n", err)
		}
		return 1
	}
	resolvedPaths, err := resolveCLIContractPlatformSpecPaths(repo, cliContractPlatformSpecPathOptions{
		ContractsPath:    opts.contractsPath,
		PlatformSpecPath: opts.platformSpecPath,
	})
	if err != nil {
		if errOut != nil {
			fmt.Fprintf(errOut, "describe failed: resolve path config: %v\n", err)
		}
		return cliAPIErrorExitCode(err, cliAPIErrorClassifier{})
	}
	contractsRoot, err := normalizeContractsRoot(resolvedPaths.ContractsPath)
	if err != nil {
		if errOut != nil {
			fmt.Fprintf(errOut, "describe failed: resolve contracts: %v\n", err)
		}
		return 1
	}
	_, bundle, err := newSwarmWorkflowModule(repo, contractsRoot, resolvedPaths.PlatformSpecPath)
	if err != nil {
		if errOut != nil {
			fmt.Fprintf(errOut, "describe failed: load Swarm contracts: %v\n", err)
		}
		return 1
	}
	source := semanticview.Wrap(bundle)
	report := runtimebootverify.Run(ctx, source, runtimebootverify.Options{})
	view, err := authoringview.Build(ctx, source, authoringview.BuildOptions{BootReport: &report})
	if err != nil {
		if errOut != nil {
			fmt.Fprintf(errOut, "describe failed: %v\n", err)
		}
		return 1
	}
	view.ContractsRoot = contractsRoot
	if err := renderCLIOutput(out, errOut, opts.output, view, func(w io.Writer) {
		writeDescribeText(w, view)
	}, func() ([]string, error) {
		return describeQuietValues(view), nil
	}); err != nil {
		return 2
	}
	return 0
}

func writeDescribeText(out io.Writer, view authoringview.View) {
	if out == nil {
		return
	}
	fmt.Fprintf(out, "describe: contracts=%s\n", view.ContractsRoot)
	fmt.Fprintf(out, "source authority: %s\n", view.SourceAuthority)
	if len(view.Flows) > 0 {
		fmt.Fprintln(out, "flows:")
		for _, flow := range view.Flows {
			label := strings.TrimSpace(flow.ID)
			if flow.Mode != "" {
				label += " (" + flow.Mode + ")"
			}
			fmt.Fprintf(out, "  - %s\n", label)
			if flow.PrimaryEntity != nil {
				fmt.Fprintf(out, "    primary entity: %s\n", flow.PrimaryEntity.Type)
			}
			if flow.TemplateInstance != nil {
				fmt.Fprintf(out, "    instance: by=%s on_missing=%s on_conflict=%s\n", strings.Join(flow.TemplateInstance.By, ","), flow.TemplateInstance.OnMissing, flow.TemplateInstance.OnConflict)
			}
			if flow.SingletonCoordinator != nil {
				fmt.Fprintf(out, "    singleton coordinator: primary_entity=%s contained_fields=%d\n", flow.SingletonCoordinator.PrimaryEntity, len(flow.SingletonCoordinator.ContainedState))
			}
			if len(flow.ContainedOperations) > 0 {
				fmt.Fprintf(out, "    contained operations: %d\n", len(flow.ContainedOperations))
			}
		}
	}
	if len(view.ConnectRoutePlans) > 0 {
		fmt.Fprintln(out, "connect route plans:")
		for _, plan := range view.ConnectRoutePlans {
			fmt.Fprintf(out, "  - %s.%s -> %s.%s resolution=%s delivery=%s\n", plan.Source.FlowID, plan.Source.Pin, plan.Receiver.FlowID, plan.Receiver.Pin, plan.ResolutionKind, plan.Delivery)
		}
	}
	if len(view.Diagnostics) > 0 {
		fmt.Fprintln(out, "diagnostics:")
		for _, diagnostic := range view.Diagnostics {
			fmt.Fprintf(out, "  - [%s] %s %s: %s\n", diagnostic.Severity, diagnostic.CheckID, diagnostic.AuthoredLocation, diagnostic.Message)
		}
	}
}

func describeQuietValues(view authoringview.View) []string {
	values := make([]string, 0, len(view.Flows)+1)
	if strings.TrimSpace(view.ContractsRoot) != "" {
		values = append(values, strings.TrimSpace(view.ContractsRoot))
	}
	for _, flow := range view.Flows {
		if id := strings.TrimSpace(flow.ID); id != "" {
			values = append(values, id)
		}
	}
	return values
}
