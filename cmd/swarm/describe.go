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
	configPath       string
	graph            bool
	output           cliOutputOptions
	logging          cliLoggingOptions
}

type describeCommandOutput struct {
	authoringview.View
	WorkspaceBackend string `json:"workspace_backend"`
}

func defaultDescribeCommandOptions() describeCommandOptions {
	return describeCommandOptions{
		logging: defaultCLILoggingOptions(),
	}
}

func newDescribeCommand(ctx context.Context, repo string, rootOpts rootCommandOptions) *cobra.Command {
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
			if rootOpts.rootFlags != nil && rootOpts.rootFlags.configPathSet {
				opts.configPath = rootOpts.rootFlags.configPath
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
	cmd.Flags().BoolVar(&opts.graph, "graph", opts.graph, "Render the per-flow lifecycle stage graph")
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
	resolvedPaths, err := resolveCLIContractPlatformSpecPaths(repo, cliContractPlatformSpecPathOptions{
		ContractsPath:    opts.contractsPath,
		PlatformSpecPath: opts.platformSpecPath,
		ConfigPath:       opts.configPath,
	})
	if err != nil {
		if errOut != nil {
			fmt.Fprintf(errOut, "describe failed: resolve path config: %v\n", err)
		}
		return cliAPIErrorExitCode(err, cliAPIErrorClassifier{})
	}
	contractsRoot, err := normalizeContractsRoot(resolvedPaths.ContractsPath)
	if err != nil {
		writeCLIAPIError(errOut, err)
		return cliExitValidation
	}
	_, bundle, err := newSwarmWorkflowModule(repo, contractsRoot, resolvedPaths.PlatformSpecPath)
	if err != nil {
		writeCLIAPIError(errOut, err)
		return cliExitValidation
	}
	source := semanticview.Wrap(bundle)
	workspaceBackend, err := resolveWorkspaceBackendDiagnostic(repo, source)
	if err != nil {
		if errOut != nil {
			fmt.Fprintf(errOut, "describe failed: resolve workspace backend: %v\n", err)
		}
		return 1
	}
	workspaceBackendDetail := workspaceBackendDecisionDetail(workspaceBackend)
	report := runtimebootverify.Run(ctx, source, runtimebootverify.Options{})
	view, err := authoringview.Build(ctx, source, authoringview.BuildOptions{BootReport: &report, IncludeStageGraph: opts.graph})
	if err != nil {
		if errOut != nil {
			fmt.Fprintf(errOut, "describe failed: %v\n", err)
		}
		return 1
	}
	view.ContractsRoot = contractsRoot
	output := describeCommandOutput{
		View:             view,
		WorkspaceBackend: workspaceBackendDetail,
	}
	if err := renderCLIOutput(out, errOut, opts.output, output, func(w io.Writer) {
		writeDescribeText(w, view, workspaceBackendDetail)
	}, func() ([]string, error) {
		return describeQuietValues(view), nil
	}); err != nil {
		return 2
	}
	return 0
}

func writeDescribeText(out io.Writer, view authoringview.View, workspaceBackendDetail string) {
	if out == nil {
		return
	}
	fmt.Fprintf(out, "describe: contracts=%s\n", view.ContractsRoot)
	fmt.Fprintf(out, "source authority: %s\n", view.SourceAuthority)
	if strings.TrimSpace(workspaceBackendDetail) != "" {
		fmt.Fprintf(out, "%s\n", workspaceBackendDetail)
	}
	if view.Root.PrimaryEntity != nil {
		fmt.Fprintf(out, "root primary entity: %s\n", view.Root.PrimaryEntity.Type)
	}
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
			if plan.Reply != nil {
				fmt.Fprintf(out, "    reply: role=%s request=%s.%s reply=%s.%s provider=%s.%s->%s\n", plan.Reply.Role, plan.Reply.RequesterFlowID, plan.Reply.RequestOutputPin, plan.Reply.RequesterFlowID, plan.Reply.ReplyInputPin, plan.Reply.ProviderFlowID, plan.Reply.ProviderInputPin, plan.Reply.ProviderOutputPin)
			}
		}
	}
	if len(view.StageGraphs) > 0 {
		fmt.Fprintln(out, "stage graph:")
		for _, graph := range view.StageGraphs {
			label := strings.TrimSpace(graph.FlowID)
			if label == "" {
				label = "root"
			}
			if strings.TrimSpace(graph.FlowPath) != "" {
				label += " (" + strings.TrimSpace(graph.FlowPath) + ")"
			}
			fmt.Fprintf(out, "  flow %s:\n", label)
			if len(graph.Nodes) > 0 {
				fmt.Fprintln(out, "    nodes:")
				for _, node := range graph.Nodes {
					markers := make([]string, 0, 2)
					if node.Initial {
						markers = append(markers, "initial")
					}
					if node.Terminal {
						markers = append(markers, "terminal")
					}
					suffix := ""
					if len(markers) > 0 {
						suffix = " [" + strings.Join(markers, ",") + "]"
					}
					if strings.TrimSpace(node.Description) != "" {
						suffix += " - " + strings.TrimSpace(node.Description)
					}
					fmt.Fprintf(out, "      - %s%s\n", node.ID, suffix)
				}
			}
			if len(graph.Edges) > 0 {
				fmt.Fprintln(out, "    edges:")
				for _, edge := range graph.Edges {
					from := strings.Join(edge.From, ",")
					if from == "" {
						from = "<none>"
					}
					detail := strings.TrimSpace(edge.Source)
					if strings.TrimSpace(edge.NodeID) != "" {
						detail += " " + strings.TrimSpace(edge.NodeID)
					}
					if strings.TrimSpace(edge.EventType) != "" {
						detail += " on " + strings.TrimSpace(edge.EventType)
					}
					if strings.TrimSpace(edge.After) != "" {
						detail += " after " + strings.TrimSpace(edge.After)
					}
					if strings.TrimSpace(edge.TimerID) != "" {
						detail += " timer " + strings.TrimSpace(edge.TimerID)
					}
					fmt.Fprintf(out, "      - %s -> %s (%s)\n", from, edge.To, strings.TrimSpace(detail))
				}
			}
			if len(graph.Timers) > 0 {
				fmt.Fprintln(out, "    timers:")
				for _, timer := range graph.Timers {
					parts := []string{
						strings.TrimSpace(timer.Stage),
						"after " + strings.TrimSpace(timer.After),
					}
					if strings.TrimSpace(timer.Emit) != "" {
						parts = append(parts, "emit "+strings.TrimSpace(timer.Emit))
					}
					if strings.TrimSpace(timer.AdvancesTo) != "" {
						parts = append(parts, "advances_to "+strings.TrimSpace(timer.AdvancesTo))
					}
					if strings.TrimSpace(timer.TimerID) != "" {
						parts = append(parts, "(timer "+strings.TrimSpace(timer.TimerID)+")")
					}
					fmt.Fprintf(out, "      - %s\n", strings.Join(parts, " "))
				}
			}
			if len(graph.FanOuts) > 0 {
				fmt.Fprintln(out, "    fan_out:")
				for _, fanOut := range graph.FanOuts {
					from := strings.Join(fanOut.From, ",")
					if from == "" {
						from = "<none>"
					}
					parts := []string{
						fmt.Sprintf("%s ->xN %s", from, strings.TrimSpace(fanOut.Emit)),
						"items_from " + strings.TrimSpace(fanOut.ItemsFrom),
					}
					if strings.TrimSpace(fanOut.ItemAlias) != "" {
						parts = append(parts, "as "+strings.TrimSpace(fanOut.ItemAlias))
					}
					if strings.TrimSpace(fanOut.Identity) != "" {
						parts = append(parts, "identity "+strings.TrimSpace(fanOut.Identity))
					}
					detail := strings.TrimSpace(fanOut.Source)
					if strings.TrimSpace(fanOut.NodeID) != "" {
						detail += " " + strings.TrimSpace(fanOut.NodeID)
					}
					if strings.TrimSpace(fanOut.EventType) != "" {
						detail += " on " + strings.TrimSpace(fanOut.EventType)
					}
					if detail != "" {
						parts = append(parts, "("+strings.TrimSpace(detail)+")")
					}
					fmt.Fprintf(out, "      - %s\n", strings.Join(parts, " "))
				}
			}
		}
	}
	if len(view.Diagnostics) > 0 {
		fmt.Fprintln(out, "diagnostics:")
		for _, diagnostic := range view.Diagnostics {
			location := strings.TrimSpace(diagnostic.AuthoredLocation)
			if location == "" {
				location = strings.TrimSpace(diagnostic.Location)
			}
			rendered := runtimebootverify.FormatTypedDiagnosticFinding(runtimebootverify.TypedDiagnosticFinding{
				CheckID:     diagnostic.CheckID,
				Severity:    diagnostic.Severity,
				Location:    location,
				Message:     diagnostic.Message,
				Remediation: diagnostic.Remediation,
				Evidence:    diagnostic.Evidence,
			}, false)
			fmt.Fprintf(out, "  - %s\n", strings.ReplaceAll(rendered, "\n", "\n    "))
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
