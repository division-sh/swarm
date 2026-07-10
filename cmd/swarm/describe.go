package main

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/authoringview"
	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/routingtopology"
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

type describeRoutesCommandOptions struct {
	contractsPath    string
	platformSpecPath string
	configPath       string
	output           cliOutputOptions
	logging          cliLoggingOptions
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
	cmd.AddCommand(newDescribeRoutesCommand(ctx, repo, rootOpts))
	return cmd
}

func newDescribeRoutesCommand(ctx context.Context, repo string, rootOpts rootCommandOptions) *cobra.Command {
	opts := describeRoutesCommandOptions{logging: defaultCLILoggingOptions()}
	cmd := &cobra.Command{
		Use:   "routes",
		Short: "Render the frozen authored routing topology.",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) > 0 {
				return returnCLIValidationError(cmd.ErrOrStderr(), fmt.Errorf("unexpected argument %q", args[0]))
			}
			if rootOpts.rootFlags != nil && rootOpts.rootFlags.configPathSet {
				opts.configPath = rootOpts.rootFlags.configPath
			}
			code := runDescribeRoutesCommandWithOutput(ctx, assetCommandRepoRoot(repo), opts, cmd.OutOrStdout(), cmd.ErrOrStderr())
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

func runDescribeRoutesCommandWithOutput(ctx context.Context, repo string, opts describeRoutesCommandOptions, out, errOut io.Writer) int {
	if err := opts.logging.validate(); err != nil {
		writeDescribeRoutesError(errOut, "describe routes failed: %v\n", err)
		return 2
	}
	if err := opts.output.validate(); err != nil {
		writeDescribeRoutesError(errOut, "describe routes failed: %v\n", err)
		return 2
	}
	resolvedPaths, err := resolveCLIContractPlatformSpecPaths(repo, cliContractPlatformSpecPathOptions{
		ContractsPath: opts.contractsPath, PlatformSpecPath: opts.platformSpecPath, ConfigPath: opts.configPath,
	})
	if err != nil {
		writeDescribeRoutesError(errOut, "describe routes failed: resolve path config: %v\n", err)
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
	report := runtimebootverify.Run(ctx, source, runtimebootverify.Options{})
	topology := authoringview.BuildRoutingTopologyWithReport(source, bundle, &report)
	if err := renderCLIOutput(out, errOut, opts.output, topology, func(w io.Writer) {
		writeRoutingTopologyText(w, topology)
	}, func() ([]string, error) {
		values := make([]string, 0, len(topology.Edges)+len(topology.Issues))
		for _, edge := range topology.Edges {
			values = append(values, edge.ID)
		}
		for _, issue := range topology.Issues {
			values = append(values, issue.ID)
		}
		return values, nil
	}); err != nil {
		return 2
	}
	return 0
}

func writeDescribeRoutesError(out io.Writer, format string, args ...any) {
	if out != nil {
		fmt.Fprintf(out, format, args...)
	}
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
	writeRoutingTopologyText(out, view.RoutingTopology)
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
					if strings.TrimSpace(edge.LoopID) != "" {
						detail += " loop " + edge.LoopID + " " + edge.LoopOperation
						if edge.MaxAttempts != "" {
							detail += " max_attempts=" + edge.MaxAttempts
						}
						if edge.LoopEscape {
							detail += " escape"
						}
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

func writeRoutingTopologyText(out io.Writer, topology routingtopology.Topology) {
	if out == nil {
		return
	}
	fmt.Fprintf(out, "routing topology: %s\n", topology.SchemaVersion)
	fmt.Fprintf(out, "  source authority: %s\n", topology.SourceAuthority)
	intra, inter := 0, 0
	for _, edge := range topology.Edges {
		if edge.Scope == routingtopology.DeliveryScopeInterFlow {
			inter++
		} else {
			intra++
		}
	}
	if len(topology.Edges) == 0 {
		fmt.Fprintln(out, "  routes: none")
	} else if inter == 0 {
		fmt.Fprintf(out, "  cross-flow routes: none (%d intra-flow routes)\n", intra)
	}
	for _, edge := range topology.Edges {
		fmt.Fprintf(out, "  - [%s] %s: %s -> %s\n", edge.Scope, edge.Event.Canonical, routingEndpointText(edge.Producer), routingEndpointText(edge.Consumer))
		if edge.Boundary != nil {
			fmt.Fprintf(out, "    connect: %s -> %s (%s)\n", edge.Boundary.From, edge.Boundary.To, edge.Boundary.AuthoredLocation)
		}
		if edge.Resolution != nil {
			fmt.Fprintf(out, "    resolution: mode=%s delivery=%s target=%s%s\n", formatCLIHumanCode(cliHumanCodeRoutingTopology, edge.Resolution.Mode), formatCLIHumanCode(cliHumanCodeRoutingTopology, edge.Resolution.Delivery), formatCLIHumanCode(cliHumanCodeRoutingTopology, edge.Resolution.TargetKind), routingResolutionDetail(edge.Resolution))
		}
	}
	if len(topology.BoundaryExposures) > 0 {
		fmt.Fprintln(out, "  boundary exposures:")
		for _, exposure := range topology.BoundaryExposures {
			fmt.Fprintf(out, "    - %s: %s -> output %s.%s\n", exposure.Event.Canonical, routingEndpointText(exposure.Producer), routingFlowLabel(exposure.Output.FlowID), exposure.Output.PinName)
		}
	}
	if len(topology.LegacyQualifiedSubscriptions) > 0 {
		fmt.Fprintln(out, "  legacy qualified subscriptions:")
		for _, subscription := range topology.LegacyQualifiedSubscriptions {
			fmt.Fprintf(out, "    - disposition=%s event=%s consumer=%s at %s\n", formatCLIHumanCode(cliHumanCodeRoutingTopology, subscription.Disposition), subscription.Event.Canonical, routingEndpointText(subscription.Consumer), subscription.AuthoredLocation)
			fmt.Fprintf(out, "      runtime delivery=%t canonical edge=%t", subscription.RuntimeDelivery, subscription.CanonicalEdge)
			if subscription.FindingID != "" {
				fmt.Fprintf(out, " finding=%s", subscription.FindingID)
			}
			fmt.Fprintln(out)
			fmt.Fprintf(out, "      migration: %s\n", subscription.Migration)
		}
	}
	if len(topology.Issues) > 0 {
		fmt.Fprintln(out, "  route issues:")
		for _, issue := range topology.Issues {
			if issue.CheckID != "" {
				fmt.Fprintf(out, "    - %s [%s] at %s: %s\n", issue.CheckID, issue.Severity, firstNonEmpty(issue.AuthoredLocation, issue.Location), issue.Message)
				if issue.Remediation != "" {
					fmt.Fprintf(out, "      remediation: %s\n", issue.Remediation)
				}
				continue
			}
			fmt.Fprintf(out, "    - %s: %s -> %s at %s: %s\n", issue.Failure, issue.From, issue.To, issue.AuthoredLocation, issue.Detail)
		}
	}
}

func routingEndpointText(endpoint routingtopology.Endpoint) string {
	actor := endpoint.NodeID
	if actor == "" {
		actor = endpoint.AgentID
	}
	if actor == "" {
		actor = endpoint.Role
	}
	if actor == "" {
		actor = endpoint.TimerID
	}
	if actor == "" {
		actor = endpoint.PinName
	}
	if actor == "" {
		actor = string(endpoint.Kind)
	}
	return routingFlowLabel(endpoint.FlowID) + "." + actor
}

func routingFlowLabel(flowID string) string {
	if flowID = strings.TrimSpace(flowID); flowID != "" {
		return flowID
	}
	return "root"
}

func routingResolutionDetail(resolution *routingtopology.Resolution) string {
	if resolution == nil {
		return ""
	}
	if resolution.InstanceKey != nil {
		return fmt.Sprintf(" key=%s mint=%s as=%s on_missing=%s on_conflict=%s", strings.Join(resolution.InstanceKey.Fields, ","), resolution.InstanceKey.Mint, resolution.InstanceKey.As, resolution.InstanceKey.OnMissing, resolution.InstanceKey.OnConflict)
	}
	if resolution.FanIn != nil {
		return fmt.Sprintf(" singleton=%s aggregation=%s window=%s dedup_by=%s", resolution.FanIn.Singleton, resolution.FanIn.Aggregation, resolution.FanIn.Window, strings.Join(resolution.FanIn.DedupBy, ","))
	}
	if resolution.Reply != nil {
		return fmt.Sprintf(" reply=%s correlation=%s", resolution.Reply.Role, resolution.Reply.CorrelationKey)
	}
	return ""
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
