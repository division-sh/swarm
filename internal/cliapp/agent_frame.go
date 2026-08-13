package cliapp

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/division-sh/swarm/internal/cli/argcount"
	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/spf13/cobra"
)

type agentFrameCommandOptions struct {
	apiOptions   rootCommandOptions
	output       cliOutputOptions
	scope        string
	bundleHash   string
	flow         string
	root         bool
	flowInstance string
}

func newAgentFrameCommand(opts rootCommandOptions) *cobra.Command {
	frameOpts := agentFrameCommandOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "frame <agent-id>",
		Short: "Inspect an agent's static or effective execution frame.",
		Args:  argcount.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := frameOpts.output.validate(); err != nil {
				return returnCLIValidationError(cmd.ErrOrStderr(), err)
			}
			return runAgentFrameCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), frameOpts, args[0])
		},
	}
	argcount.SetDiscoveryHint(cmd, "List agent ids with `swarm agent list`.")
	cmd.Flags().StringVar(&frameOpts.scope, "scope", "", "Inspection scope: static or effective")
	cmd.Flags().StringVar(&frameOpts.bundleHash, "bundle-hash", "", "Static bundle identity")
	cmd.Flags().StringVar(&frameOpts.flow, "flow", "", "Static authored flow path, or root")
	cmd.Flags().BoolVar(&frameOpts.root, "root", false, "Select the effective root agent")
	cmd.Flags().StringVar(&frameOpts.flowInstance, "flow-instance", "", "Select one effective concrete flow instance")
	bindCLIOutputFlags(cmd, &frameOpts.output)
	bindCLIAPIConnectionFlags(cmd, &frameOpts.apiOptions)
	return cmd
}

func runAgentFrameCommand(ctx context.Context, out, errOut io.Writer, opts agentFrameCommandOptions, agentID string) error {
	params, err := opts.params(agentID)
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return returnCLIAPIError(errOut, err, agentFrameAPIErrorClassifier())
	}
	var result agentframe.Inspection
	if err := client.call(ctx, "agent.frame", params, &result); err != nil {
		return returnCLIAPIError(errOut, err, agentFrameAPIErrorClassifier())
	}
	return renderCLIOutput(out, errOut, opts.output, result, func(w io.Writer) {
		writeAgentFrameResult(w, result)
	}, func() ([]string, error) {
		return []string{result.Session.AgentID}, nil
	})
}

func (opts agentFrameCommandOptions) params(agentID string) (map[string]any, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return nil, fmt.Errorf("agent id is required")
	}
	scope := strings.TrimSpace(opts.scope)
	params := map[string]any{"scope": scope, "agent_id": agentID}
	switch scope {
	case string(agentframe.InspectionStatic):
		if opts.root || strings.TrimSpace(opts.flowInstance) != "" {
			return nil, fmt.Errorf("--scope static forbids --root and --flow-instance")
		}
		bundleHash := strings.TrimSpace(opts.bundleHash)
		flow, err := exactAgentFrameCLIPath(opts.flow, "--flow")
		if err != nil {
			return nil, err
		}
		if bundleHash == "" || flow == "" {
			return nil, fmt.Errorf("--scope static requires --bundle-hash and --flow")
		}
		params["bundle_hash"] = bundleHash
		params["flow"] = flow
	case string(agentframe.InspectionEffective):
		if strings.TrimSpace(opts.bundleHash) != "" || strings.TrimSpace(opts.flow) != "" {
			return nil, fmt.Errorf("--scope effective forbids --bundle-hash and --flow")
		}
		flowInstance, err := exactAgentFrameCLIPath(opts.flowInstance, "--flow-instance")
		if err != nil {
			return nil, err
		}
		if opts.root == (flowInstance != "") {
			return nil, fmt.Errorf("--scope effective requires exactly one of --root or --flow-instance")
		}
		if opts.root {
			params["root"] = true
		} else {
			params["flow_instance"] = flowInstance
		}
	default:
		return nil, fmt.Errorf("--scope must be static or effective")
	}
	return params, nil
}

func exactAgentFrameCLIPath(value, flag string) (string, error) {
	if value == "" {
		return "", nil
	}
	if value != strings.TrimSpace(value) || value != strings.Trim(value, "/") {
		return "", fmt.Errorf("%s must be an exact canonical path without surrounding whitespace or leading or trailing slash", flag)
	}
	return value, nil
}

func writeAgentFrameResult(out io.Writer, result agentframe.Inspection) {
	provider := "unresolved"
	if result.Session.Provider.Value != nil {
		provider = result.Session.Provider.Value.Provider + "/" + result.Session.Provider.Value.Transport
	}
	writeCLILabeledDetail(out, cliLabeledDetail{
		Title: "Agent execution frame",
		Rows: []cliLabeledDetailRow{
			{Label: "Agent", Value: result.Session.AgentID},
			{Label: "Scope", Value: string(result.Scope)},
			{Label: "Version", Value: result.Version},
			{Label: "Bundle", Value: result.Session.BundleHash},
			{Label: "Flow", Value: result.Session.AuthoredFlow},
			{Label: "Intent", Value: result.Session.Intent.Identity},
			{Label: "Criteria", Value: result.Session.Criteria.Identity},
			{Label: "Provider", Value: provider},
			{Label: "Turn", Value: result.Turn.Kind.Status},
		},
	})
}

func agentFrameAPIErrorClassifier() cliAPIErrorClassifier {
	return cliAPIErrorClassifier{notFoundCodes: []string{"AGENT_NOT_FOUND", "BUNDLE_NOT_FOUND"}}
}
