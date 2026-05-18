package main

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

type agentListCommandOptions struct {
	apiOptions rootCommandOptions
	flow       string
	role       string
}

type agentListResult struct {
	Agents []agentSummary `json:"agents"`
}

type agentDetailResult struct {
	Agent             agentSummary     `json:"agent"`
	CurrentSessionRef *agentSessionRef `json:"current_session_ref,omitempty"`
	LastTurnRef       *agentTurnRef    `json:"last_turn_ref,omitempty"`
}

type agentSummary struct {
	AgentID          string `json:"agent_id"`
	Role             string `json:"role"`
	Type             string `json:"type"`
	ModelTier        string `json:"model_tier"`
	ConversationMode string `json:"conversation_mode"`
	SessionScope     string `json:"session_scope"`
	Status           string `json:"status"`
}

type agentSessionRef struct {
	SessionID string `json:"session_id"`
	StartedAt string `json:"started_at"`
}

type agentTurnRef struct {
	TurnID      string `json:"turn_id"`
	CompletedAt string `json:"completed_at"`
	ParseOK     *bool  `json:"parse_ok"`
	Error       string `json:"error,omitempty"`
}

var agentValidStatuses = map[string]struct{}{
	"idle":       {},
	"running":    {},
	"paused":     {},
	"failed":     {},
	"terminated": {},
}

var agentValidConversationModes = map[string]struct{}{
	"task":               {},
	"session":            {},
	"session_per_entity": {},
}

var agentValidSessionScopes = map[string]struct{}{
	"global": {},
	"flow":   {},
	"entity": {},
}

func newAgentsCommand(opts rootCommandOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "agents",
		Short: "List agents through v1 RPC.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(newAgentsListCommand(opts))
	return cmd
}

func newAgentCommand(opts rootCommandOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "agent",
		Short: "View one agent through v1 RPC.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(newAgentViewCommand(opts))
	return cmd
}

func newAgentsListCommand(opts rootCommandOptions) *cobra.Command {
	listOpts := agentListCommandOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List declared agents through v1 RPC.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runAgentListCommand(cmd.Context(), cmd.OutOrStdout(), listOpts)
		},
	}
	cmd.Flags().StringVar(&listOpts.flow, "flow", "", "Filter by canonical flow path")
	cmd.Flags().StringVar(&listOpts.role, "role", "", "Filter by agent role")
	return cmd
}

func newAgentViewCommand(opts rootCommandOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "view <agent-id>",
		Short: "View one agent through v1 RPC.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runAgentViewCommand(cmd.Context(), cmd.OutOrStdout(), opts, args[0])
		},
	}
}

func runAgentListCommand(ctx context.Context, out io.Writer, opts agentListCommandOptions) error {
	params := opts.params()
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return err
	}
	var result agentListResult
	if err := client.call(ctx, "agent.list", params, &result); err != nil {
		return err
	}
	if err := validateAgentListResult(result); err != nil {
		return err
	}
	writeAgentListResult(out, result)
	return nil
}

func runAgentViewCommand(ctx context.Context, out io.Writer, opts rootCommandOptions, agentID string) error {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return fmt.Errorf("agent id is required")
	}
	client, err := newCLIAPIClient(opts)
	if err != nil {
		return err
	}
	var result agentDetailResult
	if err := client.call(ctx, "agent.get", map[string]any{"agent_id": agentID}, &result); err != nil {
		return err
	}
	if err := validateAgentDetailResult(result); err != nil {
		return err
	}
	writeAgentDetailResult(out, result)
	return nil
}

func (opts agentListCommandOptions) params() map[string]any {
	params := map[string]any{}
	if flow := strings.TrimSpace(opts.flow); flow != "" {
		params["flow"] = flow
	}
	if role := strings.TrimSpace(opts.role); role != "" {
		params["role"] = role
	}
	return params
}

func validateAgentListResult(result agentListResult) error {
	if result.Agents == nil {
		return fmt.Errorf("malformed agent.list result: agents is required")
	}
	for i, agent := range result.Agents {
		if err := validateAgentSummary(agent); err != nil {
			return fmt.Errorf("malformed agent.list result: agents[%d]: %w", i, err)
		}
	}
	return nil
}

func validateAgentDetailResult(result agentDetailResult) error {
	if err := validateAgentSummary(result.Agent); err != nil {
		return fmt.Errorf("malformed agent.get result: agent: %w", err)
	}
	if ref := result.CurrentSessionRef; ref != nil {
		if strings.TrimSpace(ref.SessionID) == "" {
			return fmt.Errorf("malformed agent.get result: current_session_ref.session_id is required")
		}
		if err := validateAgentTimestamp("current_session_ref.started_at", ref.StartedAt); err != nil {
			return err
		}
	}
	if ref := result.LastTurnRef; ref != nil {
		if strings.TrimSpace(ref.TurnID) == "" {
			return fmt.Errorf("malformed agent.get result: last_turn_ref.turn_id is required")
		}
		if err := validateAgentTimestamp("last_turn_ref.completed_at", ref.CompletedAt); err != nil {
			return err
		}
		if ref.ParseOK == nil {
			return fmt.Errorf("malformed agent.get result: last_turn_ref.parse_ok is required")
		}
	}
	return nil
}

func validateAgentSummary(agent agentSummary) error {
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "agent_id", value: agent.AgentID},
		{name: "role", value: agent.Role},
		{name: "type", value: agent.Type},
		{name: "model_tier", value: agent.ModelTier},
		{name: "conversation_mode", value: agent.ConversationMode},
		{name: "session_scope", value: agent.SessionScope},
		{name: "status", value: agent.Status},
	} {
		if strings.TrimSpace(field.value) == "" {
			return fmt.Errorf("%s is required", field.name)
		}
	}
	if _, ok := agentValidStatuses[strings.TrimSpace(agent.Status)]; !ok {
		return fmt.Errorf("status=%q is not a valid AgentStatus", agent.Status)
	}
	if _, ok := agentValidConversationModes[strings.TrimSpace(agent.ConversationMode)]; !ok {
		return fmt.Errorf("conversation_mode=%q is not a valid ConversationMode", agent.ConversationMode)
	}
	if _, ok := agentValidSessionScopes[strings.TrimSpace(agent.SessionScope)]; !ok {
		return fmt.Errorf("session_scope=%q is not a valid SessionScope", agent.SessionScope)
	}
	return nil
}

func validateAgentTimestamp(field, value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return fmt.Errorf("malformed agent.get result: %s is required", field)
	}
	if _, err := time.Parse(time.RFC3339Nano, value); err != nil {
		return fmt.Errorf("malformed agent.get result: %s must be an RFC3339 timestamp: %w", field, err)
	}
	return nil
}

func writeAgentListResult(out io.Writer, result agentListResult) {
	if out == nil {
		return
	}
	if len(result.Agents) == 0 {
		fmt.Fprintln(out, "No agents match the filter.")
		return
	}
	fmt.Fprintln(out, "AGENT_ID\tROLE\tTYPE\tSTATUS\tMODEL_TIER\tMODE\tSCOPE")
	for _, agent := range result.Agents {
		fmt.Fprintf(out, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			agent.AgentID,
			agent.Role,
			agent.Type,
			agent.Status,
			agent.ModelTier,
			agent.ConversationMode,
			agent.SessionScope,
		)
	}
}

func writeAgentDetailResult(out io.Writer, result agentDetailResult) {
	if out == nil {
		return
	}
	agent := result.Agent
	fmt.Fprintf(out, "Agent %s\n", agent.AgentID)
	fmt.Fprintf(out, "role=%s type=%s status=%s model_tier=%s conversation_mode=%s session_scope=%s\n",
		agent.Role,
		agent.Type,
		agent.Status,
		agent.ModelTier,
		agent.ConversationMode,
		agent.SessionScope,
	)
	if ref := result.CurrentSessionRef; ref != nil {
		fmt.Fprintf(out, "current_session_ref: session_id=%s started_at=%s\n", ref.SessionID, ref.StartedAt)
	}
	if ref := result.LastTurnRef; ref != nil {
		fmt.Fprintf(out, "last_turn_ref: turn_id=%s completed_at=%s parse_ok=%t error=%s\n",
			ref.TurnID,
			ref.CompletedAt,
			*ref.ParseOK,
			agentDash(ref.Error),
		)
	}
}

func agentDash(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "-"
	}
	return value
}
