package main

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"
)

const (
	agentReplayMethod         = "agent.replay"
	agentReplayExitValidation = 2
	agentReplayExitRuntime    = 3
	agentReplayExitAuth       = 4
	agentReplayExitNotFound   = 5
	agentReplayExitConflict   = 6
)

type agentReplayCommandOptions struct {
	apiOptions     rootCommandOptions
	eventID        string
	idempotencyKey string
}

type agentReplayResult struct {
	EventID          string              `json:"event_id"`
	AgentID          string              `json:"agent_id"`
	ReplayEventID    string              `json:"replay_event_id"`
	AuditEventID     string              `json:"audit_event_id"`
	OriginalDelivery agentReplayDelivery `json:"original_delivery"`
	NewDelivery      agentReplayDelivery `json:"new_delivery"`
}

type agentReplayDelivery struct {
	DeliveryID       string `json:"delivery_id"`
	SubscriberID     string `json:"subscriber_id"`
	SessionID        string `json:"session_id,omitempty"`
	Status           string `json:"status"`
	Attempt          int    `json:"attempt"`
	SourceDeliveryID string `json:"source_delivery_id,omitempty"`
}

var agentReplayValidDeliveryStatuses = map[string]struct{}{
	"pending":     {},
	"in_progress": {},
	"delivered":   {},
	"failed":      {},
	"dead_letter": {},
}

func newAgentReplayCommand(opts rootCommandOptions) *cobra.Command {
	replayOpts := agentReplayCommandOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "replay <agent-id>",
		Short: "Replay a single event to an agent.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runAgentReplayCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), args, replayOpts)
		},
	}
	cmd.Flags().StringVar(&replayOpts.eventID, "event-id", "", "Required event ID to replay")
	cmd.Flags().StringVar(&replayOpts.idempotencyKey, "idempotency-key", "", "Optional idempotency key for safe retries (advanced)")
	_ = cmd.Flags().MarkHidden("idempotency-key")
	bindCLIAPIConnectionFlagsWithClass(cmd, &replayOpts.apiOptions, cliAPICommandClassMutating, "swarm agent replay")
	return cmd
}

func runAgentReplayCommand(ctx context.Context, out, errOut io.Writer, args []string, opts agentReplayCommandOptions) error {
	agentID, eventID, err := validateAgentReplayArgs(args, opts.eventID)
	if err != nil {
		writeCLIAPIError(errOut, err)
		return commandExitError{code: agentReplayExitValidation}
	}

	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		writeCLIAPIError(errOut, err)
		return commandExitError{code: agentReplayErrorExitCode(err)}
	}

	var result agentReplayResult
	if err := client.call(ctx, agentReplayMethod, opts.params(agentID, eventID), &result); err != nil {
		writeCLIAPIError(errOut, err)
		return commandExitError{code: agentReplayErrorExitCode(err)}
	}
	if err := validateAgentReplayResult(result, agentID, eventID); err != nil {
		writeCLIAPIError(errOut, err)
		return commandExitError{code: agentReplayExitRuntime}
	}
	writeAgentReplayResult(out, result)
	return nil
}

func validateAgentReplayArgs(args []string, eventIDFlag string) (string, string, error) {
	if len(args) != 1 {
		return "", "", fmt.Errorf("agent replay requires <agent-id>")
	}
	agentID := strings.TrimSpace(args[0])
	if agentID == "" {
		return "", "", fmt.Errorf("agent id is required")
	}
	eventID := strings.TrimSpace(eventIDFlag)
	if eventID == "" {
		return "", "", fmt.Errorf("--event-id is required")
	}
	return agentID, eventID, nil
}

func (opts agentReplayCommandOptions) params(agentID, eventID string) map[string]any {
	params := map[string]any{
		"agent_id": agentID,
		"event_id": eventID,
	}
	if key := strings.TrimSpace(opts.idempotencyKey); key != "" {
		params["idempotency_key"] = key
	}
	return params
}

func validateAgentReplayResult(result agentReplayResult, expectedAgentID, expectedEventID string) error {
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "agent_id", value: result.AgentID},
		{name: "event_id", value: result.EventID},
		{name: "replay_event_id", value: result.ReplayEventID},
		{name: "audit_event_id", value: result.AuditEventID},
	} {
		if strings.TrimSpace(field.value) == "" {
			return fmt.Errorf("malformed agent.replay result: %s is required", field.name)
		}
	}
	if strings.TrimSpace(result.AgentID) != expectedAgentID {
		return fmt.Errorf("malformed agent.replay result: agent_id=%q, want %q", result.AgentID, expectedAgentID)
	}
	if strings.TrimSpace(result.EventID) != expectedEventID {
		return fmt.Errorf("malformed agent.replay result: event_id=%q, want %q", result.EventID, expectedEventID)
	}
	if err := validateAgentReplayDelivery("original_delivery", result.OriginalDelivery); err != nil {
		return err
	}
	if err := validateAgentReplayDelivery("new_delivery", result.NewDelivery); err != nil {
		return err
	}
	return nil
}

func validateAgentReplayDelivery(field string, delivery agentReplayDelivery) error {
	for _, part := range []struct {
		name  string
		value string
	}{
		{name: "delivery_id", value: delivery.DeliveryID},
		{name: "subscriber_id", value: delivery.SubscriberID},
		{name: "status", value: delivery.Status},
	} {
		if strings.TrimSpace(part.value) == "" {
			return fmt.Errorf("malformed agent.replay result: %s.%s is required", field, part.name)
		}
	}
	if _, ok := agentReplayValidDeliveryStatuses[strings.TrimSpace(delivery.Status)]; !ok {
		return fmt.Errorf("malformed agent.replay result: %s.status=%q is not a valid DeliveryStatus", field, delivery.Status)
	}
	if delivery.Attempt < 1 {
		return fmt.Errorf("malformed agent.replay result: %s.attempt must be >= 1", field)
	}
	return nil
}

func writeAgentReplayResult(out io.Writer, result agentReplayResult) {
	if out == nil {
		return
	}
	fmt.Fprintf(out, "agent replay ok: agent_id=%s event_id=%s replay_event_id=%s audit_event_id=%s original_delivery.delivery_id=%s new_delivery.delivery_id=%s\n",
		result.AgentID,
		result.EventID,
		result.ReplayEventID,
		result.AuditEventID,
		result.OriginalDelivery.DeliveryID,
		result.NewDelivery.DeliveryID,
	)
}

func agentReplayErrorExitCode(err error) int {
	return cliAPIErrorExitCode(err, cliAPIErrorClassifier{
		runtimeExit:   agentReplayExitRuntime,
		authExit:      agentReplayExitAuth,
		notFoundExit:  agentReplayExitNotFound,
		conflictExit:  agentReplayExitConflict,
		notFoundCodes: []string{"EVENT_NOT_FOUND"},
		conflictCodes: []string{
			"EVENT_REPLAY_NO_DELIVERY_HISTORY",
			"EVENT_REPLAY_SUBSCRIBER_NOT_ORIGINAL",
			"EVENT_REPLAY_SUBSCRIBER_UNAVAILABLE",
			"EVENT_REPLAY_NOT_ELIGIBLE",
			"PAYLOAD_VALIDATION_FAILED",
			"IDEMPOTENCY_CONFLICT",
		},
	})
}
