package cliapp

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/division-sh/swarm/internal/cli/argcount"
	"github.com/spf13/cobra"
)

const (
	agentReplayMethod         = "agent.replay"
	agentReplayUse            = "replay <agent-id>"
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
	EventID            string                `json:"event_id"`
	AgentID            string                `json:"agent_id"`
	ReplayEventID      string                `json:"replay_event_id"`
	AuditEventID       string                `json:"audit_event_id"`
	OriginalDeliveries []agentReplayDelivery `json:"original_deliveries"`
	NewDeliveries      []agentReplayDelivery `json:"new_deliveries"`
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
		Use:   agentReplayUse,
		Short: "Replay a single event to an agent.",
		Args:  argcount.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runAgentReplayCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), args, replayOpts)
		},
	}
	argcount.SetDiscoveryHint(cmd, "List agent ids with `swarm agent list`.")
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
		return "", "", argcount.NewDiagnosticFromUse("swarm agent replay", "replay", agentReplayUse, args, argcount.Rule{Exact: 1}, "List agent ids with `swarm agent list`.")
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
	if len(result.OriginalDeliveries) == 0 {
		return fmt.Errorf("malformed agent.replay result: original_deliveries must contain at least one delivery")
	}
	if len(result.NewDeliveries) != len(result.OriginalDeliveries) {
		return fmt.Errorf("malformed agent.replay result: new_deliveries count=%d, want %d", len(result.NewDeliveries), len(result.OriginalDeliveries))
	}
	originalIDs := make(map[string]struct{}, len(result.OriginalDeliveries))
	for index, delivery := range result.OriginalDeliveries {
		field := fmt.Sprintf("original_deliveries[%d]", index)
		if err := validateAgentReplayDelivery(field, delivery); err != nil {
			return err
		}
		if strings.TrimSpace(delivery.SubscriberID) != expectedAgentID {
			return fmt.Errorf("malformed agent.replay result: %s.subscriber_id=%q, want %q", field, delivery.SubscriberID, expectedAgentID)
		}
		if _, duplicate := originalIDs[delivery.DeliveryID]; duplicate {
			return fmt.Errorf("malformed agent.replay result: duplicate original delivery_id=%q", delivery.DeliveryID)
		}
		originalIDs[delivery.DeliveryID] = struct{}{}
	}
	matched := make(map[string]struct{}, len(result.NewDeliveries))
	for index, delivery := range result.NewDeliveries {
		field := fmt.Sprintf("new_deliveries[%d]", index)
		if err := validateAgentReplayDelivery(field, delivery); err != nil {
			return err
		}
		if strings.TrimSpace(delivery.SubscriberID) != expectedAgentID {
			return fmt.Errorf("malformed agent.replay result: %s.subscriber_id=%q, want %q", field, delivery.SubscriberID, expectedAgentID)
		}
		sourceID := strings.TrimSpace(delivery.SourceDeliveryID)
		if sourceID == "" {
			return fmt.Errorf("malformed agent.replay result: %s.source_delivery_id is required", field)
		}
		if _, ok := originalIDs[sourceID]; !ok {
			return fmt.Errorf("malformed agent.replay result: %s.source_delivery_id=%q is not an original delivery", field, sourceID)
		}
		if _, duplicate := matched[sourceID]; duplicate {
			return fmt.Errorf("malformed agent.replay result: original delivery_id=%q matched multiple new deliveries", sourceID)
		}
		matched[sourceID] = struct{}{}
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
	fmt.Fprintf(out, "agent replay ok: agent_id=%s event_id=%s replay_event_id=%s audit_event_id=%s deliveries=%d\n",
		result.AgentID,
		result.EventID,
		result.ReplayEventID,
		result.AuditEventID,
		len(result.NewDeliveries),
	)
	for _, delivery := range result.NewDeliveries {
		fmt.Fprintf(out, "replayed delivery: source_delivery_id=%s new_delivery_id=%s status=%s\n",
			delivery.SourceDeliveryID, delivery.DeliveryID, formatCLIHumanCode(cliHumanCodeDeliveryStatus, delivery.Status))
	}
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
