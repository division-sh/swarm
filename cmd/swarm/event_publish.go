package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/spf13/cobra"
)

const (
	eventPublishMethod         = "event.publish"
	eventPublishExitValidation = 2
	eventPublishExitRuntime    = 3
	eventPublishExitAuth       = 4
	eventPublishExitNotFound   = 5
	eventPublishExitRejected   = 6
)

var eventPublishBundleFingerprintPattern = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)

type eventPublishCommandOptions struct {
	apiOptions           rootCommandOptions
	payloadJSON          string
	runID                string
	bundleFingerprint    string
	emitter              string
	idempotencyKey       string
	payloadJSONSet       bool
	runIDSet             bool
	bundleFingerprintSet bool
	emitterSet           bool
	idempotencyKeySet    bool
}

type eventPublishResult struct {
	EventID       string                 `json:"event_id"`
	RunID         string                 `json:"run_id"`
	NewRunCreated *bool                  `json:"new_run_created"`
	Deliveries    []eventPublishDelivery `json:"deliveries"`
}

type eventPublishDelivery struct {
	SubscriberID string `json:"subscriber_id"`
	SessionID    string `json:"session_id,omitempty"`
	Status       string `json:"status"`
	Attempt      int    `json:"attempt"`
}

func newEventPublishCommand(opts rootCommandOptions) *cobra.Command {
	publishOpts := eventPublishCommandOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "publish <event-name>",
		Short: "Publish one event through /v1/rpc event.publish.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			publishOpts.payloadJSONSet = cmd.Flags().Changed("payload-json")
			publishOpts.runIDSet = cmd.Flags().Changed("run-id")
			publishOpts.bundleFingerprintSet = cmd.Flags().Changed("bundle-fingerprint")
			publishOpts.emitterSet = cmd.Flags().Changed("emitter")
			publishOpts.idempotencyKeySet = cmd.Flags().Changed("idempotency-key")
			return runEventPublishCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), args, publishOpts)
		},
	}
	cmd.Flags().StringVar(&publishOpts.payloadJSON, "payload-json", "", "Required JSON object payload")
	cmd.Flags().StringVar(&publishOpts.runID, "run-id", "", "Optional existing nonterminal run id to inject into")
	cmd.Flags().StringVar(&publishOpts.bundleFingerprint, "bundle-fingerprint", "", "Optional expected server bundle fingerprint")
	cmd.Flags().StringVar(&publishOpts.emitter, "emitter", "", "Optional producer identifier")
	cmd.Flags().StringVar(&publishOpts.idempotencyKey, "idempotency-key", "", "Optional v1 API idempotency key")
	bindCLIAPIConnectionFlags(cmd, &publishOpts.apiOptions)
	return cmd
}

func runEventPublishCommand(ctx context.Context, out, errOut io.Writer, args []string, opts eventPublishCommandOptions) error {
	eventName, params, err := opts.params(args)
	if err != nil {
		fmt.Fprintln(errOut, err)
		return commandExitError{code: eventPublishExitValidation}
	}

	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		fmt.Fprintln(errOut, err)
		return commandExitError{code: eventPublishErrorExitCode(err)}
	}

	var result eventPublishResult
	if err := client.call(ctx, eventPublishMethod, params, &result); err != nil {
		fmt.Fprintln(errOut, err)
		return commandExitError{code: eventPublishErrorExitCode(err)}
	}
	if err := validateEventPublishResult(result); err != nil {
		fmt.Fprintln(errOut, err)
		return commandExitError{code: eventPublishExitRuntime}
	}
	writeEventPublishResult(out, eventName, result)
	return nil
}

func (opts eventPublishCommandOptions) params(args []string) (string, map[string]any, error) {
	if len(args) != 1 {
		return "", nil, fmt.Errorf("event publish requires <event-name>")
	}
	eventName := strings.TrimSpace(args[0])
	if eventName == "" {
		return "", nil, fmt.Errorf("event name is required")
	}
	payload, err := opts.payload()
	if err != nil {
		return "", nil, err
	}
	params := map[string]any{
		"event_name": eventName,
		"payload":    payload,
	}
	if runID, err := optionalNonEmptyFlag("--run-id", opts.runID, opts.runIDSet); err != nil {
		return "", nil, err
	} else if runID != "" {
		params["run_id"] = runID
	}
	if fingerprint, err := optionalNonEmptyFlag("--bundle-fingerprint", opts.bundleFingerprint, opts.bundleFingerprintSet); err != nil {
		return "", nil, err
	} else if fingerprint != "" {
		if !eventPublishBundleFingerprintPattern.MatchString(fingerprint) {
			return "", nil, fmt.Errorf("--bundle-fingerprint must be sha256:<64 lowercase hex>")
		}
		params["bundle_ref"] = map[string]any{"fingerprint": fingerprint}
	}
	if emitter, err := optionalNonEmptyFlag("--emitter", opts.emitter, opts.emitterSet); err != nil {
		return "", nil, err
	} else if emitter != "" {
		params["emitter"] = emitter
	}
	if key, err := optionalNonEmptyFlag("--idempotency-key", opts.idempotencyKey, opts.idempotencyKeySet); err != nil {
		return "", nil, err
	} else if key != "" {
		params["idempotency_key"] = key
	}
	return eventName, params, nil
}

func (opts eventPublishCommandOptions) payload() (map[string]any, error) {
	if !opts.payloadJSONSet || strings.TrimSpace(opts.payloadJSON) == "" {
		return nil, fmt.Errorf("event publish requires --payload-json <json-object>")
	}
	var payload map[string]any
	decoder := json.NewDecoder(strings.NewReader(opts.payloadJSON))
	decoder.UseNumber()
	if err := decoder.Decode(&payload); err != nil {
		return nil, fmt.Errorf("--payload-json must be a valid JSON object: %w", err)
	}
	if payload == nil {
		return nil, fmt.Errorf("--payload-json must be a JSON object")
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("--payload-json must contain exactly one JSON object")
	}
	return payload, nil
}

func optionalNonEmptyFlag(name, value string, changed bool) (string, error) {
	value = strings.TrimSpace(value)
	if changed && value == "" {
		return "", fmt.Errorf("%s must be non-empty", name)
	}
	return value, nil
}

func validateEventPublishResult(result eventPublishResult) error {
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "event_id", value: result.EventID},
		{name: "run_id", value: result.RunID},
	} {
		if strings.TrimSpace(field.value) == "" {
			return fmt.Errorf("malformed event.publish result: %s is required", field.name)
		}
	}
	if result.NewRunCreated == nil {
		return fmt.Errorf("malformed event.publish result: new_run_created is required")
	}
	if result.Deliveries == nil {
		return fmt.Errorf("malformed event.publish result: deliveries is required")
	}
	for i, delivery := range result.Deliveries {
		if err := validateEventPublishDelivery(fmt.Sprintf("deliveries[%d]", i), delivery); err != nil {
			return err
		}
	}
	return nil
}

func validateEventPublishDelivery(prefix string, delivery eventPublishDelivery) error {
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "subscriber_id", value: delivery.SubscriberID},
		{name: "status", value: delivery.Status},
	} {
		if strings.TrimSpace(field.value) == "" {
			return fmt.Errorf("malformed event.publish result: %s.%s is required", prefix, field.name)
		}
	}
	if _, ok := eventObservationValidDeliveryStatuses[strings.TrimSpace(delivery.Status)]; !ok {
		return fmt.Errorf("malformed event.publish result: %s.status=%q is not a valid DeliveryStatus", prefix, delivery.Status)
	}
	if delivery.Attempt < 1 {
		return fmt.Errorf("malformed event.publish result: %s.attempt must be >= 1", prefix)
	}
	return nil
}

func writeEventPublishResult(out io.Writer, eventName string, result eventPublishResult) {
	if out == nil {
		return
	}
	fmt.Fprintf(out, "event publish ok: event_id=%s event_name=%s run_id=%s new_run_created=%t deliveries=%d\n",
		result.EventID,
		eventName,
		result.RunID,
		*result.NewRunCreated,
		len(result.Deliveries),
	)
	for _, delivery := range result.Deliveries {
		fmt.Fprintf(out, "delivery subscriber_id=%s status=%s session_id=%s attempt=%d\n",
			delivery.SubscriberID,
			delivery.Status,
			eventObservationDash(delivery.SessionID),
			delivery.Attempt,
		)
	}
}

func eventPublishErrorExitCode(err error) int {
	return cliAPIErrorExitCode(err, cliAPIErrorClassifier{
		runtimeExit:   eventPublishExitRuntime,
		authExit:      eventPublishExitAuth,
		notFoundExit:  eventPublishExitNotFound,
		conflictExit:  eventPublishExitRejected,
		notFoundCodes: []string{"RUN_NOT_FOUND"},
		conflictCodes: []string{
			"BUNDLE_MISMATCH",
			"UNSUPPORTED_BUNDLE_REF",
			"EVENT_NOT_DECLARED",
			"PAYLOAD_VALIDATION_FAILED",
			"RUN_ALREADY_TERMINAL",
			"IDEMPOTENCY_CONFLICT",
		},
	})
}
