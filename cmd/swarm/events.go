package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/gorilla/websocket"
	"github.com/spf13/cobra"
)

const (
	eventObservationMethodList      = "event.list"
	eventObservationMethodGet       = "event.get"
	eventObservationMethodSubscribe = "event.subscribe"
	eventObservationExitValidation  = 2
	eventObservationExitRuntime     = 3
	eventObservationExitAuth        = 4
	eventObservationExitNotFound    = 5
	eventReplayMethod               = "event.replay"
	eventReplayExitValidation       = 2
	eventReplayExitRuntime          = 3
	eventReplayExitAuth             = 4
	eventReplayExitNotFound         = 5
	eventReplayExitConflict         = 6
)

type eventFilterOptions struct {
	runID            string
	entityID         string
	eventName        string
	deliveryStatus   string
	subscriberID     string
	subscriberType   string
	reasonCode       string
	hasDeadLetter    bool
	hasDeadLetterSet bool
}

type eventListCommandOptions struct {
	apiOptions rootCommandOptions
	filter     eventFilterOptions
	since      string
	until      string
	limit      int
	limitSet   bool
	cursor     string
}

type eventFollowCommandOptions struct {
	apiOptions  rootCommandOptions
	filter      eventFilterOptions
	replaySince string
}

type eventReplayCommandOptions struct {
	apiOptions     rootCommandOptions
	subscribers    []string
	idempotencyKey string
}

type eventListResult struct {
	Events     []eventFull `json:"events"`
	NextCursor string      `json:"next_cursor,omitempty"`
}

type eventFull struct {
	EventID     string            `json:"event_id"`
	EventName   string            `json:"event_name"`
	CreatedAt   string            `json:"created_at"`
	RunID       string            `json:"run_id,omitempty"`
	EntityID    string            `json:"entity_id,omitempty"`
	Source      string            `json:"source"`
	Payload     map[string]any    `json:"payload"`
	Deliveries  []eventDelivery   `json:"deliveries"`
	DeadLetters []eventDeadLetter `json:"dead_letters"`
}

type eventDelivery struct {
	DeliveryID     string `json:"delivery_id"`
	SubscriberType string `json:"subscriber_type"`
	SubscriberID   string `json:"subscriber_id"`
	Status         string `json:"status"`
	SessionID      string `json:"session_id,omitempty"`
	ReasonCode     string `json:"reason_code,omitempty"`
	LastError      string `json:"last_error,omitempty"`
}

type eventDeadLetter struct {
	DeadLetterID string `json:"dead_letter_id"`
	FailureType  string `json:"failure_type"`
	RetryCount   *int   `json:"retry_count"`
	ChainDepth   *int   `json:"chain_depth"`
	CreatedAt    string `json:"created_at"`
	HandlerNode  string `json:"handler_node,omitempty"`
	ErrorMessage string `json:"error_message,omitempty"`
}

type eventSubscriptionResult struct {
	SubscriptionID string `json:"subscription_id"`
}

type eventReplayResult struct {
	EventID             string                `json:"event_id"`
	ReplayEventID       string                `json:"replay_event_id"`
	AuditEventID        string                `json:"audit_event_id"`
	SubscribersReplayed []string              `json:"subscribers_replayed"`
	OriginalDeliveries  []eventReplayDelivery `json:"original_deliveries"`
	NewDeliveries       []eventReplayDelivery `json:"new_deliveries"`
}

type eventReplayDelivery struct {
	DeliveryID       string `json:"delivery_id"`
	SubscriberID     string `json:"subscriber_id"`
	SessionID        string `json:"session_id,omitempty"`
	Status           string `json:"status"`
	Attempt          int    `json:"attempt"`
	SourceDeliveryID string `json:"source_delivery_id,omitempty"`
}

type eventSubscriptionNotification struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  struct {
		Subscription string    `json:"subscription"`
		Result       eventFull `json:"result"`
	} `json:"params"`
}

type eventSubscription struct {
	conn           *websocket.Conn
	subscriptionID string
	events         chan eventFull
	errs           chan error
}

var eventObservationValidDeliveryStatuses = map[string]struct{}{
	"pending":     {},
	"in_progress": {},
	"delivered":   {},
	"failed":      {},
	"dead_letter": {},
}

var eventObservationValidSubscriberTypes = map[string]struct{}{
	"node":  {},
	"agent": {},
}

var eventObservationValidDeadLetterFailureTypes = map[string]struct{}{
	"handler_error":        {},
	"chain_depth_exceeded": {},
	"retry_exhausted":      {},
}

func newEventsCommand(opts rootCommandOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "events",
		Short: "List or follow events through v1 API owners.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(
		newEventsListCommand(opts),
		newEventsFollowCommand(opts),
	)
	return cmd
}

func newEventCommand(opts rootCommandOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "event",
		Short: "View or replay one event through v1 API owners.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(
		newEventViewCommand(opts),
		newEventReplayCommand(opts),
	)
	return cmd
}

func newEventsListCommand(opts rootCommandOptions) *cobra.Command {
	listOpts := eventListCommandOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List events through /v1/rpc event.list.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			listOpts.filter.hasDeadLetterSet = cmd.Flags().Changed("has-dead-letter")
			listOpts.limitSet = cmd.Flags().Changed("limit")
			return runEventListCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), listOpts)
		},
	}
	bindEventFilterFlags(cmd, &listOpts.filter)
	cmd.Flags().StringVar(&listOpts.since, "since", "", "Optional RFC3339 lower created_at bound")
	cmd.Flags().StringVar(&listOpts.until, "until", "", "Optional RFC3339 upper created_at bound")
	cmd.Flags().IntVar(&listOpts.limit, "limit", 0, "Optional page size, 1-1000")
	cmd.Flags().StringVar(&listOpts.cursor, "cursor", "", "Optional pagination cursor")
	return cmd
}

func newEventsFollowCommand(opts rootCommandOptions) *cobra.Command {
	followOpts := eventFollowCommandOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "follow",
		Short: "Follow events through /v1/ws event.subscribe.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			followOpts.filter.hasDeadLetterSet = cmd.Flags().Changed("has-dead-letter")
			return runEventFollowCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), followOpts)
		},
	}
	bindEventFilterFlags(cmd, &followOpts.filter)
	cmd.Flags().StringVar(&followOpts.replaySince, "replay-since", "", "Optional RFC3339 catch-up window start")
	return cmd
}

func newEventViewCommand(opts rootCommandOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "view <event-id>",
		Short: "View one event through /v1/rpc event.get.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runEventViewCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), opts, args[0])
		},
	}
}

func newEventReplayCommand(opts rootCommandOptions) *cobra.Command {
	replayOpts := eventReplayCommandOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "replay <event-id>",
		Short: "Replay one event through /v1/rpc event.replay.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runEventReplayCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), args, replayOpts)
		},
	}
	cmd.Flags().StringArrayVar(&replayOpts.subscribers, "subscriber", nil, "Original agent subscriber to replay to; repeat to select a subset")
	cmd.Flags().StringVar(&replayOpts.idempotencyKey, "idempotency-key", "", "Optional v1 API idempotency key")
	return cmd
}

func bindEventFilterFlags(cmd *cobra.Command, opts *eventFilterOptions) {
	cmd.Flags().StringVar(&opts.runID, "run-id", "", "Filter by run id")
	cmd.Flags().StringVar(&opts.entityID, "entity-id", "", "Filter by entity id")
	cmd.Flags().StringVar(&opts.eventName, "event-name", "", "Filter by event name")
	cmd.Flags().StringVar(&opts.deliveryStatus, "delivery-status", "", "Filter by delivery status")
	cmd.Flags().StringVar(&opts.subscriberID, "subscriber-id", "", "Filter by subscriber id")
	cmd.Flags().StringVar(&opts.subscriberType, "subscriber-type", "", "Filter by subscriber type: node or agent")
	cmd.Flags().StringVar(&opts.reasonCode, "reason-code", "", "Filter by delivery/dead-letter reason code")
	cmd.Flags().BoolVar(&opts.hasDeadLetter, "has-dead-letter", false, "Filter by whether the event has dead letters")
}

func runEventListCommand(ctx context.Context, out, errOut io.Writer, opts eventListCommandOptions) error {
	params, err := opts.params()
	if err != nil {
		fmt.Fprintln(errOut, err)
		return commandExitError{code: eventObservationExitValidation}
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		fmt.Fprintln(errOut, err)
		return commandExitError{code: eventObservationErrorExitCode(err)}
	}
	var result eventListResult
	if err := client.call(ctx, eventObservationMethodList, params, &result); err != nil {
		fmt.Fprintln(errOut, err)
		return commandExitError{code: eventObservationErrorExitCode(err)}
	}
	if err := validateEventListResult(result); err != nil {
		fmt.Fprintln(errOut, err)
		return commandExitError{code: eventObservationExitRuntime}
	}
	writeEventListResult(out, result)
	return nil
}

func runEventViewCommand(ctx context.Context, out, errOut io.Writer, opts rootCommandOptions, eventID string) error {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		fmt.Fprintln(errOut, "event id is required")
		return commandExitError{code: eventObservationExitValidation}
	}
	client, err := newCLIAPIClient(opts)
	if err != nil {
		fmt.Fprintln(errOut, err)
		return commandExitError{code: eventObservationErrorExitCode(err)}
	}
	var result eventFull
	if err := client.call(ctx, eventObservationMethodGet, map[string]any{"event_id": eventID}, &result); err != nil {
		fmt.Fprintln(errOut, err)
		return commandExitError{code: eventObservationErrorExitCode(err)}
	}
	if err := validateEventFull("event.get result", result); err != nil {
		fmt.Fprintln(errOut, err)
		return commandExitError{code: eventObservationExitRuntime}
	}
	writeEventDetailResult(out, result)
	return nil
}

func runEventReplayCommand(ctx context.Context, out, errOut io.Writer, args []string, opts eventReplayCommandOptions) error {
	eventID, subscribers, err := validateEventReplayArgs(args, opts.subscribers)
	if err != nil {
		fmt.Fprintln(errOut, err)
		return commandExitError{code: eventReplayExitValidation}
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		fmt.Fprintln(errOut, err)
		return commandExitError{code: eventReplayErrorExitCode(err)}
	}
	var result eventReplayResult
	if err := client.call(ctx, eventReplayMethod, opts.params(eventID, subscribers), &result); err != nil {
		fmt.Fprintln(errOut, err)
		return commandExitError{code: eventReplayErrorExitCode(err)}
	}
	if err := validateEventReplayResult(result, eventID); err != nil {
		fmt.Fprintln(errOut, err)
		return commandExitError{code: eventReplayExitRuntime}
	}
	writeEventReplayResult(out, result)
	return nil
}

func runEventFollowCommand(ctx context.Context, out, errOut io.Writer, opts eventFollowCommandOptions) error {
	params, err := opts.params()
	if err != nil {
		fmt.Fprintln(errOut, err)
		return commandExitError{code: eventObservationExitValidation}
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		fmt.Fprintln(errOut, err)
		return commandExitError{code: eventObservationErrorExitCode(err)}
	}
	wsEndpoint, err := runCommandWebSocketEndpoint(client.endpoint)
	if err != nil {
		fmt.Fprintln(errOut, err)
		return commandExitError{code: eventObservationErrorExitCode(err)}
	}
	sub, err := subscribeEvents(ctx, wsEndpoint, client.token, params)
	if err != nil {
		fmt.Fprintln(errOut, err)
		return commandExitError{code: eventObservationErrorExitCode(err)}
	}
	defer sub.close()
	for {
		select {
		case <-ctx.Done():
			fmt.Fprintln(errOut, "detached from event stream")
			return commandExitError{code: 130}
		case event, ok := <-sub.events:
			if !ok {
				select {
				case err := <-sub.errs:
					if err != nil {
						fmt.Fprintln(errOut, err)
						return commandExitError{code: eventObservationErrorExitCode(err)}
					}
				default:
				}
				return nil
			}
			writeEventFollowEvent(out, event)
		case err := <-sub.errs:
			if err != nil {
				fmt.Fprintln(errOut, err)
				return commandExitError{code: eventObservationErrorExitCode(err)}
			}
		}
	}
}

func (opts eventListCommandOptions) params() (map[string]any, error) {
	params := map[string]any{}
	filter, err := opts.filter.params()
	if err != nil {
		return nil, err
	}
	if len(filter) > 0 {
		params["filter"] = filter
	}
	if cursor := strings.TrimSpace(opts.cursor); cursor != "" {
		params["cursor"] = cursor
	}
	if opts.limitSet {
		if opts.limit < 1 || opts.limit > 1000 {
			return nil, fmt.Errorf("--limit must be between 1 and 1000")
		}
		params["limit"] = opts.limit
	}
	if since := strings.TrimSpace(opts.since); since != "" {
		if err := validateRFC3339Flag("--since", since); err != nil {
			return nil, err
		}
		params["since"] = since
	}
	if until := strings.TrimSpace(opts.until); until != "" {
		if err := validateRFC3339Flag("--until", until); err != nil {
			return nil, err
		}
		params["until"] = until
	}
	return params, nil
}

func (opts eventFollowCommandOptions) params() (map[string]any, error) {
	params := map[string]any{}
	filter, err := opts.filter.params()
	if err != nil {
		return nil, err
	}
	if len(filter) > 0 {
		params["filter"] = filter
	}
	if replaySince := strings.TrimSpace(opts.replaySince); replaySince != "" {
		if err := validateRFC3339Flag("--replay-since", replaySince); err != nil {
			return nil, err
		}
		params["replay_since"] = replaySince
	}
	return params, nil
}

func validateEventReplayArgs(args []string, subscriberFlags []string) (string, []string, error) {
	if len(args) != 1 {
		return "", nil, fmt.Errorf("event replay requires <event-id>")
	}
	eventID := strings.TrimSpace(args[0])
	if eventID == "" {
		return "", nil, fmt.Errorf("event id is required")
	}
	subscribers := make([]string, 0, len(subscriberFlags))
	for _, subscriber := range subscriberFlags {
		subscriber = strings.TrimSpace(subscriber)
		if subscriber == "" {
			return "", nil, fmt.Errorf("--subscriber must be a non-empty agent id")
		}
		subscribers = append(subscribers, subscriber)
	}
	return eventID, subscribers, nil
}

func (opts eventReplayCommandOptions) params(eventID string, subscribers []string) map[string]any {
	params := map[string]any{"event_id": eventID}
	if len(subscribers) > 0 {
		params["subscribers"] = subscribers
	}
	if key := strings.TrimSpace(opts.idempotencyKey); key != "" {
		params["idempotency_key"] = key
	}
	return params
}

func (opts eventFilterOptions) params() (map[string]any, error) {
	filter := map[string]any{}
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "run_id", value: opts.runID},
		{name: "entity_id", value: opts.entityID},
		{name: "event_name", value: opts.eventName},
		{name: "subscriber_id", value: opts.subscriberID},
		{name: "reason_code", value: opts.reasonCode},
	} {
		if value := strings.TrimSpace(field.value); value != "" {
			filter[field.name] = value
		}
	}
	if status := strings.ToLower(strings.TrimSpace(opts.deliveryStatus)); status != "" {
		if _, ok := eventObservationValidDeliveryStatuses[status]; !ok {
			return nil, fmt.Errorf("--delivery-status must be one of pending, in_progress, delivered, failed, dead_letter")
		}
		filter["delivery_status"] = status
	}
	if subscriberType := strings.ToLower(strings.TrimSpace(opts.subscriberType)); subscriberType != "" {
		if _, ok := eventObservationValidSubscriberTypes[subscriberType]; !ok {
			return nil, fmt.Errorf("--subscriber-type must be one of node, agent")
		}
		filter["subscriber_type"] = subscriberType
	}
	if opts.hasDeadLetterSet {
		filter["has_dead_letter"] = opts.hasDeadLetter
	}
	return filter, nil
}

func validateEventListResult(result eventListResult) error {
	if result.Events == nil {
		return fmt.Errorf("malformed event.list result: events is required")
	}
	for i, event := range result.Events {
		if err := validateEventFull(fmt.Sprintf("event.list result: events[%d]", i), event); err != nil {
			return err
		}
	}
	return nil
}

func validateEventFull(prefix string, event eventFull) error {
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "event_id", value: event.EventID},
		{name: "event_name", value: event.EventName},
		{name: "created_at", value: event.CreatedAt},
		{name: "source", value: event.Source},
	} {
		if strings.TrimSpace(field.value) == "" {
			return fmt.Errorf("malformed %s: %s is required", prefix, field.name)
		}
	}
	if err := validateRequiredTimestamp(prefix+".created_at", event.CreatedAt); err != nil {
		return err
	}
	if event.Payload == nil {
		return fmt.Errorf("malformed %s: payload is required", prefix)
	}
	if event.Deliveries == nil {
		return fmt.Errorf("malformed %s: deliveries is required", prefix)
	}
	if event.DeadLetters == nil {
		return fmt.Errorf("malformed %s: dead_letters is required", prefix)
	}
	for i, delivery := range event.Deliveries {
		if err := validateEventDelivery(fmt.Sprintf("%s.deliveries[%d]", prefix, i), delivery); err != nil {
			return err
		}
	}
	for i, deadLetter := range event.DeadLetters {
		if err := validateEventDeadLetter(fmt.Sprintf("%s.dead_letters[%d]", prefix, i), deadLetter); err != nil {
			return err
		}
	}
	return nil
}

func validateEventDelivery(prefix string, delivery eventDelivery) error {
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "delivery_id", value: delivery.DeliveryID},
		{name: "subscriber_type", value: delivery.SubscriberType},
		{name: "subscriber_id", value: delivery.SubscriberID},
		{name: "status", value: delivery.Status},
	} {
		if strings.TrimSpace(field.value) == "" {
			return fmt.Errorf("malformed %s: %s is required", prefix, field.name)
		}
	}
	if _, ok := eventObservationValidSubscriberTypes[strings.TrimSpace(delivery.SubscriberType)]; !ok {
		return fmt.Errorf("malformed %s: subscriber_type=%q is not a valid SubscriberType", prefix, delivery.SubscriberType)
	}
	if _, ok := eventObservationValidDeliveryStatuses[strings.TrimSpace(delivery.Status)]; !ok {
		return fmt.Errorf("malformed %s: status=%q is not a valid DeliveryStatus", prefix, delivery.Status)
	}
	return nil
}

func validateEventDeadLetter(prefix string, deadLetter eventDeadLetter) error {
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "dead_letter_id", value: deadLetter.DeadLetterID},
		{name: "failure_type", value: deadLetter.FailureType},
		{name: "created_at", value: deadLetter.CreatedAt},
	} {
		if strings.TrimSpace(field.value) == "" {
			return fmt.Errorf("malformed %s: %s is required", prefix, field.name)
		}
	}
	if _, ok := eventObservationValidDeadLetterFailureTypes[strings.TrimSpace(deadLetter.FailureType)]; !ok {
		return fmt.Errorf("malformed %s: failure_type=%q is not a valid DeadLetter failure_type", prefix, deadLetter.FailureType)
	}
	if deadLetter.RetryCount == nil {
		return fmt.Errorf("malformed %s: retry_count is required", prefix)
	}
	if *deadLetter.RetryCount < 0 {
		return fmt.Errorf("malformed %s: retry_count must be >= 0", prefix)
	}
	if deadLetter.ChainDepth == nil {
		return fmt.Errorf("malformed %s: chain_depth is required", prefix)
	}
	if *deadLetter.ChainDepth < 0 {
		return fmt.Errorf("malformed %s: chain_depth must be >= 0", prefix)
	}
	if err := validateRequiredTimestamp(prefix+".created_at", deadLetter.CreatedAt); err != nil {
		return err
	}
	return nil
}

func validateEventReplayResult(result eventReplayResult, expectedEventID string) error {
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "event_id", value: result.EventID},
		{name: "replay_event_id", value: result.ReplayEventID},
		{name: "audit_event_id", value: result.AuditEventID},
	} {
		if strings.TrimSpace(field.value) == "" {
			return fmt.Errorf("malformed event.replay result: %s is required", field.name)
		}
	}
	if strings.TrimSpace(result.EventID) != expectedEventID {
		return fmt.Errorf("malformed event.replay result: event_id=%q, want %q", result.EventID, expectedEventID)
	}
	if result.SubscribersReplayed == nil {
		return fmt.Errorf("malformed event.replay result: subscribers_replayed is required")
	}
	if result.OriginalDeliveries == nil {
		return fmt.Errorf("malformed event.replay result: original_deliveries is required")
	}
	if result.NewDeliveries == nil {
		return fmt.Errorf("malformed event.replay result: new_deliveries is required")
	}
	for i, subscriber := range result.SubscribersReplayed {
		if strings.TrimSpace(subscriber) == "" {
			return fmt.Errorf("malformed event.replay result: subscribers_replayed[%d] is required", i)
		}
	}
	for i, delivery := range result.OriginalDeliveries {
		if err := validateEventReplayDelivery(fmt.Sprintf("original_deliveries[%d]", i), delivery, false); err != nil {
			return err
		}
	}
	for i, delivery := range result.NewDeliveries {
		if err := validateEventReplayDelivery(fmt.Sprintf("new_deliveries[%d]", i), delivery, true); err != nil {
			return err
		}
	}
	return nil
}

func validateEventReplayDelivery(field string, delivery eventReplayDelivery, requireSource bool) error {
	for _, part := range []struct {
		name  string
		value string
	}{
		{name: "delivery_id", value: delivery.DeliveryID},
		{name: "subscriber_id", value: delivery.SubscriberID},
		{name: "status", value: delivery.Status},
	} {
		if strings.TrimSpace(part.value) == "" {
			return fmt.Errorf("malformed event.replay result: %s.%s is required", field, part.name)
		}
	}
	if _, ok := eventObservationValidDeliveryStatuses[strings.TrimSpace(delivery.Status)]; !ok {
		return fmt.Errorf("malformed event.replay result: %s.status=%q is not a valid DeliveryStatus", field, delivery.Status)
	}
	if delivery.Attempt < 1 {
		return fmt.Errorf("malformed event.replay result: %s.attempt must be >= 1", field)
	}
	if requireSource && strings.TrimSpace(delivery.SourceDeliveryID) == "" {
		return fmt.Errorf("malformed event.replay result: %s.source_delivery_id is required", field)
	}
	return nil
}

func subscribeEvents(ctx context.Context, wsEndpoint, token string, params map[string]any) (*eventSubscription, error) {
	header := http.Header{"Authorization": []string{"Bearer " + token}}
	conn, resp, err := websocket.DefaultDialer.DialContext(ctx, wsEndpoint, header)
	if err != nil {
		if resp != nil {
			return nil, eventObservationWSHTTPError(resp)
		}
		return nil, fmt.Errorf("v1 WS dial failed: %w", err)
	}
	requestID := "swarm-cli:" + eventObservationMethodSubscribe
	if err := conn.WriteJSON(jsonRPCRequest{
		JSONRPC: "2.0",
		ID:      requestID,
		Method:  eventObservationMethodSubscribe,
		Params:  params,
	}); err != nil {
		conn.Close()
		return nil, fmt.Errorf("write event.subscribe request: %w", err)
	}
	var envelope jsonRPCResponse
	if err := conn.ReadJSON(&envelope); err != nil {
		conn.Close()
		return nil, fmt.Errorf("read event.subscribe response: %w", err)
	}
	if envelope.JSONRPC != "2.0" {
		conn.Close()
		return nil, fmt.Errorf("malformed event.subscribe response: jsonrpc=%q", envelope.JSONRPC)
	}
	if id, ok := envelope.ID.(string); !ok || id != requestID {
		conn.Close()
		return nil, fmt.Errorf("malformed event.subscribe response: id=%s, want %q", formatJSONRPCID(envelope.ID), requestID)
	}
	if envelope.Error != nil {
		conn.Close()
		return nil, envelope.Error
	}
	var result eventSubscriptionResult
	if err := json.Unmarshal(envelope.Result, &result); err != nil {
		conn.Close()
		return nil, fmt.Errorf("decode event.subscribe result: %w", err)
	}
	if strings.TrimSpace(result.SubscriptionID) == "" {
		conn.Close()
		return nil, fmt.Errorf("malformed event.subscribe result: subscription_id is required")
	}
	sub := &eventSubscription{
		conn:           conn,
		subscriptionID: result.SubscriptionID,
		events:         make(chan eventFull, 16),
		errs:           make(chan error, 1),
	}
	go sub.readLoop()
	return sub, nil
}

func eventObservationWSHTTPError(resp *http.Response) error {
	if resp == nil {
		return nil
	}
	message := http.StatusText(resp.StatusCode)
	if resp.Body != nil {
		defer resp.Body.Close()
		raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		if err == nil && strings.TrimSpace(string(raw)) != "" {
			message = strings.TrimSpace(string(raw))
		}
	}
	return &cliAPIHTTPError{surface: "v1 WS", statusCode: resp.StatusCode, message: message}
}

func (s *eventSubscription) readLoop() {
	defer close(s.events)
	for {
		var notification eventSubscriptionNotification
		if err := s.conn.ReadJSON(&notification); err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) || strings.Contains(err.Error(), "use of closed network connection") {
				return
			}
			s.reportError(fmt.Errorf("read event.subscribe notification: %w", err))
			return
		}
		if notification.JSONRPC != "2.0" || notification.Method != "rpc.subscription" {
			s.reportError(fmt.Errorf("malformed event.subscribe notification"))
			return
		}
		if notification.Params.Subscription != s.subscriptionID {
			s.reportError(fmt.Errorf("malformed event.subscribe notification: subscription mismatch"))
			return
		}
		event := notification.Params.Result
		if err := validateEventFull("event.subscribe notification", event); err != nil {
			s.reportError(err)
			return
		}
		select {
		case s.events <- event:
		default:
			s.reportError(fmt.Errorf("event.subscribe notification queue overflow"))
			return
		}
	}
}

func (s *eventSubscription) reportError(err error) {
	select {
	case s.errs <- err:
	default:
	}
}

func (s *eventSubscription) close() {
	if s == nil || s.conn == nil {
		return
	}
	_ = s.conn.Close()
}

func writeEventListResult(out io.Writer, result eventListResult) {
	if out == nil {
		return
	}
	if len(result.Events) == 0 {
		fmt.Fprintln(out, "No events match the filter.")
		return
	}
	fmt.Fprintln(out, "EVENT AT\tEVENT\tEVENT ID\tRUN\tENTITY\tDELIVERIES\tDEAD_LETTERS")
	for _, event := range result.Events {
		fmt.Fprintf(out, "%s\t%s\t%s\t%s\t%s\t%d\t%d\n",
			event.CreatedAt,
			event.EventName,
			event.EventID,
			eventObservationDash(event.RunID),
			eventObservationDash(event.EntityID),
			len(event.Deliveries),
			len(event.DeadLetters),
		)
	}
	if strings.TrimSpace(result.NextCursor) != "" {
		fmt.Fprintf(out, "next_cursor=%s\n", result.NextCursor)
	}
}

func writeEventDetailResult(out io.Writer, event eventFull) {
	if out == nil {
		return
	}
	fmt.Fprintf(out, "Event %s\n", event.EventID)
	fmt.Fprintf(out, "event_name=%s created_at=%s source=%s run_id=%s entity_id=%s\n",
		event.EventName,
		event.CreatedAt,
		event.Source,
		eventObservationDash(event.RunID),
		eventObservationDash(event.EntityID),
	)
	fmt.Fprintf(out, "payload=%s\n", eventObservationCompactJSON(event.Payload))
	if len(event.Deliveries) == 0 {
		fmt.Fprintln(out, "deliveries: none")
	} else {
		fmt.Fprintln(out, "deliveries:")
		for _, delivery := range event.Deliveries {
			fmt.Fprintf(out, "  delivery_id=%s subscriber=%s/%s status=%s session_id=%s reason_code=%s last_error=%s\n",
				delivery.DeliveryID,
				delivery.SubscriberType,
				delivery.SubscriberID,
				delivery.Status,
				eventObservationDash(delivery.SessionID),
				eventObservationDash(delivery.ReasonCode),
				eventObservationDash(delivery.LastError),
			)
		}
	}
	if len(event.DeadLetters) == 0 {
		fmt.Fprintln(out, "dead_letters: none")
	} else {
		fmt.Fprintln(out, "dead_letters:")
		for _, deadLetter := range event.DeadLetters {
			fmt.Fprintf(out, "  dead_letter_id=%s failure_type=%s retry_count=%d chain_depth=%d created_at=%s handler_node=%s error=%s\n",
				deadLetter.DeadLetterID,
				deadLetter.FailureType,
				*deadLetter.RetryCount,
				*deadLetter.ChainDepth,
				deadLetter.CreatedAt,
				eventObservationDash(deadLetter.HandlerNode),
				eventObservationDash(deadLetter.ErrorMessage),
			)
		}
	}
}

func writeEventFollowEvent(out io.Writer, event eventFull) {
	if out == nil {
		return
	}
	fields := []string{
		"event_id=" + event.EventID,
		"event_name=" + event.EventName,
		"at=" + event.CreatedAt,
		"source=" + event.Source,
	}
	if event.RunID != "" {
		fields = append(fields, "run_id="+event.RunID)
	}
	if event.EntityID != "" {
		fields = append(fields, "entity_id="+event.EntityID)
	}
	fields = append(fields,
		fmt.Sprintf("deliveries=%d", len(event.Deliveries)),
		fmt.Sprintf("dead_letters=%d", len(event.DeadLetters)),
	)
	fmt.Fprintf(out, "event %s\n", strings.Join(fields, " "))
}

func writeEventReplayResult(out io.Writer, result eventReplayResult) {
	if out == nil {
		return
	}
	fmt.Fprintf(out, "event replay ok: event_id=%s replay_event_id=%s audit_event_id=%s subscribers_replayed=%s original_deliveries=%d new_deliveries=%d\n",
		result.EventID,
		result.ReplayEventID,
		result.AuditEventID,
		strings.Join(result.SubscribersReplayed, ","),
		len(result.OriginalDeliveries),
		len(result.NewDeliveries),
	)
	for _, delivery := range result.OriginalDeliveries {
		fmt.Fprintf(out, "original_delivery delivery_id=%s subscriber_id=%s status=%s session_id=%s attempt=%d\n",
			delivery.DeliveryID,
			delivery.SubscriberID,
			delivery.Status,
			eventObservationDash(delivery.SessionID),
			delivery.Attempt,
		)
	}
	for _, delivery := range result.NewDeliveries {
		fmt.Fprintf(out, "new_delivery delivery_id=%s subscriber_id=%s status=%s session_id=%s attempt=%d source_delivery_id=%s\n",
			delivery.DeliveryID,
			delivery.SubscriberID,
			delivery.Status,
			eventObservationDash(delivery.SessionID),
			delivery.Attempt,
			delivery.SourceDeliveryID,
		)
	}
}

func eventObservationCompactJSON(value map[string]any) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return "{}"
	}
	return string(raw)
}

func eventObservationDash(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "-"
	}
	return value
}

func eventObservationErrorExitCode(err error) int {
	if err == nil {
		return 0
	}
	if strings.Contains(err.Error(), "SWARM_API_TOKEN is required") {
		return eventObservationExitAuth
	}
	var httpErr *cliAPIHTTPError
	if errors.As(err, &httpErr) {
		if httpErr.statusCode == http.StatusUnauthorized || httpErr.statusCode == http.StatusForbidden {
			return eventObservationExitAuth
		}
		return eventObservationExitRuntime
	}
	var rpcErr *jsonRPCError
	if errors.As(err, &rpcErr) {
		switch applicationErrorCode(rpcErr.Data) {
		case "UNAUTHORIZED":
			return eventObservationExitAuth
		case "EVENT_NOT_FOUND":
			return eventObservationExitNotFound
		default:
			return eventObservationExitRuntime
		}
	}
	return eventObservationExitRuntime
}

func eventReplayErrorExitCode(err error) int {
	if err == nil {
		return 0
	}
	if strings.Contains(err.Error(), "SWARM_API_TOKEN is required") {
		return eventReplayExitAuth
	}
	var httpErr *cliAPIHTTPError
	if errors.As(err, &httpErr) {
		if httpErr.statusCode == http.StatusUnauthorized || httpErr.statusCode == http.StatusForbidden {
			return eventReplayExitAuth
		}
		return eventReplayExitRuntime
	}
	var rpcErr *jsonRPCError
	if errors.As(err, &rpcErr) {
		switch applicationErrorCode(rpcErr.Data) {
		case "UNAUTHORIZED":
			return eventReplayExitAuth
		case "EVENT_NOT_FOUND":
			return eventReplayExitNotFound
		case "EVENT_REPLAY_NO_DELIVERY_HISTORY",
			"EVENT_REPLAY_SUBSCRIBER_NOT_ORIGINAL",
			"EVENT_REPLAY_SUBSCRIBER_UNAVAILABLE",
			"EVENT_REPLAY_NOT_ELIGIBLE",
			"PAYLOAD_VALIDATION_FAILED",
			"IDEMPOTENCY_CONFLICT":
			return eventReplayExitConflict
		default:
			return eventReplayExitRuntime
		}
	}
	return eventReplayExitRuntime
}
