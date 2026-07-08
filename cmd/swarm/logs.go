package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/spf13/cobra"
)

const (
	runtimeLogsMethodList      = "runtime.logs"
	runtimeLogsMethodSubscribe = "runtime.subscribe_logs"
)

type runtimeLogCommandOptions struct {
	apiOptions rootCommandOptions

	runID     string
	entityID  string
	sessionID string
	component string
	level     string
	errorCode string
	source    string

	follow      bool
	replaySince string

	since  string
	until  string
	limit  int
	cursor string
	order  string

	limitSet  bool
	cursorSet bool
	orderSet  bool
	sinceSet  bool
	untilSet  bool
}

type runtimeLogListResult struct {
	Logs       []runtimeLogEntry `json:"logs"`
	NextCursor string            `json:"next_cursor,omitempty"`
}

type runtimeLogEntry struct {
	LogID     string         `json:"log_id"`
	TS        string         `json:"ts"`
	Level     string         `json:"level"`
	Component string         `json:"component"`
	Source    string         `json:"source"`
	RunID     string         `json:"run_id,omitempty"`
	EntityID  string         `json:"entity_id,omitempty"`
	SessionID string         `json:"session_id,omitempty"`
	ErrorCode string         `json:"error_code,omitempty"`
	Message   *string        `json:"message"`
	Details   map[string]any `json:"details,omitempty"`
}

type runtimeLogSubscriptionResult struct {
	SubscriptionID string `json:"subscription_id"`
}

type runtimeLogSubscriptionNotification struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  struct {
		Subscription string          `json:"subscription"`
		Result       runtimeLogEntry `json:"result"`
	} `json:"params"`
}

type runtimeLogSubscription struct {
	conn           *websocket.Conn
	endpoint       string
	subscriptionID string
	logs           chan runtimeLogEntry
	errs           chan error
}

var runtimeLogValidLevels = map[string]struct{}{
	"debug": {},
	"info":  {},
	"warn":  {},
	"error": {},
}

var runtimeLogValidOrders = map[string]struct{}{
	"asc":  {},
	"desc": {},
}

func newLogsCommand(opts rootCommandOptions) *cobra.Command {
	logOpts := runtimeLogCommandOptions{apiOptions: opts}
	cmd := &cobra.Command{
		Use:   "logs [filters]",
		Short: "List or follow runtime logs.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			logOpts.limitSet = cmd.Flags().Changed("limit")
			logOpts.cursorSet = cmd.Flags().Changed("cursor")
			logOpts.orderSet = cmd.Flags().Changed("order")
			logOpts.sinceSet = cmd.Flags().Changed("since")
			logOpts.untilSet = cmd.Flags().Changed("until")
			return runLogsCommand(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), logOpts)
		},
	}
	cmd.Flags().StringVar(&logOpts.runID, "run-id", "", "Filter by run id")
	cmd.Flags().StringVar(&logOpts.entityID, "entity-id", "", "Filter by entity id")
	cmd.Flags().StringVar(&logOpts.sessionID, "session-id", "", "Filter by session id")
	cmd.Flags().StringVar(&logOpts.component, "component", "", "Filter by runtime component")
	cmd.Flags().StringVar(&logOpts.level, "level", "", "Filter by log level: debug, info, warn, or error")
	cmd.Flags().StringVar(&logOpts.errorCode, "error-code", "", "Filter by error code")
	cmd.Flags().StringVar(&logOpts.source, "source", "", "Filter by log source")
	cmd.Flags().BoolVar(&logOpts.follow, "follow", false, "Follow matching runtime logs as they stream")
	cmd.Flags().StringVar(&logOpts.replaySince, "replay-since", "", "With --follow, optional RFC3339 catch-up window start")
	cmd.Flags().StringVar(&logOpts.since, "since", "", "Snapshot-only RFC3339 lower timestamp bound")
	cmd.Flags().StringVar(&logOpts.until, "until", "", "Snapshot-only RFC3339 upper timestamp bound")
	cmd.Flags().IntVar(&logOpts.limit, "limit", 0, "Snapshot-only page size, 1-1000")
	cmd.Flags().StringVar(&logOpts.cursor, "cursor", "", "Snapshot-only pagination cursor")
	cmd.Flags().StringVar(&logOpts.order, "order", "", "Snapshot-only sort order: asc or desc")
	bindCLIAPIConnectionFlags(cmd, &logOpts.apiOptions)
	return cmd
}

func runLogsCommand(ctx context.Context, out, errOut io.Writer, opts runtimeLogCommandOptions) error {
	if opts.follow {
		return runLogsFollowCommand(ctx, out, errOut, opts)
	}
	return runLogsSnapshotCommand(ctx, out, errOut, opts)
}

func runLogsSnapshotCommand(ctx context.Context, out, errOut io.Writer, opts runtimeLogCommandOptions) error {
	params, err := opts.snapshotParams()
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return returnCLIAPIError(errOut, err, runtimeLogErrorClassifier())
	}
	var result runtimeLogListResult
	if err := client.call(ctx, runtimeLogsMethodList, params, &result); err != nil {
		return returnCLIAPIError(errOut, err, runtimeLogErrorClassifier())
	}
	if err := validateRuntimeLogListResult(result); err != nil {
		return returnCLIAPIError(errOut, err, runtimeLogErrorClassifier())
	}
	writeRuntimeLogListResult(out, result)
	return nil
}

func runLogsFollowCommand(ctx context.Context, out, errOut io.Writer, opts runtimeLogCommandOptions) error {
	params, err := opts.followParams()
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	client, err := newCLIAPIClient(opts.apiOptions)
	if err != nil {
		return returnCLIAPIError(errOut, err, runtimeLogErrorClassifier())
	}
	wsEndpoint, err := runCommandWebSocketEndpoint(client.endpoint)
	if err != nil {
		return returnCLIAPIError(errOut, err, runtimeLogErrorClassifier())
	}
	sub, err := subscribeRuntimeLogs(ctx, wsEndpoint, client.token, params)
	if err != nil {
		return returnCLIAPIError(errOut, err, runtimeLogErrorClassifier())
	}
	defer sub.close()
	for {
		select {
		case <-ctx.Done():
			fmt.Fprintln(errOut, "detached from runtime log stream")
			return commandExitError{code: cliExitInterrupted}
		case log, ok := <-sub.logs:
			if !ok {
				select {
				case err := <-sub.errs:
					if err != nil {
						return returnCLIAPIError(errOut, err, runtimeLogErrorClassifier())
					}
				default:
				}
				return nil
			}
			writeRuntimeLogFollowEntry(out, log)
		case err := <-sub.errs:
			if err != nil {
				return returnCLIAPIError(errOut, err, runtimeLogErrorClassifier())
			}
		}
	}
}

func (opts runtimeLogCommandOptions) snapshotParams() (map[string]any, error) {
	if strings.TrimSpace(opts.replaySince) != "" {
		return nil, fmt.Errorf("--replay-since requires --follow")
	}
	params, err := opts.commonParams()
	if err != nil {
		return nil, err
	}
	if opts.limitSet {
		if opts.limit < 1 || opts.limit > 1000 {
			return nil, fmt.Errorf("--limit must be between 1 and 1000")
		}
		params["limit"] = opts.limit
	}
	if cursor := strings.TrimSpace(opts.cursor); cursor != "" {
		params["cursor"] = cursor
	}
	if order := strings.ToLower(strings.TrimSpace(opts.order)); order != "" {
		if _, ok := runtimeLogValidOrders[order]; !ok {
			return nil, fmt.Errorf("--order must be one of asc, desc")
		}
		params["order"] = order
	}
	since := strings.TrimSpace(opts.since)
	until := strings.TrimSpace(opts.until)
	if since != "" {
		if err := validateRFC3339Flag("--since", since); err != nil {
			return nil, err
		}
		params["since"] = since
	}
	if until != "" {
		if err := validateRFC3339Flag("--until", until); err != nil {
			return nil, err
		}
		params["until"] = until
	}
	if since != "" && until != "" {
		sinceTime, _ := time.Parse(time.RFC3339Nano, since)
		untilTime, _ := time.Parse(time.RFC3339Nano, until)
		if untilTime.Before(sinceTime) {
			return nil, fmt.Errorf("--until must be greater than or equal to --since")
		}
	}
	return params, nil
}

func (opts runtimeLogCommandOptions) followParams() (map[string]any, error) {
	for _, flag := range []struct {
		name string
		set  bool
	}{
		{name: "--limit", set: opts.limitSet},
		{name: "--cursor", set: opts.cursorSet},
		{name: "--order", set: opts.orderSet},
		{name: "--since", set: opts.sinceSet},
		{name: "--until", set: opts.untilSet},
	} {
		if flag.set {
			if flag.name == "--since" {
				return nil, fmt.Errorf("--since is not supported with --follow; use --replay-since")
			}
			return nil, fmt.Errorf("%s is not supported with --follow", flag.name)
		}
	}
	params, err := opts.commonParams()
	if err != nil {
		return nil, err
	}
	if replaySince := strings.TrimSpace(opts.replaySince); replaySince != "" {
		if err := validateRFC3339Flag("--replay-since", replaySince); err != nil {
			return nil, err
		}
		params["replay_since"] = replaySince
	}
	return params, nil
}

func (opts runtimeLogCommandOptions) commonParams() (map[string]any, error) {
	params := map[string]any{}
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "run_id", value: opts.runID},
		{name: "entity_id", value: opts.entityID},
		{name: "session_id", value: opts.sessionID},
		{name: "component", value: opts.component},
		{name: "error_code", value: opts.errorCode},
		{name: "source", value: opts.source},
	} {
		if value := strings.TrimSpace(field.value); value != "" {
			params[field.name] = value
		}
	}
	if level := strings.ToLower(strings.TrimSpace(opts.level)); level != "" {
		if _, ok := runtimeLogValidLevels[level]; !ok {
			return nil, fmt.Errorf("--level must be one of debug, info, warn, error")
		}
		params["level"] = level
	}
	return params, nil
}

func validateRuntimeLogListResult(result runtimeLogListResult) error {
	if result.Logs == nil {
		return fmt.Errorf("malformed runtime.logs result: logs is required")
	}
	for i, log := range result.Logs {
		if err := validateRuntimeLogEntry(fmt.Sprintf("runtime.logs result: logs[%d]", i), log); err != nil {
			return err
		}
	}
	return nil
}

func validateRuntimeLogEntry(prefix string, log runtimeLogEntry) error {
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "log_id", value: log.LogID},
		{name: "ts", value: log.TS},
		{name: "level", value: log.Level},
		{name: "component", value: log.Component},
		{name: "source", value: log.Source},
	} {
		if strings.TrimSpace(field.value) == "" {
			return fmt.Errorf("malformed %s: %s is required", prefix, field.name)
		}
	}
	if err := validateRequiredTimestamp(prefix+".ts", log.TS); err != nil {
		return err
	}
	level := strings.TrimSpace(log.Level)
	if _, ok := runtimeLogValidLevels[level]; !ok {
		return fmt.Errorf("malformed %s: level=%q is not a valid LogLevel", prefix, log.Level)
	}
	if log.Message == nil {
		return fmt.Errorf("malformed %s: message is required", prefix)
	}
	return nil
}

func subscribeRuntimeLogs(ctx context.Context, wsEndpoint, token string, params map[string]any) (*runtimeLogSubscription, error) {
	header := http.Header{"Authorization": []string{"Bearer " + token}}
	conn, resp, err := websocket.DefaultDialer.DialContext(ctx, wsEndpoint, header)
	if err != nil {
		if resp != nil {
			return nil, cliAPIWebSocketHTTPError("runtime log stream", wsEndpoint, resp)
		}
		return nil, &cliAPITransportError{surface: "runtime log stream", endpoint: wsEndpoint, operation: "dial", err: err}
	}
	requestID := "swarm-cli:" + runtimeLogsMethodSubscribe
	if err := conn.WriteJSON(jsonRPCRequest{
		JSONRPC: "2.0",
		ID:      requestID,
		Method:  runtimeLogsMethodSubscribe,
		Params:  params,
	}); err != nil {
		conn.Close()
		return nil, &cliAPITransportError{surface: "runtime log stream", endpoint: wsEndpoint, operation: "subscription request", err: err}
	}
	var envelope jsonRPCResponse
	if err := cliAPIReadWebSocketJSON(conn, "runtime log stream", wsEndpoint, "subscription response", &envelope); err != nil {
		conn.Close()
		return nil, err
	}
	if envelope.JSONRPC != "2.0" {
		conn.Close()
		return nil, &cliAPIProtocolError{surface: "runtime log stream", endpoint: wsEndpoint, operation: "subscription response", err: fmt.Errorf("jsonrpc=%q", envelope.JSONRPC)}
	}
	if id, ok := envelope.ID.(string); !ok || id != requestID {
		conn.Close()
		return nil, &cliAPIProtocolError{surface: "runtime log stream", endpoint: wsEndpoint, operation: "subscription response", err: fmt.Errorf("id=%s, want %q", formatJSONRPCID(envelope.ID), requestID)}
	}
	if envelope.Error != nil {
		conn.Close()
		return nil, envelope.Error
	}
	var result runtimeLogSubscriptionResult
	if err := json.Unmarshal(envelope.Result, &result); err != nil {
		conn.Close()
		return nil, &cliAPIProtocolError{surface: "runtime log stream", endpoint: wsEndpoint, operation: "subscription result", err: err}
	}
	if strings.TrimSpace(result.SubscriptionID) == "" {
		conn.Close()
		return nil, &cliAPIProtocolError{surface: "runtime log stream", endpoint: wsEndpoint, operation: "subscription result", err: fmt.Errorf("subscription_id is required")}
	}
	sub := &runtimeLogSubscription{
		conn:           conn,
		endpoint:       wsEndpoint,
		subscriptionID: result.SubscriptionID,
		logs:           make(chan runtimeLogEntry, 16),
		errs:           make(chan error, 1),
	}
	go sub.readLoop()
	return sub, nil
}

func (s *runtimeLogSubscription) readLoop() {
	defer close(s.logs)
	for {
		var notification runtimeLogSubscriptionNotification
		if err := cliAPIReadWebSocketJSON(s.conn, "runtime log stream", s.endpoint, "notification read", &notification); err != nil {
			if cliAPIIsNormalWebSocketClose(err) {
				return
			}
			s.reportError(err)
			return
		}
		if notification.JSONRPC != "2.0" || notification.Method != "rpc.subscription" {
			s.reportError(&cliAPIProtocolError{surface: "runtime log stream", endpoint: s.endpoint, operation: "notification", err: fmt.Errorf("malformed runtime.subscribe_logs notification")})
			return
		}
		if notification.Params.Subscription != s.subscriptionID {
			s.reportError(&cliAPIProtocolError{surface: "runtime log stream", endpoint: s.endpoint, operation: "notification", err: fmt.Errorf("subscription mismatch")})
			return
		}
		log := notification.Params.Result
		if err := validateRuntimeLogEntry("runtime.subscribe_logs notification", log); err != nil {
			s.reportError(&cliAPIProtocolError{surface: "runtime log stream", endpoint: s.endpoint, operation: "notification", err: err})
			return
		}
		select {
		case s.logs <- log:
		default:
			s.reportError(fmt.Errorf("runtime.subscribe_logs notification queue overflow"))
			return
		}
	}
}

func (s *runtimeLogSubscription) reportError(err error) {
	select {
	case s.errs <- err:
	default:
	}
}

func (s *runtimeLogSubscription) close() {
	if s == nil || s.conn == nil {
		return
	}
	_ = s.conn.Close()
}

func writeRuntimeLogListResult(out io.Writer, result runtimeLogListResult) {
	if out == nil {
		return
	}
	if len(result.Logs) == 0 {
		fmt.Fprintln(out, "No runtime logs match the filter.")
		return
	}
	fmt.Fprintln(out, "TIME\tLEVEL\tCOMPONENT\tACTION\tSOURCE\tRUN\tENTITY\tSESSION\tERROR\tMESSAGE")
	for _, log := range result.Logs {
		fmt.Fprintf(out, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			log.TS,
			log.Level,
			log.Component,
			runtimeLogDash(runtimeLogAction(log)),
			log.Source,
			runtimeLogDash(log.RunID),
			runtimeLogDash(log.EntityID),
			runtimeLogDash(log.SessionID),
			runtimeLogDash(log.ErrorCode),
			runtimeLogMessage(log),
		)
	}
	if strings.TrimSpace(result.NextCursor) != "" {
		fmt.Fprintf(out, "next_cursor=%s\n", result.NextCursor)
	}
}

func writeRuntimeLogFollowEntry(out io.Writer, log runtimeLogEntry) {
	if out == nil {
		return
	}
	fields := []string{
		"log_id=" + log.LogID,
		"ts=" + log.TS,
		"level=" + log.Level,
		"component=" + log.Component,
	}
	if action := runtimeLogAction(log); action != "" {
		fields = append(fields, "action="+action)
	}
	fields = append(fields, "source="+log.Source)
	if log.RunID != "" {
		fields = append(fields, "run_id="+log.RunID)
	}
	if log.EntityID != "" {
		fields = append(fields, "entity_id="+log.EntityID)
	}
	if log.SessionID != "" {
		fields = append(fields, "session_id="+log.SessionID)
	}
	if log.ErrorCode != "" {
		fields = append(fields, "error_code="+log.ErrorCode)
	}
	fields = append(fields, "message="+runtimeLogMessage(log))
	if len(log.Details) > 0 {
		fields = append(fields, "details="+runtimeLogCompactJSON(log.Details))
	}
	fmt.Fprintf(out, "log %s\n", strings.Join(fields, " "))
}

func runtimeLogCompactJSON(value map[string]any) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return "{}"
	}
	return string(raw)
}

func runtimeLogDash(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "-"
	}
	return value
}

func runtimeLogAction(log runtimeLogEntry) string {
	if len(log.Details) == 0 {
		return ""
	}
	action, _ := log.Details["action"].(string)
	return strings.TrimSpace(action)
}

func runtimeLogMessage(log runtimeLogEntry) string {
	if log.Message == nil {
		return ""
	}
	return *log.Message
}

func runtimeLogErrorClassifier() cliAPIErrorClassifier {
	return cliAPIErrorClassifier{}
}
