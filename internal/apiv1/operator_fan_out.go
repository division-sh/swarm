package apiv1

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
)

func runFanOutListHandler(opts RunReadHandlerOptions) MethodHandler {
	return func(ctx context.Context, req Request) (any, error) {
		if raw, present := req.Params["filter"]; present {
			filter, ok := raw.(map[string]any)
			if !ok {
				return nil, NewInvalidParamsError(map[string]any{"field": "filter", "reason": "must be an object"})
			}
			for key, value := range filter {
				switch key {
				case "status", "triggering_delivery_id", "flow_path", "semantic_path":
				default:
					return nil, NewInvalidParamsError(map[string]any{"field": "filter." + key, "reason": "unknown filter"})
				}
				if text, ok := value.(string); !ok || text == "" {
					return nil, NewInvalidParamsError(map[string]any{"field": "filter." + key, "reason": "must be a nonempty string; root flow path is ."})
				}
			}
		}
		if raw, present := req.Params["cursor"]; present {
			if cursor, ok := raw.(string); !ok || cursor == "" {
				return nil, NewInvalidParamsError(map[string]any{"field": "cursor", "reason": "must be a nonempty string"})
			}
		}
		var query fanoutobligation.ListQuery
		raw, err := json.Marshal(req.Params)
		if err != nil {
			return nil, err
		}
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&query); err != nil {
			return nil, NewInvalidParamsError(map[string]any{"reason": "invalid fan-out list parameters"})
		}
		if _, supplied := req.Params["limit"]; supplied && query.Limit == 0 {
			return nil, NewInvalidParamsError(map[string]any{"field": "limit", "reason": "limit must be between 1 and 500"})
		}
		if err := query.Validate(); err != nil {
			return nil, NewInvalidParamsError(map[string]any{"reason": err.Error()})
		}
		reader, ok := opts.Runs.(operatorread.FanOutReader)
		if !ok {
			return nil, NewApplicationError(MethodUnavailableCode, false, map[string]any{"method": "run.fan_out.list"})
		}
		page, err := reader.ListFanOutIntents(ctx, query)
		if errors.Is(err, operatorread.ErrRunNotFound) {
			return nil, NewApplicationError(RunNotFoundCode, false, map[string]any{"run_id": query.RunID})
		}
		if errors.Is(err, fanoutobligation.ErrInvalidListCursor) {
			return nil, NewInvalidParamsError(map[string]any{"field": "cursor", "reason": err.Error()})
		}
		if err != nil {
			return nil, err
		}
		if err := page.Validate(query); err != nil {
			return nil, err
		}
		if opts.FanOutRuntime != nil && len(page.Intents) != 0 {
			// The process owner enriches only runtime evidence; durable facts
			// remain the selected store's bounded observation.
			input := page
			input.Intents = append([]fanoutobligation.IntentReadback(nil), page.Intents...)
			observed, err := opts.FanOutRuntime(ctx, input)
			if err != nil {
				return nil, err
			}
			if observed.RunID != page.RunID || len(observed.Intents) != len(page.Intents) {
				return nil, errors.New("fan-out runtime observation changed page identity")
			}
			page.Intents = append([]fanoutobligation.IntentReadback(nil), page.Intents...)
			for index, row := range observed.Intents {
				if row.Key != page.Intents[index].Key {
					return nil, errors.New("fan-out runtime observation changed intent identity")
				}
				if err := row.Runtime.Validate(); err != nil {
					return nil, err
				}
				page.Intents[index].Runtime = row.Runtime
			}
		}
		return page, nil
	}
}
