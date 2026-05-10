package apiv1

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/store"
)

type Pinger interface {
	Ping(context.Context) error
}

type RunReadStore interface {
	LoadRunHeader(context.Context, string) (store.RunHeader, error)
	ListRunHeaders(context.Context, store.RunHeaderListOptions) ([]store.RunHeader, string, error)
	LoadRunDebugReport(context.Context, string, store.RunDebugQueryOptions) (store.RunDebugReport, error)
}

type OperatorReadOptions struct {
	Now      func() time.Time
	Ready    func() bool
	Database Pinger
	Runs     RunReadStore
	Bundle   runtimecontracts.BundleIdentity
}

type healthPingResult struct {
	OK bool   `json:"ok"`
	TS string `json:"ts"`
}

type healthCheckResult struct {
	Alive     bool                            `json:"alive"`
	Ready     bool                            `json:"ready"`
	DBOK      bool                            `json:"db_ok"`
	RuntimeOK bool                            `json:"runtime_ok"`
	Bundle    runtimecontracts.BundleIdentity `json:"bundle"`
}

type runGetResult struct {
	Run store.RunHeader `json:"run"`
}

type runListResult struct {
	Runs       []store.RunHeader `json:"runs"`
	NextCursor string            `json:"next_cursor,omitempty"`
}

type runDiagnosis struct {
	Run              store.RunHeader `json:"run"`
	OperationalState string          `json:"operational_state"`
	BlockingLayer    string          `json:"blocking_layer"`
	BlockingReason   string          `json:"blocking_reason"`
	Heuristics       []string        `json:"heuristics"`
}

func OperatorReadHandlers(opts OperatorReadOptions) map[string]MethodHandler {
	now := opts.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	ready := opts.Ready
	if ready == nil {
		ready = func() bool { return false }
	}
	return map[string]MethodHandler{
		"health.ping": func(context.Context, Request) (any, error) {
			return healthPingResult{OK: true, TS: now().UTC().Format(time.RFC3339Nano)}, nil
		},
		"health.check": func(ctx context.Context, _ Request) (any, error) {
			runtimeOK := ready()
			dbOK := false
			if opts.Database != nil {
				dbOK = opts.Database.Ping(ctx) == nil
			}
			return healthCheckResult{
				Alive:     true,
				Ready:     runtimeOK && dbOK,
				DBOK:      dbOK,
				RuntimeOK: runtimeOK,
				Bundle:    opts.Bundle,
			}, nil
		},
		"run.get": func(ctx context.Context, req Request) (any, error) {
			runs, err := requireRunReadStore(opts.Runs)
			if err != nil {
				return nil, err
			}
			runID := stringParam(req.Params, "run_id")
			header, err := runs.LoadRunHeader(ctx, runID)
			if errors.Is(err, store.ErrRunNotFound) {
				return nil, NewApplicationError(RunNotFoundCode, false, map[string]any{"run_id": runID})
			}
			if err != nil {
				return nil, err
			}
			return runGetResult{Run: header}, nil
		},
		"run.list": func(ctx context.Context, req Request) (any, error) {
			runs, err := requireRunReadStore(opts.Runs)
			if err != nil {
				return nil, err
			}
			listOpts, err := runHeaderListOptionsFromParams(req.Params)
			if err != nil {
				return nil, err
			}
			headers, nextCursor, err := runs.ListRunHeaders(ctx, listOpts)
			if errors.Is(err, store.ErrInvalidRunListCursor) {
				return nil, NewInvalidParamsError(map[string]any{"field": "cursor", "reason": "invalid run list cursor"})
			}
			if err != nil {
				return nil, err
			}
			if headers == nil {
				headers = []store.RunHeader{}
			}
			return runListResult{Runs: headers, NextCursor: nextCursor}, nil
		},
		"run.diagnose": func(ctx context.Context, req Request) (any, error) {
			runs, err := requireRunReadStore(opts.Runs)
			if err != nil {
				return nil, err
			}
			runID := stringParam(req.Params, "run_id")
			header, err := runs.LoadRunHeader(ctx, runID)
			if errors.Is(err, store.ErrRunNotFound) {
				return nil, NewApplicationError(RunNotFoundCode, false, map[string]any{"run_id": runID})
			}
			if err != nil {
				return nil, err
			}
			report, err := runs.LoadRunDebugReport(ctx, runID, store.RunDebugQueryOptions{})
			if errors.Is(err, store.ErrRunNotFound) {
				return nil, NewApplicationError(RunNotFoundCode, false, map[string]any{"run_id": runID})
			}
			if err != nil {
				return nil, err
			}
			status := store.ProjectRunOperationalStatus(report)
			return runDiagnosis{
				Run:              header,
				OperationalState: strings.TrimSpace(status.State),
				BlockingLayer:    strings.TrimSpace(status.BlockingLayer),
				BlockingReason:   strings.TrimSpace(status.BlockingReason),
				Heuristics:       status.Heuristics,
			}, nil
		},
	}
}

func requireRunReadStore(runs RunReadStore) (RunReadStore, error) {
	if runs == nil {
		return nil, fmt.Errorf("run read store is required")
	}
	return runs, nil
}

func runHeaderListOptionsFromParams(params map[string]any) (store.RunHeaderListOptions, error) {
	out := store.RunHeaderListOptions{
		Status: strings.ToLower(strings.TrimSpace(stringParam(params, "status"))),
		Cursor: strings.TrimSpace(stringParam(params, "cursor")),
	}
	if raw, ok := params["limit"]; ok && !isEmptyParam(raw) {
		limit, ok := integerParam(raw)
		if !ok || limit < 1 || limit > 500 {
			return store.RunHeaderListOptions{}, NewInvalidParamsError(map[string]any{"field": "limit", "reason": "must be an integer from 1 to 500"})
		}
		out.Limit = limit
	}
	var err error
	if out.Since, err = timestampParam(params, "since"); err != nil {
		return store.RunHeaderListOptions{}, err
	}
	if out.Until, err = timestampParam(params, "until"); err != nil {
		return store.RunHeaderListOptions{}, err
	}
	return out, nil
}

func timestampParam(params map[string]any, name string) (*time.Time, error) {
	raw := strings.TrimSpace(stringParam(params, name))
	if raw == "" {
		return nil, nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return nil, NewInvalidParamsError(map[string]any{"field": name, "reason": "must be RFC3339 timestamp"})
	}
	value := parsed.UTC()
	return &value, nil
}

func stringParam(params map[string]any, name string) string {
	if params == nil {
		return ""
	}
	value, _ := params[name].(string)
	return strings.TrimSpace(value)
}

func integerParam(value any) (int, bool) {
	switch typed := value.(type) {
	case float64:
		if math.Trunc(typed) != typed {
			return 0, false
		}
		return int(typed), true
	default:
		return 0, false
	}
}
