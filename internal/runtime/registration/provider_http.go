package registration

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
)

const providerRegistrationSource = "provider_registration"

type HTTPExecutor struct {
	Client *http.Client
}

type PendingApply struct {
	handle           *runtimeeffects.Handle
	responseObserved bool
}

type ApplyResult struct {
	Output       any
	Pending      *PendingApply
	Acknowledged bool
}

func (e HTTPExecutor) Read(ctx context.Context, toolID string, tool runtimecontracts.ToolSchemaEntry, input, credentials map[string]any) (any, error) {
	if tool.Category() != runtimecontracts.ToolCategoryProviderRegistration || tool.Effect() != runtimecontracts.ActivityEffectClassReadOnly {
		return nil, fmt.Errorf("provider registration read tool %q has an invalid contract", strings.TrimSpace(toolID))
	}
	prepared, secrets, err := prepareProviderRequest(toolID, tool, input, credentials)
	if err != nil {
		return nil, err
	}
	response, raw, err := e.executeProviderRequest(ctx, prepared)
	if err != nil {
		return nil, redactProviderError(err, secrets)
	}
	return projectProviderResponse(toolID, tool, response, raw, secrets)
}

func (e HTTPExecutor) Apply(ctx context.Context, toolID string, tool runtimecontracts.ToolSchemaEntry, input, credentials map[string]any, lineage map[string]string) (ApplyResult, error) {
	if tool.Category() != runtimecontracts.ToolCategoryProviderRegistration || tool.Effect() != runtimecontracts.ActivityEffectClassNonIdempotentWrite {
		return ApplyResult{}, fmt.Errorf("provider registration apply tool %q has an invalid contract", strings.TrimSpace(toolID))
	}
	prepared, secrets, err := prepareProviderRequest(toolID, tool, input, credentials)
	if err != nil {
		return ApplyResult{}, err
	}
	fingerprint, err := semanticProviderRequest(toolID, tool, input)
	if err != nil {
		return ApplyResult{}, err
	}
	handle, err := runtimeeffects.BeginServeRegistration(ctx, fingerprint, lineage)
	if err != nil {
		return ApplyResult{}, err
	}
	pending := &PendingApply{handle: handle}
	response, raw, launched, err := e.executeProviderApply(ctx, prepared, handle)
	if err != nil {
		if !launched {
			return ApplyResult{}, err
		}
		return ApplyResult{Pending: pending}, runtimefailures.Wrap(runtimefailures.ClassOutcomeUncertain, "provider_registration_acknowledgment_lost", providerRegistrationSource, "dispatch", map[string]any{"tool": strings.TrimSpace(toolID)}, redactProviderError(err, secrets))
	}
	if err := handle.MarkResponseObserved(ctx, map[string]any{"status": response.StatusCode}); err != nil {
		return ApplyResult{Pending: pending}, err
	}
	pending.responseObserved = true
	output, err := projectProviderResponse(toolID, tool, response, raw, secrets)
	if err != nil {
		return ApplyResult{Pending: pending}, runtimefailures.Wrap(runtimefailures.ClassOutcomeUncertain, "provider_registration_response_unconfirmed", providerRegistrationSource, "validate_response", map[string]any{"tool": strings.TrimSpace(toolID), "status": response.StatusCode}, err)
	}
	if err := handle.Succeed(ctx, map[string]any{"status": response.StatusCode}); err != nil {
		return ApplyResult{Output: output, Pending: pending}, err
	}
	return ApplyResult{Output: output, Acknowledged: true}, nil
}

func (p *PendingApply) SettleReadback(ctx context.Context, exact bool, cause error) error {
	if p == nil || p.handle == nil {
		return fmt.Errorf("provider registration pending apply is missing")
	}
	if exact {
		if !p.responseObserved {
			if err := p.handle.MarkResponseObserved(ctx, map[string]any{"authority": "provider_readback", "matched": true}); err != nil {
				return err
			}
			p.responseObserved = true
		}
		return p.handle.Succeed(ctx, map[string]any{"authority": "provider_readback", "matched": true})
	}
	return p.handle.Fail(ctx, runtimeeffects.StateOutcomeUncertain, runtimefailures.ClassOutcomeUncertain, "provider_registration_outcome_uncertain", providerRegistrationSource, "reconcile_readback", map[string]any{"matched": false}, cause)
}

func prepareProviderRequest(toolID string, tool runtimecontracts.ToolSchemaEntry, input, credentials map[string]any) (runtimecontracts.PreparedToolHTTPRequest, []string, error) {
	if err := tool.InputSchema().Validate(input); err != nil {
		return runtimecontracts.PreparedToolHTTPRequest{}, nil, fmt.Errorf("provider registration tool %q input: %w", toolID, err)
	}
	httpExecution, ok := tool.HTTPExecution()
	if !ok {
		return runtimecontracts.PreparedToolHTTPRequest{}, nil, fmt.Errorf("provider registration tool %q has no HTTP execution plan", toolID)
	}
	secrets := make([]string, 0, len(tool.Credentials()))
	admitted := make(map[string]any, len(tool.Credentials()))
	for _, logical := range tool.Credentials() {
		value, ok := credentials[logical]
		text, textOK := value.(string)
		if !ok || !textOK || text == "" {
			return runtimecontracts.PreparedToolHTTPRequest{}, nil, fmt.Errorf("provider registration tool %q credential %q is missing", toolID, logical)
		}
		admitted[logical] = text
		secrets = append(secrets, text)
	}
	request, err := httpExecution.Prepare(input, admitted)
	if err != nil {
		return runtimecontracts.PreparedToolHTTPRequest{}, nil, redactProviderError(err, secrets)
	}
	parsed, err := url.Parse(request.URL())
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil {
		return runtimecontracts.PreparedToolHTTPRequest{}, nil, fmt.Errorf("provider registration tool %q must prepare an HTTPS URL without userinfo", toolID)
	}
	return request, secrets, nil
}

func (e HTTPExecutor) executeProviderRequest(ctx context.Context, prepared runtimecontracts.PreparedToolHTTPRequest) (*http.Response, []byte, error) {
	requestCtx, cancel := context.WithTimeout(ctx, prepared.Timeout())
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, prepared.Method(), prepared.URL(), bytes.NewReader(prepared.Body()))
	if err != nil {
		return nil, nil, err
	}
	request.Header = prepared.Headers()
	client := e.providerClient(prepared.Timeout())
	response, err := client.Do(request)
	if err != nil {
		return nil, nil, err
	}
	defer response.Body.Close()
	raw, err := readProviderBody(response.Body)
	if err != nil {
		return response, nil, err
	}
	return response, raw, nil
}

func (e HTTPExecutor) executeProviderApply(ctx context.Context, prepared runtimecontracts.PreparedToolHTTPRequest, handle *runtimeeffects.Handle) (*http.Response, []byte, bool, error) {
	requestCtx, cancel := context.WithTimeout(ctx, prepared.Timeout())
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, prepared.Method(), prepared.URL(), bytes.NewReader(prepared.Body()))
	if err != nil {
		return nil, nil, false, err
	}
	request.Header = prepared.Headers()
	client := e.providerClient(prepared.Timeout())
	if err := handle.MarkLaunched(ctx); err != nil {
		return nil, nil, false, err
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, nil, true, err
	}
	defer response.Body.Close()
	raw, err := readProviderBody(response.Body)
	return response, raw, true, err
}

func (e HTTPExecutor) providerClient(timeout time.Duration) *http.Client {
	client := http.Client{Timeout: timeout}
	if e.Client != nil {
		client = *e.Client
		if client.Timeout <= 0 {
			client.Timeout = timeout
		}
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &client
}

func readProviderBody(body io.Reader) ([]byte, error) {
	const limit = 4 << 20
	raw, err := io.ReadAll(io.LimitReader(body, limit+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > limit {
		return nil, fmt.Errorf("provider registration response exceeds %d bytes", limit)
	}
	return raw, nil
}

func projectProviderResponse(toolID string, tool runtimecontracts.ToolSchemaEntry, response *http.Response, raw []byte, secrets []string) (any, error) {
	var body any = map[string]any{}
	if len(bytes.TrimSpace(raw)) > 0 {
		admitted, err := canonicaljson.Decode(raw)
		if err != nil {
			return nil, redactProviderError(fmt.Errorf("provider registration tool %q returned invalid semantic JSON: %w", toolID, err), secrets)
		}
		body = admitted.Interface()
	}
	environment := map[string]any{"response": map[string]any{
		"status": response.StatusCode, "headers": responseHeaders(response.Header), "body": body,
	}}
	if policy, ok := tool.ResponseSuccessPolicy(); ok {
		if err := policy.Evaluate(environment); err != nil {
			return nil, redactProviderError(err, secrets)
		}
	}
	result := body
	if mapping, ok := tool.CompiledResponseMapping(); ok {
		mapped, err := mapping.Render(environment)
		if err != nil {
			return nil, redactProviderError(err, secrets)
		}
		result = mapped
	}
	if err := tool.OutputSchema().Validate(result); err != nil {
		return nil, redactProviderError(fmt.Errorf("provider registration tool %q projected response: %w", toolID, err), secrets)
	}
	return result, nil
}

func responseHeaders(headers http.Header) map[string]any {
	out := make(map[string]any, len(headers))
	for key, values := range headers {
		if len(values) == 1 {
			out[strings.ToLower(key)] = values[0]
		} else {
			items := make([]any, len(values))
			for index := range values {
				items[index] = values[index]
			}
			out[strings.ToLower(key)] = items
		}
	}
	return out
}

func semanticProviderRequest(toolID string, tool runtimecontracts.ToolSchemaEntry, input map[string]any) ([]byte, error) {
	contract, err := tool.CanonicalValue()
	if err != nil {
		return nil, fmt.Errorf("canonicalize provider registration tool %q: %w", strings.TrimSpace(toolID), err)
	}
	return canonicaljson.Bytes(map[string]any{
		"tool_id":  strings.TrimSpace(toolID),
		"contract": contract,
		"input":    input,
	})
}

func redactProviderError(err error, secrets []string) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s", runtimemanagedcredentials.RedactString(err.Error(), secrets...))
}
