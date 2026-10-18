package serveapp

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
)

// The test clock expires only after the CLI has decoded the real claim-wait
// response. Transport/admission time is not the behavior under test (LSF-018).
type channelClaimDeadlineContext struct{ context.Context }

func (c channelClaimDeadlineContext) Err() error {
	if c.Context.Err() != nil {
		return context.Cause(c.Context)
	}
	return nil
}

type channelClaimDeadlineKey struct{}

func channelClaimDeadlineAtReadback(t *testing.T, admitted func(channelonboarding.Result)) context.Context {
	t.Helper()
	safety, stop := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(stop)
	controlled, expire := context.WithCancelCause(safety)
	t.Cleanup(func() { expire(context.Canceled) })
	ctx := channelClaimDeadlineContext{controlled}
	base := http.DefaultTransport
	credentialReadbackOperationID := ""
	http.DefaultTransport = servePublicIngressRoundTripper(func(req *http.Request) (*http.Response, error) {
		if req.Context().Value(channelClaimDeadlineKey{}) != controlled || req.GetBody == nil {
			return base.RoundTrip(req)
		}
		body, err := req.GetBody()
		if err != nil {
			return nil, err
		}
		var request struct{ Method string }
		err = json.NewDecoder(body).Decode(&request)
		_ = body.Close()
		if err != nil {
			return nil, err
		}
		resp, err := base.RoundTrip(req)
		if err != nil || request.Method != "channel.onboarding_get" {
			return resp, err
		}
		var raw bytes.Buffer
		resp.Body = &channelClaimReadbackBody{
			Reader: io.TeeReader(resp.Body, &raw), Closer: resp.Body,
			closed: func() {
				var envelope struct {
					Result channelonboarding.Result
					Error  json.RawMessage
				}
				if err := json.Unmarshal(raw.Bytes(), &envelope); err != nil {
					t.Errorf("decode real claim-wait response: %v", err)
					return
				}
				result := envelope.Result
				// Credential discovery reads the retained responsibility before the
				// real claim wait. Admit that one read without expiring the clock.
				if len(envelope.Error) == 0 || string(envelope.Error) == "null" {
					if result.Operation.Phase == channelonboarding.PhasePreparing && result.IdentityOperation == nil {
						role, err := result.ProviderCredentialRequirement()
						if err != nil || role == "" || credentialReadbackOperationID != "" {
							t.Errorf("credential discovery did not identify one retained provider requirement: %v", err)
							return
						}
						credentialReadbackOperationID = result.Operation.OperationID
						return
					}
				}
				if (len(envelope.Error) != 0 && string(envelope.Error) != "null") || result.Operation.OperationID == "" ||
					(credentialReadbackOperationID != "" && result.Operation.OperationID != credentialReadbackOperationID) ||
					result.Operation.Phase != channelonboarding.PhaseAwaitingExternalIdentity ||
					result.IdentityOperation == nil || result.IdentityOperation.State != "awaiting_claim" {
					t.Errorf("deadline cut did not reach real awaiting-identity readback: %s", raw.Bytes())
					return
				}
				admitted(result)
				expire(context.DeadlineExceeded)
			},
		}
		return resp, nil
	})
	t.Cleanup(func() { http.DefaultTransport = base })
	return context.WithValue(ctx, channelClaimDeadlineKey{}, controlled)
}

type channelClaimReadbackBody struct {
	io.Reader
	io.Closer
	closed func()
}

func (b *channelClaimReadbackBody) Close() error {
	err := b.Closer.Close()
	// cliAPIClient.call closes after decoding, before the claim-wait select.
	b.closed()
	return err
}
