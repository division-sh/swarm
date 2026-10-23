package providertriggers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime/core/provideroutput"
)

type PollingWebhookActive struct{ URL string }

func (PollingWebhookActive) Error() string {
	return "Telegram has a webhook; connect through its authenticated deployment holder"
}

// CompileRegistrationPolling selects the original immutable normalizer without
// creating HTTP ingress or a webhook-secret requirement.
func (s *CatalogSnapshot) CompileRegistrationPolling(provider string) (InboundAdmissionPlan, error) {
	if s == nil || provider != "telegram" {
		return InboundAdmissionPlan{}, fmt.Errorf("registration polling requires the admitted Telegram trigger")
	}
	entry, ok := s.entryValueByProvider(provider)
	if !ok || entry.manifest.Transport() != packs.ChannelTransportWebhook {
		return InboundAdmissionPlan{}, fmt.Errorf("registration polling has no admitted Telegram normalizer")
	}
	manifest, identity := entry.manifest, entry.identity
	return InboundAdmissionPlan{generation: s.Generation(), provider: provider, policySource: PolicySourceVerifiedPack,
		transport: manifest.Transport(), requestAuthentication: RequestAuthenticationToken, registrationPolling: true,
		packIdentity: &identity, manifest: &manifest, outputs: manifest.OutputManifest()}, nil
}

// PollTelegramClaim owns the authenticated provider read. It never invents a
// webhook header or publishes customer traffic during registration.
func (p InboundAdmissionPlan) PollTelegramClaim(ctx context.Context, client *http.Client, token string, channel packs.SatisfactionPlan, challenge string, offset int64) (operatorchannel.InboundClaim, int64, error) {
	var absent operatorchannel.InboundClaim
	if !p.Valid() || !p.registrationPolling || p.provider != "telegram" || p.manifest == nil || p.packIdentity == nil ||
		channel.Provider() != p.provider || !operatorchannel.ValidChallenge(challenge) || offset < 0 || token == "" || strings.ContainsAny(token, "/?#\r\n") {
		return absent, offset, fmt.Errorf("Telegram claim requires its admitted provider and exact challenge")
	}
	var webhook struct {
		URL string `json:"url"`
	}
	if err := readTelegramRegistration(ctx, client, token, "getWebhookInfo", nil, &webhook); err != nil {
		return absent, offset, err
	}
	if webhook.URL != "" {
		return absent, offset, PollingWebhookActive{URL: webhook.URL}
	}
	var updates []json.RawMessage
	if err := readTelegramRegistration(ctx, client, token, "getUpdates", url.Values{"offset": {strconv.FormatInt(offset, 10)}, "timeout": {"0"}, "limit": {"100"}}, &updates); err != nil {
		return absent, offset, err
	}
	for _, raw := range updates {
		var identity struct {
			ID int64 `json:"update_id"`
		}
		if err := json.Unmarshal(raw, &identity); err != nil || identity.ID < offset || identity.ID < 0 || identity.ID == 1<<63-1 {
			return absent, offset, fmt.Errorf("Telegram update identity is invalid")
		}
		offset = identity.ID + 1
		outputs, err := p.manifest.ProjectNormalizedPayload(raw)
		if err != nil {
			return absent, offset, err
		}
		for _, output := range outputs {
			authorization, err := provideroutput.NewAuthorization(p.provider, string(output.Name), p.packIdentity.ID(), p.packIdentity.Version(), p.packIdentity.ManifestHash(), p.generation)
			if err != nil {
				return absent, offset, err
			}
			fact, matches, err := channel.ProjectTextFact(string(output.Name), authorization, output.Payload)
			if err != nil {
				return absent, offset, err
			}
			if !matches || fact.Text != challenge {
				continue
			}
			eventID := strconv.FormatInt(identity.ID, 10)
			return operatorchannel.InboundClaim{TextFact: fact, Provider: p.provider, ProviderEventID: eventID,
				Challenge: challenge, ProviderAuthorization: operatorchannel.Hash("authenticated-telegram-poll-v1", p.generation.Diagnostic())}, offset, nil
		}
	}
	return absent, offset, nil
}

// ConfirmTelegramClaim acknowledges only the durably consumed update. Updates
// returned by this read are deliberately left for their eventual consumer.
func (p InboundAdmissionPlan) ConfirmTelegramClaim(ctx context.Context, client *http.Client, token, eventID string) (int64, error) {
	id, err := strconv.ParseInt(eventID, 10, 64)
	if !p.Valid() || !p.registrationPolling || p.provider != "telegram" || token == "" || strings.ContainsAny(token, "/?#\r\n") ||
		err != nil || id < 0 || id == 1<<63-1 || strconv.FormatInt(id, 10) != eventID {
		return 0, fmt.Errorf("Telegram confirmation requires its exact admitted update")
	}
	var later []json.RawMessage
	err = readTelegramRegistration(ctx, client, token, "getUpdates", url.Values{"offset": {strconv.FormatInt(id+1, 10)}, "timeout": {"0"}, "limit": {"1"}}, &later)
	return id + 1, err
}

// Both provider reads share the same response bound, redirect refusal and
// redacted errors; neither can expose the token-bearing request URL.
func readTelegramRegistration(ctx context.Context, client *http.Client, token, method string, query url.Values, result any) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.telegram.org/bot"+token+"/"+method+"?"+query.Encode(), nil)
	if err != nil {
		return fmt.Errorf("Telegram registration request is invalid")
	}
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	copy := *client
	copy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := copy.Do(request)
	if err != nil {
		return fmt.Errorf("Telegram registration read unavailable")
	}
	defer response.Body.Close()
	var envelope struct {
		OK     bool            `json:"ok"`
		Result json.RawMessage `json:"result"`
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil || len(raw) > 1<<20 || response.StatusCode != http.StatusOK || json.Unmarshal(raw, &envelope) != nil || !envelope.OK {
		return fmt.Errorf("Telegram refused registration read (HTTP %d)", response.StatusCode)
	}
	return json.Unmarshal(envelope.Result, result)
}
