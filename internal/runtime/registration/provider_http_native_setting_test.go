package registration

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/channelnative"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/plangeneration"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
	"github.com/google/uuid"
)

type nativeSettingHarness struct{ *effecttest.Harness }

func (h *nativeSettingHarness) IsExternalEffectAuthorityCurrent(_ context.Context, authority runtimeeffects.Authority) (bool, error) {
	return authority.Kind == runtimeeffects.AuthorityChannelNativeSetting && authority.Valid(), nil
}

func nativeSettingTestContext(h *nativeSettingHarness) context.Context {
	plan, err := plangeneration.FromCanonicalValue(map[string]string{"test": "channel-native-setting"})
	if err != nil {
		panic(err)
	}
	settingID := uuid.NewString()
	operationID, err := channelnative.InstallOperationID(settingID, 1)
	if err != nil {
		panic(err)
	}
	authority := runtimeeffects.Authority{
		Kind: runtimeeffects.AuthorityChannelNativeSetting, ID: operationID,
		ExecutionOwner: "channel-native:test", LeaseExpiresAt: time.Now().Add(time.Minute), FenceGeneration: 1,
		ExecutionMode: runtimeeffects.ExecutionModeLive,
		ChannelNativeSetting: runtimeeffects.ChannelNativeSettingAuthority{
			EffectOperationID: operationID, SettingID: settingID, SettingGeneration: 1,
			Provider: "telegram", ResourceSlotID: "telegram:bot_webhook:42", ConversationRef: "42",
			PrincipalID: uuid.NewString(), EntryContractHash: "sha256:contract", PackID: "telegram",
			PackVersion: "2", PackManifestHash: "sha256:pack", ActivationID: uuid.NewString(),
			ActivationRevision: 1, BindingRevision: 1, BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64),
			BundleIdentity: "bundle:test@sha256:native", PackInventoryGeneration: "sha256:inventory",
			RuntimeInstanceID: uuid.NewString(), ContextPublicationGeneration: 1,
			PlanGeneration: plan, TargetGeneration: 1,
		},
	}
	ctx := runtimeeffects.WithExecutionMode(context.Background(), runtimeeffects.ExecutionModeLive)
	ctx = runtimeeffects.WithController(ctx, runtimeeffects.NewController(h).WithExecutionPosture(executionposture.Live))
	return runtimeeffects.WithAuthority(ctx, authority)
}

func TestChannelNativeSettingEffectOutcomes(t *testing.T) {
	tool := packfixture.ConnectorTool(t, "telegram", "telegram.install_inbox_commands").Tool
	input := map[string]any{"chat_id": "42", "commands": []any{map[string]any{"command": "inbox", "description": "Open inbox"}}}
	credentials := map[string]any{"telegram_bot_token": "bot-secret"}
	t.Run("exact readback settles", func(t *testing.T) {
		h := &nativeSettingHarness{Harness: effecttest.New()}
		client := &http.Client{Transport: registrationRoundTripFunc(func(*http.Request) (*http.Response, error) {
			if err := h.RequireState("channel_native_setting", runtimeeffects.StateLaunched); err != nil {
				t.Fatal(err)
			}
			return registrationResponse(http.StatusOK, `{"ok":true,"result":true}`), nil
		})}
		result, err := (HTTPExecutor{Client: client}).ApplyChannelNativeSetting(nativeSettingTestContext(h), "telegram.install_inbox_commands", tool, input, credentials, nil)
		if err != nil || !result.Acknowledged || result.Pending == nil {
			t.Fatalf("native setting apply = %#v, %v", result, err)
		}
		if err := h.RequireState("channel_native_setting", runtimeeffects.StateResponseObserved); err != nil {
			t.Fatal(err)
		}
		desired, err := channelnative.DesiredCommands()
		if err != nil {
			t.Fatal(err)
		}
		if err := result.Pending.SettleNativeSettingReadback(context.Background(), desired, nil); err != nil {
			t.Fatal(err)
		}
		if err := h.RequireState("channel_native_setting", runtimeeffects.StateSettled); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("mismatched readback is uncertain", func(t *testing.T) {
		h := &nativeSettingHarness{Harness: effecttest.New()}
		client := &http.Client{Transport: registrationRoundTripFunc(func(*http.Request) (*http.Response, error) {
			return registrationResponse(http.StatusOK, `{"ok":true,"result":true}`), nil
		})}
		result, err := (HTTPExecutor{Client: client}).ApplyChannelNativeSetting(nativeSettingTestContext(h), "telegram.install_inbox_commands", tool, input, credentials, nil)
		if err != nil || result.Pending == nil {
			t.Fatalf("native setting apply = %#v, %v", result, err)
		}
		if err := result.Pending.SettleNativeSettingReadback(context.Background(), []byte(`[]`), nil); err != nil {
			t.Fatal(err)
		}
		if err := h.RequireState("channel_native_setting", runtimeeffects.StateOutcomeUncertain); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("provider rejected install cannot use matching readback", func(t *testing.T) {
		h := &nativeSettingHarness{Harness: effecttest.New()}
		client := &http.Client{Transport: registrationRoundTripFunc(func(*http.Request) (*http.Response, error) {
			return registrationResponse(http.StatusOK, `{"ok":true,"result":false}`), nil
		})}
		result, err := (HTTPExecutor{Client: client}).ApplyChannelNativeSetting(nativeSettingTestContext(h), "telegram.install_inbox_commands", tool, input, credentials, nil)
		if err == nil || result.Acknowledged || result.Pending == nil {
			t.Fatalf("rejected native setting apply = %#v, %v", result, err)
		}
		desired, err := channelnative.DesiredCommands()
		if err != nil {
			t.Fatal(err)
		}
		if err := result.Pending.SettleNativeSettingReadback(context.Background(), desired, nil); err != nil {
			t.Fatal(err)
		}
		if err := h.RequireState("channel_native_setting", runtimeeffects.StateOutcomeUncertain); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("lost acknowledgment is never redispatched", func(t *testing.T) {
		h := &nativeSettingHarness{Harness: effecttest.New()}
		client := &http.Client{Transport: registrationRoundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("transport lost bot-secret")
		})}
		ctx := nativeSettingTestContext(h)
		result, err := (HTTPExecutor{Client: client}).ApplyChannelNativeSetting(ctx, "telegram.install_inbox_commands", tool, input, credentials, nil)
		if err == nil || result.Pending == nil || strings.Contains(err.Error(), "bot-secret") {
			t.Fatalf("lost native setting acknowledgment = %#v, %v", result, err)
		}
		desired, err := channelnative.DesiredCommands()
		if err != nil {
			t.Fatal(err)
		}
		if err := result.Pending.SettleNativeSettingReadback(ctx, desired, nil); err != nil {
			t.Fatal(err)
		}
		if err := h.RequireState("channel_native_setting", runtimeeffects.StateOutcomeUncertain); err != nil {
			t.Fatal(err)
		}
		if _, err := (HTTPExecutor{Client: client}).ApplyChannelNativeSetting(ctx, "telegram.install_inbox_commands", tool, input, credentials, nil); err == nil {
			t.Fatal("launched native setting was redispatched")
		}
	})
}
