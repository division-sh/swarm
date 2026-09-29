package serveapp

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/runtime/channelnative"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestChannelNativeLocaleQualificationPublicJourney(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, language := range []string{"en", "fr"} {
			t.Run(string(backend)+"/"+language, func(t *testing.T) {
				h := newChannelOnboardingE2EHarness(t, backend, true)
				h.start(t)
				defer h.stop(t)
				begun := startChannelOnboardingRPC(t, h, channelonboarding.VerbConnect, "locale-token", nil)
				claimed := claimPendingResetRPC(t, h, begun, 824100)
				var confirmed map[string]any
				requireServedJSONRPCResult(t, h.rpcEndpoint(), "channel.confirm", map[string]any{
					"operation_id": claimed.OperationID, "expected_revision": claimed.Revision, "approve": true,
				}, &confirmed)
				completed := retryChannelOnboardingRPC(t, h, begun.Operation.OperationID, "")
				if completed.Operation.Phase != channelonboarding.PhaseSucceeded || completed.Operation.ClientLanguage != "" {
					t.Fatalf("connection invented a locale: %#v", completed)
				}
				waitNativeQualification(t, h, completed.Operation.OperationID, channelnative.QualificationMissing, time.Time{})
				if len(h.provider.CommandWrites()) != 0 {
					t.Fatal("missing declaration installed native commands")
				}
				var declared channelonboarding.Result
				requireServedJSONRPCResult(t, h.rpcEndpoint(), "channel.onboarding_retry", map[string]any{
					"operation_id": completed.Operation.OperationID, "client_language": language,
					"expected_locale_revision": completed.Operation.ClientLocaleRevision,
				}, &declared)
				if declared.Operation.ClientLocaleRevision != 2 || declared.Operation.Revision != completed.Operation.Revision {
					t.Fatalf("locale declaration changed connection revision: %#v", declared)
				}
				initial := waitNativeQualification(t, h, completed.Operation.OperationID, channelnative.QualificationQualified, time.Time{})
				writes := h.provider.CommandWrites()
				if len(writes) != 1 {
					t.Fatalf("installation writes=%v", writes)
				}
				body, err := json.Marshal(writes[0]["commands"])
				if err != nil {
					t.Fatal(err)
				}
				var exact []map[string]any
				if err := json.Unmarshal(body, &exact); err != nil || len(exact) != 1 {
					t.Fatalf("exact native commands %q: %v", body, err)
				}
				foreign := []map[string]any{{"command": "foreign", "description": "Foreign owner"}}
				scope := map[string]any{"type": "chat", "chat_id": "9593"}
				seed := func(locale string, entries []map[string]any) {
					t.Helper()
					if err := h.provider.SeedCommands("locale-token", scope, locale, entries); err != nil {
						t.Fatal(err)
					}
				}
				for _, test := range []struct {
					name   string
					mutate func()
					state  channelnative.QualificationState
				}{
					{"selected locale beats foreign fallback", func() { seed(language, exact); seed("", foreign) }, channelnative.QualificationQualified},
					{"higher precedence foreign commands", func() { seed("", exact); seed(language, foreign) }, channelnative.QualificationInvalid},
					{"selected absent uses exact fallback", func() { seed(language, nil) }, channelnative.QualificationQualified},
					{"chat launcher conflict", func() { requireNativeLauncherFixture(t, h, "locale-token", "9593", "web_app") }, channelnative.QualificationInvalid},
					{"chat launcher beats foreign default", func() {
						requireNativeLauncherFixture(t, h, "locale-token", "9593", "commands")
						requireNativeLauncherFixture(t, h, "locale-token", "", "web_app")
					}, channelnative.QualificationQualified},
					{"inherited launcher conflict", func() { requireNativeLauncherFixture(t, h, "locale-token", "9593", "default") }, channelnative.QualificationInvalid},
					{"provider default command launcher", func() { requireNativeLauncherFixture(t, h, "locale-token", "", "default") }, channelnative.QualificationQualified},
				} {
					t.Run(test.name, func(t *testing.T) {
						cut := time.Now().UTC()
						test.mutate()
						current := waitNativeQualification(t, h, completed.Operation.OperationID, test.state, cut)
						if current.SettingID != initial.SettingID || current.SettingGeneration != initial.SettingGeneration || len(h.provider.CommandWrites()) != 1 {
							t.Fatal("qualification replayed installation or borrowed another generation")
						}
					})
				}
				other := "fr"
				if language == other {
					other = "en"
				}
				seed(other, foreign)
				requireServedJSONRPCResult(t, h.rpcEndpoint(), "channel.onboarding_retry", map[string]any{
					"operation_id": completed.Operation.OperationID, "client_language": other,
					"expected_locale_revision": declared.Operation.ClientLocaleRevision,
				}, &declared)
				waitNativeQualification(t, h, completed.Operation.OperationID, channelnative.QualificationInvalid, time.Time{})
				cut := time.Now().UTC()
				seed(other, nil)
				waitNativeQualification(t, h, completed.Operation.OperationID, channelnative.QualificationQualified, cut)
				h.stop(t)
				seed(other, foreign)
				h.start(t)
				waitNativeQualification(t, h, completed.Operation.OperationID, channelnative.QualificationInvalid, time.Time{})
				cut = time.Now().UTC()
				seed(other, nil)
				waitNativeQualification(t, h, completed.Operation.OperationID, channelnative.QualificationQualified, cut)
				if len(h.provider.CommandWrites()) != 1 {
					t.Fatal("locale change/restart reinstalled acknowledged command generation")
				}
			})
		}
	}
}

func TestChannelNativePreinstallConflictsPublicJourney(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, conflict := range []string{"selected language", "fallback", "chat launcher", "default launcher", "shared member language"} {
			t.Run(string(backend)+"/"+conflict, func(t *testing.T) {
				h := newChannelOnboardingE2EHarness(t, backend, true)
				chat, kind := int64(9593), "private"
				scope := map[string]any{"type": "chat", "chat_id": "9593"}
				if conflict == "shared member language" {
					chat, kind = -9593, "group"
					scope = map[string]any{"type": "chat_member", "chat_id": "-9593", "user_id": "8593"}
				}
				foreign := []map[string]any{{"command": "foreign", "description": "Foreign owner"}}
				language := "fr"
				switch conflict {
				case "fallback":
					language = ""
				case "chat launcher":
					requireNativeLauncherFixture(t, h, "locale-token", "9593", "web_app")
				case "default launcher":
					requireNativeLauncherFixture(t, h, "locale-token", "", "web_app")
				}
				if conflict != "chat launcher" && conflict != "default launcher" {
					if err := h.provider.SeedCommands("locale-token", scope, language, foreign); err != nil {
						t.Fatal(err)
					}
				}
				h.start(t)
				defer h.stop(t)
				begun := startChannelOnboardingRPC(t, h, channelonboarding.VerbConnect, "locale-token", map[string]string{"client_language": "fr"})
				callback, signing, _ := h.provider.Registration()
				requireChannelClaimDisposition(t, "locale claimant", submitChannelOnboardingClaimWithChatType(t,
					callback, signing, begun.IdentityOperation.Challenge, 824200, 8593, chat, kind, "locale_operator"), "consumed_by_binding")
				claimed := getChannelOnboardingRPC(t, h, begun.Operation.OperationID)
				var confirmation map[string]any
				requireServedJSONRPCResult(t, h.rpcEndpoint(), "channel.confirm", map[string]any{
					"operation_id": claimed.IdentityOperation.OperationID, "expected_revision": claimed.IdentityOperation.Revision, "approve": true,
				}, &confirmation)
				completed := retryChannelOnboardingRPC(t, h, begun.Operation.OperationID, "")
				invalid := waitNativeQualification(t, h, completed.Operation.OperationID, channelnative.QualificationInvalid, time.Time{})
				if invalid.ClientLanguage != "fr" || invalid.Reason == "" || len(h.provider.CommandWrites()) != 0 {
					t.Fatalf("preinstall conflict acquired native authority: %#v writes=%v", invalid, h.provider.CommandWrites())
				}
				cut := time.Now().UTC()
				if err := h.provider.SeedCommands("locale-token", scope, language, nil); err != nil {
					t.Fatal(err)
				}
				requireNativeLauncherFixture(t, h, "locale-token", "9593", "default")
				requireNativeLauncherFixture(t, h, "locale-token", "", "default")
				qualified := waitNativeQualification(t, h, completed.Operation.OperationID, channelnative.QualificationQualified, cut)
				if qualified.SettingID != invalid.SettingID || qualified.SettingGeneration != invalid.SettingGeneration || len(h.provider.CommandWrites()) != 1 {
					t.Fatal("repairing external conflict created another setting or replayed installation")
				}
			})
		}
	}
}

func waitNativeQualification(t *testing.T, h *channelOnboardingE2EHarness, operationID string, state channelnative.QualificationState, after time.Time) channelnative.Qualification {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		result := getChannelOnboardingRPC(t, h, operationID)
		if result.Readiness != nil && result.Readiness.NativeInbox != nil {
			qualification := *result.Readiness.NativeInbox
			if qualification.State == state && (after.IsZero() || qualification.ObservedAt.After(after)) {
				return qualification
			}
		}
		if time.Now().After(deadline) {
			readiness, _ := json.Marshal(result.Readiness)
			t.Fatalf("qualification did not reach %s after %s: readiness=%s operation=%#v\n%s", state, after, readiness, result.Operation, h.process.outputString())
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func requireNativeLauncherFixture(t *testing.T, h *channelOnboardingE2EHarness, credential, chat, launcher string) {
	t.Helper()
	if err := h.provider.SeedLauncher(credential, chat, launcher); err != nil {
		t.Fatal(fmt.Errorf("seed launcher: %w", err))
	}
}
