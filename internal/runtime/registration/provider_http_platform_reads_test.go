package registration

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/packs"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
)

func TestTelegramPlatformReadsUseCompiledOperationPolicy(t *testing.T) {
	plan := registrationChannelPlanForTest(t, nil)
	registration, ok := plan.Registration()
	if !ok {
		t.Fatal("missing compiled registration")
	}
	binding, err := packs.NewOutboundBindingPlan("telegram-platform-read-proof", plan, "42", nil)
	if err != nil {
		t.Fatal(err)
	}
	member, err := binding.RestoreOpaqueReference("external_account_reference", "7")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		tool, operation, method, path string
		registration                  bool
		input                         map[string]any
		body, response, projected     string
	}{
		{
			tool: "telegram.identify_bot", operation: packs.RegistrationOperationIdentify, registration: true,
			method: http.MethodGet, path: "/getMe", response: `{"ok":true,"result":{"id":42,"username":"proof_bot"}}`,
			projected: `{"resource_id":"42"}`,
		},
		{
			tool: "telegram.read_webhook", operation: packs.RegistrationOperationReadback, registration: true,
			method: http.MethodGet, path: "/getWebhookInfo", response: `{"ok":true,"result":{"url":"https://hooks.example.test/current"}}`,
			projected: `{"callback_url":"https://hooks.example.test/current"}`,
		},
		{
			tool: "telegram.read_bot_address", operation: "identify_inbox_address", input: map[string]any{},
			method: http.MethodGet, path: "/getMe", response: `{"ok":true,"result":{"id":42,"username":"proof_bot"}}`,
			projected: `{"address_reference":"proof_bot"}`,
		},
		{
			tool: "telegram.read_inbox_commands", operation: "read_inbox_entry", input: map[string]any{"language_code": "en"},
			method: http.MethodPost, path: "/getMyCommands", body: `{"language_code":"en","scope":{"type":"chat","chat_id":"42"}}`,
			response:  `{"ok":true,"result":[{"command":"open","description":"Open inbox"}]}`,
			projected: `{"commands":[{"command":"open","description":"Open inbox"}]}`,
		},
		{
			tool: "telegram.read_shared_inbox_commands", operation: "read_shared_inbox_entry", input: map[string]any{"language_code": "en", "member_reference": member},
			method: http.MethodPost, path: "/getMyCommands", body: `{"language_code":"en","scope":{"type":"chat_member","chat_id":"42","user_id":"7"}}`,
			response:  `{"ok":true,"result":[{"command":"open","description":"Open inbox"}]}`,
			projected: `{"commands":[{"command":"open","description":"Open inbox"}]}`,
		},
		{
			tool: "telegram.read_chat_menu_button", operation: "read_inbox_launcher", input: map[string]any{},
			method: http.MethodPost, path: "/getChatMenuButton", body: `{"chat_id":"42"}`,
			response: `{"ok":true,"result":{"type":"commands"}}`, projected: `{"launcher":"commands"}`,
		},
		{
			tool: "telegram.read_default_menu_button", operation: "read_default_inbox_launcher", input: map[string]any{},
			method: http.MethodPost, path: "/getChatMenuButton", body: `{}`,
			response: `{"ok":true,"result":{"type":"commands"}}`, projected: `{"launcher":"commands"}`,
		},
	} {
		t.Run(tc.tool, func(t *testing.T) {
			h := effecttest.New()
			calls := 0
			executor := HTTPExecutor{Client: &http.Client{Transport: registrationRoundTripFunc(func(request *http.Request) (*http.Response, error) {
				calls++
				if request.Method != tc.method || request.URL.Scheme != "https" || request.URL.Host != "api.telegram.org" || request.URL.Path != "/botbot-secret"+tc.path || request.URL.RawQuery != "" {
					t.Fatalf("wrong compiled provider request: %s %s", request.Method, request.URL)
				}
				var body []byte
				if request.Body != nil {
					body, err = io.ReadAll(request.Body)
					if err != nil {
						t.Fatal(err)
					}
				}
				if tc.body == "" {
					if len(body) != 0 {
						t.Fatalf("read unexpectedly acquired a request body: %s", body)
					}
				} else {
					assertPlatformReadJSON(t, body, tc.body)
				}
				return registrationResponse(http.StatusOK, tc.response), nil
			})}}
			ctx := h.Context(tc.tool)
			credentials := map[string]any{"telegram_bot_token": "bot-secret"}
			var projected map[string]any
			if tc.registration {
				operation, err := registration.Operation(tc.operation)
				if err != nil {
					t.Fatal(err)
				}
				policy, err := operation.EffectClass()
				if err != nil || policy != runtimecontracts.ActivityEffectClassReadOnly || operation.ToolID() != tc.tool {
					t.Fatalf("wrong admitted registration read: %s/%s %v", operation.ToolID(), policy, err)
				}
				input, err := operation.Prepare(map[string]any{})
				if err != nil {
					t.Fatal(err)
				}
				output, err := executor.Read(ctx, operation, input, credentials)
				if err != nil {
					t.Fatal(err)
				}
				projected, err = operation.Project(output)
				if err != nil {
					t.Fatal(err)
				}
			} else {
				policy, err := binding.OperationEffectClass(tc.operation)
				toolID, _, toolErr := binding.ConnectorOperation(tc.operation)
				if err != nil || toolErr != nil || policy != runtimecontracts.ActivityEffectClassReadOnly || toolID != tc.tool {
					t.Fatalf("wrong admitted native read: %s/%s %v %v", toolID, policy, err, toolErr)
				}
				_, input, err := binding.PrepareOperation(tc.operation, tc.input)
				if err != nil {
					t.Fatal(err)
				}
				output, err := executor.ReadChannelOperation(ctx, binding, tc.operation, input, credentials)
				if err != nil {
					t.Fatal(err)
				}
				projected, err = binding.ProjectOperationOutput(tc.operation, output)
				if err != nil {
					t.Fatal(err)
				}
			}
			encoded, err := json.Marshal(projected)
			if err != nil {
				t.Fatal(err)
			}
			assertPlatformReadJSON(t, encoded, tc.projected)
			if calls != 1 || len(h.Attempts) != 0 || len(h.Authorizations) != 0 {
				t.Fatalf("read changed write accounting: calls=%d attempts=%v authorizations=%v", calls, h.Attempts, h.Authorizations)
			}
		})
	}
}

func assertPlatformReadJSON(t testing.TB, actual []byte, expected string) {
	t.Helper()
	var got, want any
	if err := json.Unmarshal(actual, &got); err != nil {
		t.Fatalf("invalid actual JSON %s: %v", actual, err)
	}
	if err := json.Unmarshal([]byte(expected), &want); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("compiled read projection differs: got=%s want=%s", actual, expected)
	}
}

func TestProviderPlatformOperationRefusesWrongSlotBeforeDispatch(t *testing.T) {
	plan := registrationChannelPlanForTest(t, nil)
	registration, ok := plan.Registration()
	if !ok {
		t.Fatal("missing compiled registration")
	}
	binding, err := packs.NewOutboundBindingPlan("telegram-platform-refusal-proof", plan, "42", nil)
	if err != nil {
		t.Fatal(err)
	}
	identify, err := registration.Operation(packs.RegistrationOperationIdentify)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		call func(HTTPExecutor) error
	}{
		{"missing_read_slot", func(executor HTTPExecutor) error {
			_, err := executor.Read(context.Background(), packs.RegistrationOperationPlan{}, nil, nil)
			return err
		}},
		{"missing_apply_slot", func(executor HTTPExecutor) error {
			_, err := executor.Apply(context.Background(), packs.RegistrationOperationPlan{}, nil, nil, nil)
			return err
		}},
		{"read_slot_as_apply", func(executor HTTPExecutor) error {
			_, err := executor.Apply(context.Background(), identify, nil, nil, nil)
			return err
		}},
		{"missing_native_plan", func(executor HTTPExecutor) error {
			_, err := executor.ReadChannelOperation(context.Background(), packs.OutboundBindingPlan{}, "read_inbox_entry", nil, nil)
			return err
		}},
		{"native_write_as_read", func(executor HTTPExecutor) error {
			_, err := executor.ReadChannelOperation(context.Background(), binding, "install_inbox_entry", nil, nil)
			return err
		}},
		{"unknown_native_operation", func(executor HTTPExecutor) error {
			_, err := executor.ReadChannelOperation(context.Background(), binding, "absent_operation", nil, nil)
			return err
		}},
		{"missing_read_credentials", func(executor HTTPExecutor) error {
			_, err := executor.Read(context.Background(), identify, map[string]any{}, map[string]any{})
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			executor := HTTPExecutor{Client: &http.Client{Transport: registrationRoundTripFunc(func(*http.Request) (*http.Response, error) {
				calls++
				return registrationResponse(http.StatusOK, `{"ok":true,"result":true}`), nil
			})}}
			if err := tc.call(executor); err == nil || calls != 0 {
				t.Fatalf("wrong platform slot dispatched: calls=%d err=%v", calls, err)
			}
		})
	}
}
