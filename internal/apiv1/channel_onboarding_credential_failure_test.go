package apiv1

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

func TestChannelOnboardingUnusableCredentialFailureProjection(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"direct", credentials.ErrCredentialValueUnusable},
		{"wrapped", fmt.Errorf("private receipt and seal diagnostics: %w", credentials.ErrCredentialValueUnusable)},
		{"joined_same_class", errors.Join(credentials.ErrCredentialValueUnusable, fmt.Errorf("private: %w", credentials.ErrCredentialValueUnusable))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mapped := channelOnboardingError(tc.err)
			if !errors.Is(mapped, credentials.ErrCredentialValueUnusable) {
				t.Fatalf("typed cause lost: %v", mapped)
			}
			failure, ok := runtimefailures.EnvelopeFromError(mapped)
			if !ok || failure.Class != runtimefailures.ClassAuthenticationNeeded || failure.Detail.Code != "credential_value_unusable" || failure.Detail.Attributes["auth_kind"] != "channel_credential" {
				t.Fatalf("failure = %#v, ok=%t", failure, ok)
			}
			if failure.Retryable || !failure.Deterministic {
				t.Fatalf("unusable credential grants automatic retry: %#v", failure)
			}
			raw, err := json.Marshal(failure)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(raw), "private") || strings.Contains(mapped.Error(), "private") {
				t.Fatalf("wrapped private diagnostics leaked: %s / %v", raw, mapped)
			}
		})
	}
}

func TestChannelOnboardingCredentialProjectionPreservesErrorPrecedence(t *testing.T) {
	ioErr := errors.New("credential tier I/O failure")
	for _, err := range []error{
		ioErr,
		errors.Join(&channelonboarding.CredentialRequiredError{OperationID: "operation", Role: "provider", StoreKey: "provider"}, ioErr),
		errors.Join(&channelonboarding.CredentialRequiredError{OperationID: "operation", Role: "provider", StoreKey: "provider"}, channelonboarding.ErrRevisionConflict),
		errors.Join(credentials.ErrCredentialValueUnusable, ioErr),
		errors.Join(credentials.ErrCredentialValueUnusable, channelonboarding.ErrRevisionConflict),
		errors.Join(credentials.ErrCredentialValueUnusable, &channelonboarding.CredentialRequiredError{OperationID: "operation", Role: "provider", StoreKey: "provider"}),
	} {
		mapped := channelOnboardingError(err)
		if failure, ok := runtimefailures.EnvelopeFromError(mapped); ok && failure.Detail.Code == "credential_value_unusable" {
			t.Fatalf("unusable credential hid another failure: %v -> %#v", err, failure)
		}
		if mapped != err {
			t.Fatalf("mixed/I/O error was replaced: %v -> %v", err, mapped)
		}
	}
}

func TestChannelOnboardingMissingCredentialApplicationProjection(t *testing.T) {
	provider := &channelonboarding.CredentialRequiredError{OperationID: "operation", Role: "provider", StoreKey: "provider"}
	signing := &channelonboarding.CredentialRequiredError{OperationID: "operation", Role: "signing", StoreKey: "signing"}
	for _, err := range []error{provider, fmt.Errorf("admission: %w", provider), errors.Join(provider, signing)} {
		mapped := channelOnboardingError(err)
		var app *ApplicationError
		if !errors.As(mapped, &app) || app.Code != ChannelCredentialRequiredCode {
			t.Fatalf("pure missing-credential meaning lost: %v -> %v", err, mapped)
		}
	}
}
