package apiv1

import (
	"errors"
	"fmt"
	"testing"

	"github.com/division-sh/swarm/internal/operatorchannel"
)

func TestChannelOnboardingUsesExistingIdentityConflictProjection(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		code string
	}{
		{"revision", operatorchannel.ErrRevisionConflict, ChannelRevisionConflictCode},
		{"wrapped_revision", fmt.Errorf("identity child: %w", operatorchannel.ErrRevisionConflict), ChannelRevisionConflictCode},
		{"binding", operatorchannel.ErrConflict, ChannelBindingConflictCode},
		{"ambiguous", fmt.Errorf("%w: exact interface is ambiguous", operatorchannel.ErrConflict), ChannelInterfaceAmbiguousCode},
	} {
		t.Run(test.name, func(t *testing.T) {
			var application *ApplicationError
			if mapped := channelOnboardingError(test.err); !errors.As(mapped, &application) || application.Code != test.code {
				t.Fatal("onboarding identity conflict became an internal failure", mapped)
			}
		})
	}
	unknown := errors.New("identity observation failed")
	for _, err := range []error{unknown, errors.Join(unknown, operatorchannel.ErrRevisionConflict), errors.Join(unknown, operatorchannel.ErrConflict)} {
		if mapped := channelOnboardingError(err); mapped != err {
			t.Fatal("typed identity conflict hid an independent failure", mapped)
		}
	}
}
