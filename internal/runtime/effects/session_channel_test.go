package effects

import "testing"

func TestSessionChannelEffectsKeepExistingKindsAndUncertainty(t *testing.T) {
	for _, row := range []struct {
		adapter string
		kind    Kind
	}{
		{"channel_confirmation_whatsapp", KindChannelConfirmation},
		{"channel_delivery_whatsapp", KindChannelDelivery},
	} {
		t.Run(row.adapter, func(t *testing.T) {
			registration, ok := RegistrationFor(row.adapter)
			if !ok || registration.Kind != row.kind || registration.Transport != "in_process" ||
				registration.Class != EffectWriteOrUnknown || registration.PostlaunchFailure != StateOutcomeUncertain ||
				len(registration.PrimitiveKeys) != 1 {
				t.Fatalf("native channel effect lacks its existing journal contract: %+v", registration)
			}
		})
	}
}
