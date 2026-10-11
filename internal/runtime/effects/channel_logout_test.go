package effects

import (
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestChannelLogoutJournalIdentityIsExactAndStable(t *testing.T) {
	id := "aabbeecc-0011-4222-8333-abcdef001234"
	one, err := ChannelLogoutOperationID(id)
	if err != nil {
		t.Fatal(err)
	}
	two, err := ChannelLogoutOperationID(id)
	if err != nil || two != one || one == id {
		t.Fatal("teardown replay changed or reused its journal identity", err)
	}
	other, err := ChannelLogoutOperationID(uuid.NewString())
	if err != nil || other == one {
		t.Fatal("distinct teardown adopted another journal operation", err)
	}
	for _, invalid := range []string{"", "unknown", uuid.Nil.String(), strings.ToUpper(id), " " + id, id + " "} {
		if _, err := ChannelLogoutOperationID(invalid); err == nil {
			t.Fatalf("non-exact responsibility accepted: %q", invalid)
		}
	}
}

func TestChannelLogoutRegistrationIsClosed(t *testing.T) {
	r, ok := RegistrationFor("channel_logout_whatsapp")
	if !ok || r.Kind != Kind("channel_logout") || r.Transport != "in_process" || r.Class != EffectWriteOrUnknown ||
		r.PrelaunchFailure != StateTerminalFailure || r.PostlaunchFailure != StateOutcomeUncertain {
		t.Fatal("explicit logout has no distinct, fail-closed journal registration")
	}
}
