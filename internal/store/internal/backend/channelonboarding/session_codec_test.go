package channelonboarding

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/google/uuid"
)

func TestRetainedSessionAccountCodecRefusesCorruptEvidence(t *testing.T) {
	account := operatorchannel.SessionAccountAdmission{Provider: "whatsapp", ConnectionID: uuid.NewString(),
		AccountRef: "15551234567@s.whatsapp.net", AdmissionID: uuid.NewString(), Revision: 1}
	encoded, err := marshalSessionAccount(account)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := unmarshalSessionAccount(sql.NullString{Valid: true, String: encoded.(string)})
	if err != nil || decoded != account {
		t.Fatalf("retained account round trip: %+v %v", decoded, err)
	}
	absent, err := unmarshalSessionAccount(sql.NullString{})
	if err != nil || absent != (operatorchannel.SessionAccountAdmission{}) {
		t.Fatalf("absent account: %+v %v", absent, err)
	}
	for _, raw := range []string{"", "null", "{}", "[]", `"account"`, encoded.(string) + "{}", encoded.(string) + " ",
		strings.TrimSuffix(encoded.(string), "}") + `,"provider":"whatsapp"}`,
		strings.Replace(encoded.(string), `"admission_revision":1`, `"admission_revision":0`, 1),
		strings.TrimSuffix(encoded.(string), "}") + `,"unknown":true}`} {
		if _, err := unmarshalSessionAccount(sql.NullString{Valid: true, String: raw}); err == nil {
			t.Fatalf("corrupt account accepted: %q", raw)
		}
	}
}
