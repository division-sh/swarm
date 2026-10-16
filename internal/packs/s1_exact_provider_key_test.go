package packs

import (
	"strings"
	"testing"
)

func TestS1TriggerSubjectRetainsExactPortableProviderKey(t *testing.T) {
	for _, key := range []string{"telegram-sales", "telegram_sales", "_team", "con", "Telegram", "telegram.bot", " acme", "acme ", strings.Repeat("a", 101)} {
		subject := Subject{ID: "test.trigger", Kind: SubjectProviderTrigger, Provider: key, Source: "trigger_pack", Applicability: "installed"}
		got, err := NormalizeSubjects([]Subject{subject})
		valid := key == "telegram-sales" || key == "telegram_sales"
		if !valid {
			if err == nil {
				t.Errorf("nonportable subject provider %q admitted: %+v", key, got)
			}
			if id, err := IngressSubjectID("bundle", ".", key); err == nil {
				t.Errorf("nonportable subject provider %q gained id %q", key, id)
			}
			continue
		}
		if err != nil || len(got) != 1 || got[0].Provider != key {
			t.Fatalf("portable subject key changed: %+v err=%v", got, err)
		}
		if id, err := IngressSubjectID("bundle", ".", key); err != nil || id != "ingress:bundle:.:"+key {
			t.Errorf("subject id lost exact key: %q err=%v", id, err)
		}
	}
}
