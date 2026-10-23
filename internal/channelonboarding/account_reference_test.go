package channelonboarding

import "testing"

func TestAccountReferenceCanonicalIdentity(t *testing.T) {
	for _, cell := range []struct{ provider, account, want string }{
		{"whatsapp", "+1 (555) 010-0123", "+15550100123"},
		{"whatsapp", "+15550100124", "+15550100124"},
		{"telegram", "123456789", "123456789"},
	} {
		got, err := CanonicalAccount(cell.provider, cell.account)
		if err != nil || got.Account != cell.want || got.Provider != cell.provider || got.Validate() != nil {
			t.Fatalf("canonical account %q: %+v, %v", cell.provider, got, err)
		}
	}
	for _, cell := range []struct{ provider, account string }{
		{"whatsapp", "15550100123"}, {"whatsapp", "+0123"}, {"whatsapp", "+1"},
		{"whatsapp", "+1234567890123456"}, {"whatsapp", "+123:4"}, {"whatsapp", "+123@server"},
		{"whatsapp", "+123\n456"}, {"whatsapp", "+123/456"},
		{"telegram", "00123"}, {"telegram", "-123"}, {"telegram", "0"}, {"telegram", "@bot"},
		{"telegram", " 123"}, {"WHATSAPP", "+123"}, {" whatsapp", "+123"}, {"other", "+123"},
	} {
		if _, err := CanonicalAccount(cell.provider, cell.account); err == nil {
			t.Fatalf("invalid provider/account accepted: %+v", cell)
		}
	}
	if (AccountReference{Provider: "whatsapp", Account: "+1 234"}).Validate() == nil {
		t.Fatal("noncanonical persisted identity was accepted")
	}
}
