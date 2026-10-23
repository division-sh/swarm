package forkrecipient

import "testing"

func TestS1ProviderDirectoryKeysRemainDistinctSelectedRecipientEvidence(t *testing.T) {
	in := nodeInput(t)
	in.HandlerEvent = "inbound.telegram-sales.message"
	first, err := NewLocal(in)
	if err != nil {
		t.Fatal(err)
	}
	in.HandlerEvent = "inbound.telegram_sales.message"
	second, err := NewLocal(in)
	if err != nil {
		t.Fatal(err)
	}
	firstKey, err := first.Key()
	if err != nil {
		t.Fatal(err)
	}
	secondKey, err := second.Key()
	if err != nil {
		t.Fatal(err)
	}
	equal, err := Equal(first, second)
	if err != nil {
		t.Fatal(err)
	}
	if firstKey == (Key{}) || firstKey == secondKey || equal {
		t.Fatal("selected recipient evidence normalized distinct provider keys")
	}
	in.HandlerEvent = "business.telegram-sales.message"
	if _, err := NewLocal(in); err == nil {
		t.Fatal("selected recipient widened ordinary event tokens")
	}
}
