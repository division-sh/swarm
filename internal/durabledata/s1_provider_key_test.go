package durabledata

import "testing"

func TestS1ProviderDirectoryKeysRemainDistinctDeclarationReferences(t *testing.T) {
	first, err := ParseDeclarationRef("child", "inbound.telegram-sales.message")
	if err != nil {
		t.Fatal(err)
	}
	second, err := ParseDeclarationRef("child", "inbound.telegram_sales.message")
	if err != nil {
		t.Fatal(err)
	}
	if first.EventName != "inbound.telegram-sales.message" || first.FlowPath != "child" || first.Key() == "" || first.Key() == second.Key() || CompareDeclarationRef(first, second) == 0 {
		t.Fatal("declaration reference normalized distinct provider keys")
	}
}
