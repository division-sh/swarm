package bus

import "testing"

func TestS1ProviderDirectoryKeySurvivesBusNameAndPatternFacades(t *testing.T) {
	const name = "inbound.telegram-sales.message"
	if !IsValidEventTypeName(name) || !RouteMatches("inbound.telegram-sales.*", name) || !RouteMatches("**/inbound.telegram-sales.*", "child/"+name) {
		t.Fatal("bus facades rejected exact provider directory spelling")
	}
	if RouteMatches("inbound.telegram_sales.*", name) || IsValidEventTypeName("business.telegram-sales.message") || IsValidEventTypeName("inbound.telegram-sales.text-message") {
		t.Fatal("provider spelling broadened or normalized ordinary event identity")
	}
}
