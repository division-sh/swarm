package bus

import "testing"

func TestS1ProviderDirectoryKeySurvivesBusNameAndPatternFacades(t *testing.T) {
	const name = "inbound.telegram-sales.message"
	if !isValidEventTypeName(name) || !routeMatches("inbound.telegram-sales.*", name) || !routeMatches("**/inbound.telegram-sales.*", "child/"+name) {
		t.Fatal("bus facades rejected exact provider directory spelling")
	}
	if routeMatches("inbound.telegram_sales.*", name) || isValidEventTypeName("business.telegram-sales.message") || isValidEventTypeName("inbound.telegram-sales.text-message") {
		t.Fatal("provider spelling broadened or normalized ordinary event identity")
	}
}
