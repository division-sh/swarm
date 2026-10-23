package runtimepersistence

func replyFixtureRunOwnerKey(parameter string, postgres bool) string {
	if postgres {
		parameter += "::uuid"
	}
	return "'run:' || CAST(" + parameter + " AS TEXT)"
}
