package store_test

import "testing"

// Each family consumes one fresh, complete checkout census. There is no cached
// source, filtered inventory or repeated type-load per assertion family.
func TestNativeFixtureFamiliesDoNotReceiveRawAuthority(t *testing.T) {
	findings := debtLoadPersistenceAuthorityFindings(t, persistenceAuthorityRepoRoot(t))
	for _, family := range []struct {
		name   string
		verify func(*testing.T, []authorityFinding)
	}{
		{"TestInboundSetupSeedDoesNotReceiveRawAuthority", verifyInboundSetupSeedDoesNotReceiveRawAuthority},
		{"TestNativeActivitySetupDoesNotReceiveRawAuthority", verifyNativeActivitySetupDoesNotReceiveRawAuthority},
		{"TestNativeChannelTerminalFixturesDoNotReceiveRawAuthority", verifyNativeChannelTerminalFixturesDoNotReceiveRawAuthority},
		{"TestNativeJournalFixturesDoNotReceiveRawAuthority", verifyNativeJournalFixturesDoNotReceiveRawAuthority},
		{"TestNativeLoopClaimFixturesDoNotReceiveRawAuthority", verifyNativeLoopClaimFixturesDoNotReceiveRawAuthority},
		{"TestNativeMockFixturesDoNotReceiveRawAuthority", verifyNativeMockFixturesDoNotReceiveRawAuthority},
		{"TestNativeAPIReadSetupDoesNotReceiveRawAuthority", verifyNativeAPIReadSetupDoesNotReceiveRawAuthority},
	} {
		t.Run(family.name, func(t *testing.T) {
			family.verify(t, findings)
		})
	}
}
