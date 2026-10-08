package store_test

import "testing"

// All families and the ratchet share one complete census of identical live
// inputs within this process; changed source or context requires a fresh scan.
func TestNativeFixtureFamiliesDoNotReceiveRawAuthority(t *testing.T) {
	findings := debtLoadHeadCensus(t, persistenceAuthorityRepoRoot(t))
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
