package storetest

import private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"

func DecisionCardDomain(selected any) private.DecisionCardDomainFixture {
	return private.DecisionCardDomainForTest(selected)
}
