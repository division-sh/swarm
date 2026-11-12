package channelonboarding

import (
	"fmt"

	"github.com/division-sh/swarm/internal/channelprovider"
)

// AccountReference identifies an account in one deployment, never a flow or a
// credential. Phone presentation is normalized before it becomes an identity.
type AccountReference struct {
	Provider string `yaml:"provider" json:"provider"`
	Account  string `yaml:"account" json:"account"`
}

func CanonicalAccount(provider, account string) (AccountReference, error) {
	binding, _ := channelprovider.Lookup(provider)
	return canonicalAccountWithFacts(provider, account, binding)
}

func canonicalAccountWithFacts(provider, account string, binding channelprovider.Binding) (AccountReference, error) {
	if !binding.MatchesProvider(provider) {
		binding = channelprovider.Binding{}
	}
	canonical, err := binding.CanonicalAccount(account)
	if err != nil {
		return AccountReference{}, err
	}
	return AccountReference{Provider: provider, Account: canonical}, nil
}

func (r AccountReference) Validate() error {
	binding, _ := channelprovider.Lookup(r.Provider)
	return r.ValidateWithFacts(binding)
}

func (r AccountReference) ValidateWithFacts(binding channelprovider.Binding) error {
	canonical, err := canonicalAccountWithFacts(r.Provider, r.Account, binding)
	if err == nil && canonical != r {
		return fmt.Errorf("account reference must use its canonical provider/account")
	}
	return err
}
