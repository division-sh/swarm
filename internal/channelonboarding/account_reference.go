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
	canonical, err := binding.CanonicalAccount(account)
	if err != nil {
		return AccountReference{}, err
	}
	return AccountReference{Provider: provider, Account: canonical}, nil
}

func (r AccountReference) Validate() error {
	canonical, err := CanonicalAccount(r.Provider, r.Account)
	if err == nil && canonical != r {
		return fmt.Errorf("account reference must use its canonical provider/account")
	}
	return err
}
