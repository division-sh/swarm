package channelonboarding

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// AccountReference identifies an account in one deployment, never a flow or a
// credential. Phone presentation is normalized before it becomes an identity.
type AccountReference struct {
	Provider string `yaml:"provider" json:"provider"`
	Account  string `yaml:"account" json:"account"`
}

var e164Account = regexp.MustCompile(`^\+[1-9][0-9]{1,14}$`)

func CanonicalAccount(provider, account string) (AccountReference, error) {
	ref := AccountReference{Provider: provider}
	switch provider {
	case "whatsapp":
		ref.Account = strings.NewReplacer(" ", "", "-", "", "(", "", ")", "", ".", "").Replace(strings.TrimSpace(account))
		if !e164Account.MatchString(ref.Account) {
			return AccountReference{}, fmt.Errorf("account must be an international E.164 number beginning with +")
		}
	case "telegram":
		id, err := strconv.ParseUint(account, 10, 64)
		if err != nil || id == 0 || strconv.FormatUint(id, 10) != account {
			return AccountReference{}, fmt.Errorf("account must be the exact Telegram bot ID shown by channel list")
		}
		ref.Account = account
	default:
		return AccountReference{}, fmt.Errorf("provider must be whatsapp or telegram")
	}
	return ref, nil
}

func (r AccountReference) Validate() error {
	canonical, err := CanonicalAccount(r.Provider, r.Account)
	if err == nil && canonical != r {
		return fmt.Errorf("account reference must use its canonical provider/account")
	}
	return err
}
