package channelonboarding

import (
	"fmt"
	"strconv"
	"strings"
)

// AccountReference identifies an account in one deployment, never a flow or a
// credential. Phone presentation is normalized before it becomes an identity.
type AccountReference struct {
	Provider string `yaml:"provider" json:"provider"`
	Account  string `yaml:"account" json:"account"`
}

func CanonicalAccount(provider, account string) (AccountReference, error) {
	ref := AccountReference{Provider: provider}
	switch provider {
	case "whatsapp":
		var digits strings.Builder
		for _, c := range strings.TrimSpace(account) {
			switch {
			case c >= '0' && c <= '9':
				digits.WriteRune(c)
			case c == '+' && digits.Len() == 0:
				digits.WriteRune(c)
			case c == ' ' || c == '-' || c == '(' || c == ')' || c == '.':
			default:
				return ref, fmt.Errorf("account must be an international E.164 number beginning with +")
			}
		}
		ref.Account = digits.String()
		if len(ref.Account) < 3 || len(ref.Account) > 16 || ref.Account[0] != '+' || ref.Account[1] == '0' {
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
	if err != nil {
		return err
	}
	if canonical != r {
		return fmt.Errorf("account reference must use its canonical provider/account")
	}
	return nil
}
