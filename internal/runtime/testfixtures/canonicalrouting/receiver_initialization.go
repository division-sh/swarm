package canonicalrouting

import (
	"path/filepath"
	"testing"
)

// CopyReceiverInitialization exercises typed creation values independently of
// the business key used to select the receiver.
func CopyReceiverInitialization(t testing.TB) string {
	t.Helper()
	root := CopyTemplateSelectResolution(t, TemplateSelectResolutionOptions{Mode: SelectResolutionSelectOrCreate})
	applyClosedReplacement(t, filepath.Join(root, "account/entities.yaml"), "  account_id:", "  count: integer?\n  label: text?\n  ratio: numeric?\n  active: boolean?\n  attributes: json?\n  account_id:")
	for _, event := range []string{"account.setup", "account.ready"} {
		applyClosedReplacement(t, filepath.Join(root, "producer/events.yaml"), event+":\n  key: account_id\n  account_id: text\n", event+`:
  key: account_id
  account_id: text
  count: integer?
  label: text
  ratio: numeric?
  active: boolean?
  attributes: json?
`)
	}
	return root
}
