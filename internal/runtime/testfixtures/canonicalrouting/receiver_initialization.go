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
	applyClosedReplacement(t, filepath.Join(root, "account/schema.yaml"), "mode: template\n", `mode: template
instance_variables:
  variables:
    count: {type: integer, default: 3}
    label: text
    ratio: {type: numeric, default: 2.0}
    active: boolean
    attributes: json
`)
	applyClosedReplacement(t, filepath.Join(root, "account/schema.yaml"), "          mode: select-or-create\n", `          mode: select-or-create
        initialize:
          count: payload.count
          label: payload.label
          ratio: payload.ratio
          active: payload.active
          attributes: payload.attributes
`)
	applyClosedReplacement(t, filepath.Join(root, "producer/events.yaml"), "account.ready:\n  key: account_id\n  account_id: text\n", `account.ready:
  key: account_id
  account_id: text
  count: integer?
  label: text?
  ratio: numeric?
  active: boolean?
  attributes: json?
`)
	return root
}
