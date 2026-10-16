package main

import (
	"encoding/json"
	"fmt"
	"os/exec"
)

// Decision 8, #2698/6205327685: preserve the complete reviewed change and its
// output, then authenticate the three exact native-fixture source successors.
// This is not formatter accounting and does not rewrite finite-fixtures.json.
const finiteNativeRepairBefore = "56c8969ea64d2bc0b90f80eff47bce676bf0a26c"
const finiteNativeRepairAfter = "f5c57acafdae2cd0750f493c15ca667ca3d84e33"

type finiteNativeRepairPin struct{ Change, Before, After string }

var finiteNativeRepairPins = map[string]finiteNativeRepairPin{
	"internal/runtime/conformance/data_text_file_journey_2456_test.go": {
		"3e2e939c873c9ba2eda530251ebcf568a76a9d109388fcdb66e208caeb5c287e",
		"39785b6de6159b06c974cc36790de6067994476159b99fc4ba84f29f3b694b7f",
		"a78c04cf39c93f88423b1e82bfac5e5efa22216a42ede3318fc9ebedb7db2dde",
	},
	"internal/runtime/conformance/data_text_file_support_2456_test.go": {
		"1dc35945b141eb6eb1ae2cafb5d5f2bd338985becb19474ceccca1a6c1e44ca9",
		"1f0e8b070cc1c3a3340f1f3041a4433b61abd7947edb3f19f278c1ea40e42bbf",
		"2e11f6c2b6eaca7887aa974a333e1123a902411523823cd61e9cdf8970be84a7",
	},
	"internal/runtime/conformance/fan_out_resource_source_proof_test.go": {
		"67956d0a476a76b8015882e0eef533bc7653826e45a7e56259793d1bea139c77",
		"7cfb6a0a913381e41536b3ffe18d2b9a40b7512227369e2e52f0657b59a0f2f4",
		"39724d9a724d722d778d2933b9a043d62ded139a3d6c83b5219d2e98bc4f3b59",
	},
}

func finiteNativeRepairChange(c change) (finiteNativeRepairPin, bool, error) {
	pin, listed := finiteNativeRepairPins[c.File]
	if !listed {
		return pin, false, nil
	}
	raw, err := json.Marshal(c)
	if err != nil || digest(raw) != pin.Change || c.AfterHash != pin.Before {
		return pin, true, fmt.Errorf("native fixture repair changed its complete reviewed ledger row: %s: %v", c.File, err)
	}
	return pin, true, nil
}

func finiteNativeRepairOutput(root string, c change, historical []byte) ([]byte, bool, error) {
	pin, listed, err := finiteNativeRepairChange(c)
	if err != nil || !listed {
		return nil, listed, err
	}
	if digest(historical) != pin.Before {
		return nil, true, fmt.Errorf("native fixture repair lost its reviewed output: %s", c.File)
	}
	for _, cut := range [][2]string{{finiteNativeRepairBefore, pin.Before}, {finiteNativeRepairAfter, pin.After}} {
		raw, err := exec.Command("git", "-C", root, "show", cut[0]+":"+c.File).Output()
		if err != nil || digest(raw) != cut[1] {
			return nil, true, fmt.Errorf("native fixture repair lost its exact source cut: %s:%s: %v", cut[0], c.File, err)
		}
		if cut[0] == finiteNativeRepairAfter {
			return raw, true, nil
		}
	}
	panic("unreachable finite source pair")
}

func finiteNativeRepairSource(root string, c change, actual []byte) ([]byte, bool, error) {
	pin, listed, err := finiteNativeRepairChange(c)
	if err != nil || !listed {
		return nil, listed, err
	}
	if digest(actual) == pin.After {
		// Check both immutable cuts even when replaying the current endpoint.
		old, err := exec.Command("git", "-C", root, "show", finiteNativeRepairBefore+":"+c.File).Output()
		if err != nil {
			return nil, true, err
		}
		return finiteNativeRepairOutput(root, c, old)
	}
	historical, err := rewrite(c, actual)
	if err != nil {
		return nil, true, err
	}
	return finiteNativeRepairOutput(root, c, historical)
}
