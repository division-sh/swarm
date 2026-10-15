package runscalar

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
)

const Path = "tools/fixture-codemod/pipeline-observations/family145-run-scalar-preimages.json"
const Digest = "441c152d124c3f218fe1577a3962b8b7bc153919bec0121ad89a48a904dfddca"
const Predecessor = "c42d7ac7c3b3aa07affa1489021c1ab3e2685849"

type Pin struct {
	File, Function, Before, After, SHA256 string
	Calls                                 int
}

func Decode(raw []byte) ([]Pin, error) {
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != Digest {
		return nil, fmt.Errorf("scalar complete source manifest changed")
	}
	var pins []Pin
	if err := json.Unmarshal(raw, &pins); err != nil {
		return nil, err
	}
	if len(pins) != 6 {
		return nil, fmt.Errorf("scalar declaration inventory changed")
	}
	return pins, nil
}

func Previous(pins []Pin, file, function, current string) string {
	for _, pin := range pins {
		if pin.File == file && pin.Function == function && pin.After == current {
			return pin.Before
		}
	}
	return current
}
