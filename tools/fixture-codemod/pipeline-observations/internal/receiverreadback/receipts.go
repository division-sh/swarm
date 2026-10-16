package receiverreadback

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
)

const Path = "tools/fixture-codemod/pipeline-observations/family145-receiver-public-readback-preimages.json"
const Digest = "a75db13f04646436e22522c83c9a04d929d12e384d9b48bfa585fe5bdf499709"

type Pin struct {
	File, Function, Before, After, SHA256 string
	Calls                                 int
	Removed                               bool
	Source                                string
}

func Decode(raw []byte) ([]Pin, error) {
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != Digest {
		return nil, fmt.Errorf("complete receiver readback source manifest changed")
	}
	var pins []Pin
	if err := json.Unmarshal(raw, &pins); err != nil {
		return nil, err
	}
	if len(pins) != 15 {
		return nil, fmt.Errorf("receiver readback declaration inventory changed")
	}
	return pins, nil
}

// Earlier function-specific proofs already pin the containing file. Compose
// only the full exact successor; neither partial nor retired bodies normalize.
func Previous(pins []Pin, function, current string) string {
	for _, pin := range pins {
		if !pin.Removed && pin.Function == function && pin.After == current {
			return pin.Before
		}
	}
	return current
}
