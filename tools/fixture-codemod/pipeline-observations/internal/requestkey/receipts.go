package requestkey

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
)

const Path = "tools/fixture-codemod/pipeline-observations/family145-request-key-preimages.json"
const Digest = "cb7b37f1c09039dff3e2c4b92e9288e25ab6001ee1bbe65225396e91773a5e1c"
const Predecessor = "80d42e2aba875393cae592e3b3f7a37f6af3d892"

type Pin struct {
	File, Function, Before, After, SHA256, Source string
	Calls                                         int
}

var decodedPinCache sync.Map

func Decode(raw []byte) ([]Pin, error) {
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != Digest {
		return nil, fmt.Errorf("request-key complete source manifest changed")
	}
	if cached, ok := decodedPinCache.Load(Digest); ok {
		return slices.Clone(cached.([]Pin)), nil
	}
	var pins []Pin
	if err := json.Unmarshal(raw, &pins); err != nil {
		return nil, err
	}
	if len(pins) != 28 {
		return nil, fmt.Errorf("request-key declaration inventory changed")
	}
	decodedPinCache.Store(Digest, slices.Clone(pins))
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
