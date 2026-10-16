package channelintent

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
)

const Path = "tools/fixture-codemod/pipeline-observations/family145-channel-intent-preimages.json"
const Digest = "03b674c3da5a9d6aa45eaad38dc9005037239a2ff993ac71cce136e4120b2355"
const Predecessor = "3dc4108680367fcf0633aae75c8f7cdca2959961"

type Pin struct {
	File, Function, Before, After, SHA256, Class string
	Calls                                        int
}

var decodedPinCache sync.Map

func Decode(raw []byte) ([]Pin, error) {
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != Digest {
		return nil, fmt.Errorf("channel-intent complete source manifest changed")
	}
	if cached, ok := decodedPinCache.Load(Digest); ok {
		return slices.Clone(cached.([]Pin)), nil
	}
	var pins []Pin
	if err := json.Unmarshal(raw, &pins); err != nil {
		return nil, err
	}
	if len(pins) != 41 {
		return nil, fmt.Errorf("channel-intent declaration inventory changed")
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
