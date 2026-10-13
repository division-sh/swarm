package publicjoin

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
)

const Path = "tools/fixture-codemod/pipeline-observations/family145-public-join-preimages.json"
const Digest = "d1f25c42a603866353f1f33a5cc479205eee723b49afa4166565462d1eb13a94"

type Pin struct {
	File, Function, Before, After, SHA256 string
	Calls                                 int
	Source                                string
}

func Decode(raw []byte) ([]Pin, error) {
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != Digest {
		return nil, fmt.Errorf("complete public join source manifest changed")
	}
	var pins []Pin
	if err := json.Unmarshal(raw, &pins); err != nil {
		return nil, err
	}
	if len(pins) != 10 {
		return nil, fmt.Errorf("public join declaration inventory changed")
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
