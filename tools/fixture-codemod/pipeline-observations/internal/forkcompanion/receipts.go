package forkcompanion

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
)

const Path = "tools/fixture-codemod/pipeline-observations/family145-fork-companion-preimages.json"
const Digest = "24a214ba558ea44202656a48ad6695f6344254c5f9c335fda2a392d16c34054a"

type Pin struct {
	File, Function, Before, After, SHA256 string
	Calls                                 int
	Source                                string
}

func Decode(raw []byte) ([]Pin, error) {
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != Digest {
		return nil, fmt.Errorf("fork companion complete source manifest changed")
	}
	var pins []Pin
	if err := json.Unmarshal(raw, &pins); err != nil {
		return nil, err
	}
	if len(pins) != 7 {
		return nil, fmt.Errorf("fork companion declaration inventory changed")
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
