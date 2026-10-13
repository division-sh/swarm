package forknotice

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
)

const Path = "tools/fixture-codemod/pipeline-observations/family145-fork-notice-preimages.json"
const Digest = "2c569e9fcb4d63d943f640c11245a4d3d00cdcf62653ec768ec70af713abb03e"

type Pin struct {
	File, Function, Before, After, SHA256 string
	Calls                                 int
	Source                                string
}

func Decode(raw []byte) ([]Pin, error) {
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != Digest {
		return nil, fmt.Errorf("fork notice complete source manifest changed")
	}
	var pins []Pin
	if err := json.Unmarshal(raw, &pins); err != nil {
		return nil, err
	}
	if len(pins) != 4 {
		return nil, fmt.Errorf("fork notice declaration inventory changed")
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
