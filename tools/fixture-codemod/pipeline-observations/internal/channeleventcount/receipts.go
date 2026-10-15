package channeleventcount

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
)

const Path = "tools/fixture-codemod/pipeline-observations/family145-channel-event-count-preimages.json"
const Digest = "0ae312b38187870818561ce67f894777d362156c34b5181162774b817497db39"
const Predecessor = "c3e48876761af8b84d8398b49c19dd1cf5002add"
const Calls = 15
const Bodies = 11

type Pin struct {
	File, Function, Before, After, SHA256 string
	Calls                                 int
}

func Decode(raw []byte) ([]Pin, error) {
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != Digest {
		return nil, fmt.Errorf("channel event-count complete source manifest changed")
	}
	var pins []Pin
	if err := json.Unmarshal(raw, &pins); err != nil {
		return nil, err
	}
	if len(pins) != Bodies {
		return nil, fmt.Errorf("channel event-count declaration inventory changed")
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

func ReadPrevious(file, function, current string) (string, error) {
	raw, err := os.ReadFile(Path)
	if err != nil {
		return "", err
	}
	pins, err := Decode(raw)
	if err != nil {
		return "", err
	}
	return Previous(pins, file, function, current), nil
}
