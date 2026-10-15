package servedeventcount

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
)

const Path = "tools/fixture-codemod/pipeline-observations/family145-served-event-count-preimages.json"
const Digest = "aabc674fc81f40d1957138e91bb464cf8209352ec8502721048ba257e7526fa9"
const Predecessor = "4e337ff8d33baf1d7884cc7b7430b5581dd7d653"
const Calls = 6
const Bodies = 4

type Pin struct {
	File, Function, Before, After, SHA256 string
	Calls                                 int
}

func Decode(raw []byte) ([]Pin, error) {
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != Digest {
		return nil, fmt.Errorf("served event-count complete source manifest changed")
	}
	var pins []Pin
	if err := json.Unmarshal(raw, &pins); err != nil {
		return nil, err
	}
	if len(pins) != Bodies {
		return nil, fmt.Errorf("served event-count declaration inventory changed")
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
