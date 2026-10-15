package deliverystatus

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/eventname"
)

const Path = "tools/fixture-codemod/pipeline-observations/family145-delivery-status-preimages.json"
const Digest = "b4284bb05b933c6ac8bb151de9355e3f5bd28a6ff363b2f926d9da5a8b0042d4"
const Predecessor = "5e6f4477a1a401df2c6227f617dd97eeba6449e8"
const Calls = 28
const Bodies = 14

type Pin struct {
	File, Function, Before, After, SHA256 string
	Calls                                 int
}

func Decode(raw []byte) ([]Pin, error) {
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != Digest {
		return nil, fmt.Errorf("delivery status complete source manifest changed")
	}
	var pins []Pin
	if err := json.Unmarshal(raw, &pins); err != nil {
		return nil, err
	}
	if len(pins) != Bodies {
		return nil, fmt.Errorf("delivery status declaration inventory changed")
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
	var err error
	current, err = eventname.ReadPrevious(file, function, current)
	if err != nil {
		return "", err
	}
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
