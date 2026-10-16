package eventname

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"sync"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/channelanchor"
)

const Path = "tools/fixture-codemod/pipeline-observations/family145-event-name-preimages.json"
const Digest = "3ec05c09f5a792662c6623c10efac08ae2c897fd8308a4d9e62aa941d1f3b0b1"
const Predecessor = "cee2b46e824700b380339cba079cb5dfdd191aba"
const Calls = 14
const Bodies = 6

type Pin struct {
	File, Function, Before, After, SHA256 string
	Calls                                 int
}

var decodedPinCache sync.Map

func Decode(raw []byte) ([]Pin, error) {
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != Digest {
		return nil, fmt.Errorf("event name complete source manifest changed")
	}
	if cached, ok := decodedPinCache.Load(Digest); ok {
		return slices.Clone(cached.([]Pin)), nil
	}
	var pins []Pin
	if err := json.Unmarshal(raw, &pins); err != nil {
		return nil, err
	}
	if len(pins) != Bodies {
		return nil, fmt.Errorf("event name declaration inventory changed")
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

func ReadPrevious(file, function, current string) (string, error) {
	var err error
	current, err = channelanchor.ReadPrevious(file, function, current)
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
