package channelanchor

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/channeldecision"
)

const Path = "tools/fixture-codemod/pipeline-observations/family145-channel-anchor-preimages.json"
const Digest = "cea892c3e4281d02483f29ae522df599d66b886bb94711328d2f1957c1c0ca5e"
const Predecessor = "adc1db8bd24b1dfcedb691d7be8fc3afe83db268"
const Calls = 54
const Bodies = 26

type Pin struct {
	File, Function, Before, After, SHA256 string
	Calls                                 int
}

func Decode(raw []byte) ([]Pin, error) {
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != Digest {
		return nil, fmt.Errorf("channel anchor complete source manifest changed")
	}
	var pins []Pin
	if err := json.Unmarshal(raw, &pins); err != nil {
		return nil, err
	}
	if len(pins) != Bodies {
		return nil, fmt.Errorf("channel anchor declaration inventory changed")
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
	current, err = channeldecision.ReadPrevious(file, function, current)
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
