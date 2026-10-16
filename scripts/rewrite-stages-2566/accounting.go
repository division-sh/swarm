package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"github.com/division-sh/swarm/scripts/rewrite-stages-2566/formatproof"
)

func validateFiniteAccounting(root string) error {
	raw, err := os.ReadFile(filepath.Join(root, formatproof.Successors))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	history, err := formatproof.Read(raw)
	if err != nil {
		return err
	}
	endpoint, _, err := history.Endpoint()
	if err != nil || endpoint == nil {
		return err
	}
	body, err := os.ReadFile(filepath.Join(root, formatproof.Source))
	if err != nil || !bytes.Equal(body, endpoint) {
		return fmt.Errorf("finite current source is not its actual resolved endpoint: %v", err)
	}
	return nil
}

func finiteCurrentOutput(root string, c change, historical []byte) ([]byte, error) {
	raw, err := os.ReadFile(filepath.Join(root, formatproof.Successors))
	if os.IsNotExist(err) {
		return historical, nil
	}
	if err != nil {
		return nil, err
	}
	history, err := formatproof.Read(raw)
	if err != nil {
		return nil, err
	}
	if c.File != formatproof.Source {
		return historical, nil
	}
	if c.AfterHash != formatproof.Origin || digest(historical) != c.AfterHash {
		return nil, fmt.Errorf("finite successor origin differs from immutable ledger")
	}
	endpoint, _, err := history.Endpoint()
	if err != nil {
		return nil, err
	}
	if endpoint == nil {
		return historical, nil
	}
	if bytes.Equal(historical, endpoint) {
		return nil, fmt.Errorf("finite successor resurrected its historical source")
	}
	return endpoint, nil
}

func finiteCurrentSource(root string, c change, body []byte) ([]byte, error) {
	if c.File != formatproof.Source {
		return rewrite(c, body)
	}
	raw, err := os.ReadFile(filepath.Join(root, formatproof.Successors))
	if os.IsNotExist(err) {
		return rewrite(c, body)
	}
	if err != nil {
		return nil, err
	}
	history, err := formatproof.Read(raw)
	if err != nil {
		return nil, err
	}
	endpoint, _, err := history.Endpoint()
	if err != nil {
		return nil, err
	}
	if endpoint == nil {
		return rewrite(c, body)
	}
	if c.AfterHash != formatproof.Origin || digest([]byte(history.Records[0].Before)) != c.AfterHash || !bytes.Equal(endpoint, body) {
		return nil, fmt.Errorf("current finite source does not match its complete successor chain")
	}
	return body, nil
}
