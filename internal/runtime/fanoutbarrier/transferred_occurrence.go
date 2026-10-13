package fanoutbarrier

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// DecodeTransferredOccurrence is transport decoding only. Barrier.Validate
// supplies the exact owner relation; native history proves publication membership.
func DecodeTransferredOccurrence(raw []byte) (*TransferredOccurrence, error) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, nil
	}
	var occurrence TransferredOccurrence
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&occurrence); err != nil {
		return nil, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("transferred occurrence contains trailing or invalid JSON")
	}
	return &occurrence, nil
}
