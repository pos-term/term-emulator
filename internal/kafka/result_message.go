package kafka

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"time"

	"github.com/google/uuid"
	"github.com/pos-term/term-emulator/internal/queue"
)

var reasonPattern = regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`)

type resultMessage struct {
	TransactionID string
	TerminalID    string
	Result        queue.Result
}

func decodeResult(value []byte) (resultMessage, error) {
	var wire struct {
		SchemaVersion int             `json:"schema_version"`
		TransactionID string          `json:"transaction_id"`
		TerminalID    string          `json:"terminal_id"`
		Status        string          `json:"status"`
		Reason        json.RawMessage `json:"reason"`
		ProcessedAt   string          `json:"processed_at"`
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return resultMessage{}, fmt.Errorf("decode result: %w", err)
	}
	if err := decoder.Decode(new(json.RawMessage)); err != io.EOF {
		return resultMessage{}, errors.New("expected a single result object")
	}
	if wire.SchemaVersion != 1 {
		return resultMessage{}, fmt.Errorf("unsupported result schema_version %d", wire.SchemaVersion)
	}
	id, err := uuid.Parse(wire.TransactionID)
	if err != nil || id.Version() != 7 || id.Variant() != uuid.RFC4122 || id.String() != wire.TransactionID {
		return resultMessage{}, errors.New("transaction_id must be a lowercase UUIDv7")
	}
	if wire.TerminalID == "" {
		return resultMessage{}, errors.New("terminal_id is required")
	}
	if _, err := time.Parse(time.RFC3339, wire.ProcessedAt); err != nil {
		return resultMessage{}, fmt.Errorf("processed_at must be RFC 3339: %w", err)
	}

	result := queue.Result{Status: wire.Status, ProcessedAt: wire.ProcessedAt}
	switch wire.Status {
	case "success":
		if string(wire.Reason) != "null" {
			return resultMessage{}, errors.New("successful result requires a null reason")
		}
	case "failed":
		var reason string
		if err := json.Unmarshal(wire.Reason, &reason); err != nil || !reasonPattern.MatchString(reason) {
			return resultMessage{}, errors.New("failed result requires an uppercase reason code")
		}
		result.Reason = &reason
	default:
		return resultMessage{}, fmt.Errorf("invalid result status %q", wire.Status)
	}
	return resultMessage{TransactionID: wire.TransactionID, TerminalID: wire.TerminalID, Result: result}, nil
}
