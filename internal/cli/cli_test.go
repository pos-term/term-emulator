package cli

import (
	"bytes"
	"encoding/json"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pos-term/term-emulator/internal/queue"
)

func TestQueueSurvivesReopen(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "queue.db")
	command := func(id, action, input string) string {
		t.Helper()
		var out bytes.Buffer
		if err := Run(t.Context(), []string{"-db", dbPath, "-terminal-id", id, action},
			strings.NewReader(input), &out, io.Discard); err != nil {
			t.Fatal(err)
		}
		return out.String()
	}
	decode := func(result string) queue.Transaction {
		t.Helper()
		var payment queue.Transaction
		if err := json.Unmarshal([]byte(result), &payment); err != nil {
			t.Fatal(err)
		}
		return payment
	}

	first := command("term-001", "submit", "150.00\ny\n")
	payment := decode(first)
	id, err := uuid.Parse(payment.TransactionID)
	if err != nil || id.Version() != 7 || id.String() != payment.TransactionID {
		t.Fatalf("expected lowercase UUIDv7, got %q (%v)", payment.TransactionID, err)
	}
	if _, err := time.Parse("2006-01-02T15:04:05.000Z", payment.CreatedAt); err != nil {
		t.Fatalf("expected UTC timestamp with milliseconds: %v", err)
	}
	if payment.AmountMinor != 15000 || payment.Seq != 1 || payment.SchemaVersion != 1 ||
		payment.Currency != "RUB" || payment.TerminalID != "term-001" {
		t.Fatalf("unexpected payment: %+v", payment)
	}

	if got := command("term-001", "submit", "10\nn\n"); got != "" {
		t.Fatalf("cancelled payment produced output: %q", got)
	}
	if got := command("term-001", "list", ""); got != first {
		t.Fatalf("expected the same persisted payment, got %q", got)
	}
	second := decode(command("term-001", "submit", "0,01\nyes\n"))
	if second.Seq != 2 || second.AmountMinor != 1 || second.TransactionID == payment.TransactionID {
		t.Fatalf("unexpected second payment: %+v", second)
	}
	separate := command("term-002", "submit", "1\ny\n")
	if got := decode(separate).Seq; got != 1 {
		t.Fatalf("separate terminal starts at sequence %d, want 1", got)
	}
	if got := command("term-002", "list", ""); got != separate {
		t.Fatalf("queue returned records from another terminal: %q", got)
	}
}

func TestParseAmount(t *testing.T) {
	for _, tc := range []struct {
		input string
		want  int64
	}{
		{"1.2", 120},
		{"0,01", 1},
		{"92233720368547758.07", 9223372036854775807},
		{"", 0},
		{"0.00", 0},
		{"-1", 0},
		{"1.001", 0},
		{"1e2", 0},
		{"92233720368547758.08", 0},
	} {
		t.Run(tc.input, func(t *testing.T) {
			got, err := parseAmount(tc.input)
			if (err != nil) != (tc.want == 0) || got != tc.want {
				t.Fatalf("parseAmount(%q) = %d, %v; want %d", tc.input, got, err, tc.want)
			}
		})
	}
}
