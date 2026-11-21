package delivery

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/pos-term/term-emulator/internal/queue"
)

type publisherFunc func(context.Context, queue.Transaction) error

func (f publisherFunc) Publish(ctx context.Context, payment queue.Transaction) error {
	return f(ctx, payment)
}

type outboxWithAckHook struct {
	*queue.Queue
	markSent func(context.Context, string, string) error
}

func (q outboxWithAckHook) MarkSent(ctx context.Context, terminalID, transactionID string) error {
	return q.markSent(ctx, terminalID, transactionID)
}

func TestSenderRetriesInSequence(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	db, err := queue.Open(ctx, filepath.Join(t.TempDir(), "queue.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	const terminal = "term-001"
	first, err := db.Enqueue(ctx, terminal, 15050)
	if err != nil {
		t.Fatal(err)
	}
	second, err := db.Enqueue(ctx, terminal, 100)
	if err != nil {
		t.Fatal(err)
	}
	var events []string
	var sent []queue.Transaction
	producer := publisherFunc(func(_ context.Context, payment queue.Transaction) error {
		sent = append(sent, payment)
		events = append(events, "publish "+payment.TransactionID)
		if len(sent) == 1 {
			return errors.New("broker unavailable")
		}
		return nil
	})
	marks := 0
	outbox := outboxWithAckHook{Queue: db, markSent: func(ctx context.Context, terminalID, transactionID string) error {
		events = append(events, "mark "+transactionID)
		marks++
		if marks == 1 {
			return errors.New("SQLite temporarily busy")
		}
		if err := db.MarkSent(ctx, terminalID, transactionID); err != nil {
			return err
		}
		if transactionID == second.TransactionID {
			cancel()
		}
		return nil
	}}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	RunSender(ctx, terminal, outbox, producer, log)
	if want := []queue.Transaction{first, first, second}; !slices.Equal(sent, want) {
		t.Fatalf("published payments = %+v, want %+v", sent, want)
	}
	wantEvents := []string{
		"publish " + first.TransactionID, "publish " + first.TransactionID,
		"mark " + first.TransactionID, "mark " + first.TransactionID,
		"publish " + second.TransactionID, "mark " + second.TransactionID,
	}
	if !slices.Equal(events, wantEvents) {
		t.Fatalf("delivery events = %v, want %v", events, wantEvents)
	}
	payments, err := db.List(t.Context(), terminal)
	if err != nil {
		t.Fatal(err)
	}
	for _, payment := range payments {
		if payment.SentAt == nil || payment.Result != nil {
			t.Fatalf("payment after Kafka ack = %+v, want sent and awaiting result", payment)
		}
	}
}
