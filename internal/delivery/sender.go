package delivery

import (
	"context"
	"log/slog"
	"time"

	"github.com/pos-term/term-emulator/internal/queue"
)

const (
	minBackoff   = 200 * time.Millisecond
	maxBackoff   = 5 * time.Second
	idleInterval = 250 * time.Millisecond
)

type Outbox interface {
	NextUnsent(context.Context, string) (queue.Transaction, bool, error)
	MarkSent(ctx context.Context, terminalID, transactionID string) error
}

type Publisher interface {
	// Publish returns nil once Kafka acknowledges the record.
	Publish(context.Context, queue.Transaction) error
}

// RunSender delivers payments for one terminal in sequence order until cancelled.
func RunSender(ctx context.Context, terminalID string, outbox Outbox, producer Publisher, log *slog.Logger) {
	log = log.With("terminal_id", terminalID)
	backoff := minBackoff
	for ctx.Err() == nil {
		payment, found, err := outbox.NextUnsent(ctx, terminalID)
		if err != nil {
			backoff = retry(ctx, backoff, log, "read unsent payment", err)
			continue
		}
		if !found {
			backoff = minBackoff
			pause(ctx, idleInterval)
			continue
		}

		paymentLog := log.With("transaction_id", payment.TransactionID)
		if err := producer.Publish(ctx, payment); err != nil {
			backoff = retry(ctx, backoff, paymentLog, "publish payment", err)
			continue
		}
		// Keep the acknowledged payment in memory while retrying the SQLite
		// write. If the process stops here, the next run can safely resend it.
		for delay := minBackoff; ctx.Err() == nil; {
			if err := outbox.MarkSent(ctx, terminalID, payment.TransactionID); err != nil {
				delay = retry(ctx, delay, paymentLog, "record Kafka acknowledgement", err)
				continue
			}
			break
		}
		backoff = minBackoff
	}
}

func retry(ctx context.Context, delay time.Duration, log *slog.Logger, operation string, err error) time.Duration {
	if ctx.Err() == nil {
		log.Warn(operation, "error", err, "retry_in", delay)
		pause(ctx, delay)
	}
	return min(delay*2, maxBackoff)
}

func pause(ctx context.Context, delay time.Duration) {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
