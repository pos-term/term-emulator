package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/pos-term/term-emulator/internal/queue"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const testTerminalID = "term-001"

type resultStoreFunc func(context.Context, string, string, queue.Result) (bool, error)

func (f resultStoreFunc) SaveResult(ctx context.Context, terminalID, transactionID string, result queue.Result) (bool, error) {
	return f(ctx, terminalID, transactionID, result)
}

func TestResultConsumerReplaysAfterSaveBeforeCommit(t *testing.T) {
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, resultsTopic))
	t.Cleanup(cluster.Close)
	path := filepath.Join(t.TempDir(), "payments.db")
	db := openResultQueue(t, path)
	payment := enqueuePayment(t, db)
	broker := cluster.ListenAddrs()
	produceResults(t, broker, resultRecord(t, testTerminalID, payment.TransactionID, "failed", "LIMIT_EXCEEDED"))

	firstCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	persisted := make(chan error, 1)
	_, stop := startResultConsumer(t, firstCtx, broker, resultStoreFunc(func(ctx context.Context, terminalID, transactionID string, result queue.Result) (bool, error) {
		saved, err := db.SaveResult(ctx, terminalID, transactionID, result)
		// Stop with a durable result and before Run sends the offset commit.
		cancel()
		if err == nil && !saved {
			err = errors.New("first delivery did not save a result")
		}
		persisted <- err
		return saved, err
	}))
	select {
	case err := <-persisted:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("first consumer did not save the result")
	}
	stop()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db = openResultQueue(t, path)
	var replayCount int
	consumer, stop := startResultConsumer(t, t.Context(), broker, resultStoreFunc(func(ctx context.Context, terminalID, transactionID string, result queue.Result) (bool, error) {
		replayCount++
		saved, err := db.SaveResult(ctx, terminalID, transactionID, result)
		if saved {
			t.Error("replayed result was saved twice")
		}
		return saved, err
	}))
	waitForResultOffset(t, consumer, 1)
	stop()
	if replayCount != 1 {
		t.Fatalf("same consumer group replayed the result %d times, want 1", replayCount)
	}
	stored := resultPayments(t, db)[0]
	if stored.SentAt != nil || stored.Result == nil || stored.Result.Status != "failed" ||
		stored.Result.Reason == nil || *stored.Result.Reason != "LIMIT_EXCEEDED" {
		t.Fatalf("expected the refused payment to survive with no saved producer ack: %+v", stored)
	}
}

func TestResultConsumerFiltersAndStoresBeforeCommit(t *testing.T) {
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, resultsTopic))
	t.Cleanup(cluster.Close)
	db := openResultQueue(t, filepath.Join(t.TempDir(), "payments.db"))
	first, second := enqueuePayment(t, db), enqueuePayment(t, db)
	wrongKey := resultRecord(t, testTerminalID, first.TransactionID, "success", nil)
	wrongKey.Key = []byte("term-002")
	wrongTerminal := resultRecord(t, "term-002", first.TransactionID, "success", nil)
	wrongTerminal.Key = []byte(testTerminalID)
	invalid := resultRecord(t, testTerminalID, second.TransactionID, "success", "LIMIT_EXCEEDED")
	broker := cluster.ListenAddrs()
	produceResults(t, broker, wrongKey, wrongTerminal, invalid,
		resultRecord(t, testTerminalID, first.TransactionID, "failed", "LIMIT_EXCEEDED"),
		resultRecord(t, testTerminalID, second.TransactionID, "success", nil),
		resultRecord(t, testTerminalID, first.TransactionID, "success", nil))

	cluster.ControlKey(int16(kmsg.OffsetCommit), func(req kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		commit := req.(*kmsg.OffsetCommitRequest)
		if commit.Group != "terminal-"+testTerminalID {
			t.Errorf("consumer group = %q", commit.Group)
		}
		for _, topic := range commit.Topics {
			for _, partition := range topic.Partitions {
				if partition.Offset < 4 {
					continue
				}
				payments, err := db.List(t.Context(), testTerminalID)
				if err != nil || len(payments) != 2 || payments[0].Result == nil {
					t.Errorf("committing offset %d before saving the first result: %v", partition.Offset, err)
				}
				if partition.Offset >= 5 && (len(payments) != 2 || payments[1].Result == nil) {
					t.Errorf("committing offset %d before saving the second result", partition.Offset)
				}
			}
		}
		return nil, nil, false
	})

	var attempts []string
	consumer, stop := startResultConsumer(t, t.Context(), broker, resultStoreFunc(func(ctx context.Context, terminalID, transactionID string, result queue.Result) (bool, error) {
		attempts = append(attempts, transactionID)
		if len(attempts) == 1 {
			return false, errors.New("SQLite temporarily busy")
		}
		return db.SaveResult(ctx, terminalID, transactionID, result)
	}))
	waitForResultOffset(t, consumer, 6)
	stop()
	if want := []string{first.TransactionID, first.TransactionID, second.TransactionID, first.TransactionID}; !slices.Equal(attempts, want) {
		t.Fatalf("store attempts = %v, want %v", attempts, want)
	}
	payments := resultPayments(t, db)
	if payments[0].Result == nil || payments[0].Result.Status != "failed" ||
		payments[0].Result.Reason == nil || *payments[0].Result.Reason != "LIMIT_EXCEEDED" {
		t.Fatalf("first payment after conflicting duplicate: %+v", payments[0])
	}
	if payments[1].Result == nil || payments[1].Result.Status != "success" || payments[1].Result.Reason != nil {
		t.Fatalf("second payment after successful result: %+v", payments[1])
	}
}

func startResultConsumer(t *testing.T, parent context.Context, brokers []string, store ResultStore) (*ResultConsumer, func()) {
	t.Helper()
	consumer, err := NewResultConsumer(brokers, testTerminalID)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	go func() {
		defer close(done)
		consumer.Run(ctx, store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()
	stop := sync.OnceFunc(func() {
		cancel()
		<-done
		consumer.Close()
	})
	t.Cleanup(stop)
	return consumer, stop
}

func waitForResultOffset(t *testing.T, consumer *ResultConsumer, offset int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		committed := consumer.client.CommittedOffsets()[resultsTopic][0].Offset
		if committed == offset {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("committed result offset %d, want %d", committed, offset)
		case <-ticker.C:
		}
	}
}

func produceResults(t *testing.T, brokers []string, records ...*kgo.Record) {
	t.Helper()
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.AllowIdempotentProduceCancellation())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

func resultRecord(t *testing.T, terminalID, transactionID, status string, reason any) *kgo.Record {
	t.Helper()
	value, err := json.Marshal(map[string]any{
		"schema_version": 1, "terminal_id": terminalID, "transaction_id": transactionID,
		"status": status, "reason": reason, "processed_at": "2026-11-21T06:10:31.005Z",
	})
	if err != nil {
		t.Fatalf("encode test result: %v", err)
	}
	return &kgo.Record{Topic: resultsTopic, Key: []byte(terminalID), Value: value}
}

func openResultQueue(t *testing.T, path string) *queue.Queue {
	t.Helper()
	db, err := queue.Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func enqueuePayment(t *testing.T, db *queue.Queue) queue.Transaction {
	t.Helper()
	payment, err := db.Enqueue(t.Context(), testTerminalID, 15050)
	if err != nil {
		t.Fatal(err)
	}
	return payment
}

func resultPayments(t *testing.T, db *queue.Queue) []queue.Payment {
	t.Helper()
	payments, err := db.List(t.Context(), testTerminalID)
	if err != nil {
		t.Fatal(err)
	}
	return payments
}
