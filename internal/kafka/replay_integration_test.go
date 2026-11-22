package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pos-term/term-emulator/internal/delivery"
	"github.com/pos-term/term-emulator/internal/queue"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestPaymentReplayWithKafka(t *testing.T) {
	rawBrokers := os.Getenv("TEST_KAFKA_E2E_BROKERS")
	if rawBrokers == "" {
		t.Skip("set TEST_KAFKA_E2E_BROKERS to the local Kafka and payment-processor stack")
	}
	brokers := strings.Split(rawBrokers, ",")
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Second)
	defer cancel()
	terminalID := "term-replay-" + uuid.NewString()
	t.Logf("terminal_id=%s", terminalID)
	terminalGroup := "terminal-" + terminalID

	wire := newReplayClient(t, kgo.SeedBrokers(brokers...), kgo.AllowIdempotentProduceCancellation())
	admin := kadm.NewClient(wire)
	traffic := observeTerminal(t, ctx, admin, brokers, terminalID)
	path := filepath.Join(t.TempDir(), "payments.db")
	db := openResultQueue(t, path)
	payment, err := db.Enqueue(ctx, terminalID, 15050)
	if err != nil {
		t.Fatal(err)
	}
	firstProducer, err := NewProducer(brokers)
	if err != nil {
		t.Fatal(err)
	}
	closeFirstProducer := sync.OnceFunc(firstProducer.Close)
	t.Cleanup(closeFirstProducer)
	if err := firstProducer.Publish(ctx, payment); err != nil {
		t.Fatal(err)
	}
	closeFirstProducer()
	// Simulate stopping after the Kafka acknowledgement and before MarkSent.
	closeReplayQueue(t, db)

	db = openResultQueue(t, path)
	sendAgain(t, ctx, brokers, db, terminalID)
	traffic.readUntil(t, ctx, "two submissions and the processor result", func() bool {
		return len(traffic.submissions) >= 2 && len(traffic.results) >= 1
	})
	first, resent := traffic.submissions[0], traffic.submissions[1]
	if first.Partition != resent.Partition || first.Offset >= resent.Offset || !sameKafkaEvent(first, resent) {
		t.Fatal("expected the same submission at two increasing offsets in one partition")
	}
	var published queue.Transaction
	if err := json.Unmarshal(resent.Value, &published); err != nil || published != payment {
		t.Fatalf("resent payment = %+v, want %+v (decode error: %v)", published, payment, err)
	}
	waitForBrokerCommit(t, ctx, admin, "payment-processor", resent)
	t.Logf("processor committed both copies in input partition %d at offsets %d and %d", resent.Partition, first.Offset, resent.Offset)
	result := traffic.results[0]

	// Save the processor result, then stop before the consumer can send OffsetCommit.
	firstRead, stopFirstRead := context.WithCancel(ctx)
	defer stopFirstRead()
	saved := make(chan bool, 1)
	_, stop := startResultConsumer(t, firstRead, brokers, terminalID, resultStoreFunc(func(ctx context.Context, terminalID, id string, result queue.Result) (bool, error) {
		inserted, err := db.SaveResult(ctx, terminalID, id, result)
		if err == nil {
			stopFirstRead()
			saved <- inserted
		}
		return inserted, err
	}))
	select {
	case inserted := <-saved:
		if !inserted {
			t.Fatal("first result did not update the queued payment")
		}
	case <-ctx.Done():
		t.Fatalf("wait for durable processor result: %v", ctx.Err())
	}
	stop()
	if offset := brokerOffset(t, ctx, admin, terminalGroup, result); offset > result.Offset {
		t.Fatalf("result offset committed before restart: %d > %d", offset, result.Offset)
	}
	stored := replayPayment(t, ctx, db, terminalID)
	if stored.Transaction != payment || stored.SentAt == nil || stored.Result == nil ||
		stored.Result.Status != "success" || stored.Result.Reason != nil {
		t.Fatalf("unexpected payment after processor result: %+v", stored)
	}
	closeReplayQueue(t, db)

	// Simulate a second publication of the same result, as can happen before
	// the processor records its publish acknowledgement.
	republished, err := wire.ProduceSync(ctx, &kgo.Record{
		Topic: result.Topic, Key: result.Key, Value: result.Value, Headers: result.Headers,
	}).First()
	if err != nil {
		t.Fatal(err)
	}
	traffic.readUntil(t, ctx, "the explicit result republication", func() bool {
		return traffic.results[len(traffic.results)-1].Offset >= republished.Offset
	})
	if len(traffic.results) != 2 || traffic.results[1].Offset != republished.Offset ||
		!sameKafkaEvent(result, traffic.results[1]) {
		t.Fatal("expected one result from the processor and one identical test republication")
	}

	db = openResultQueue(t, path)
	var attempts []string
	_, stop = startResultConsumer(t, ctx, brokers, terminalID, resultStoreFunc(func(ctx context.Context, terminalID, id string, result queue.Result) (bool, error) {
		attempts = append(attempts, id)
		inserted, err := db.SaveResult(ctx, terminalID, id, result)
		if inserted {
			t.Error("consumer saved an already recorded result again")
		}
		return inserted, err
	}))
	waitForBrokerCommit(t, ctx, admin, terminalGroup, republished)
	stop()
	if len(attempts) != 2 || attempts[0] != payment.TransactionID || attempts[1] != payment.TransactionID {
		t.Fatalf("expected both the uncommitted result and its republication, got %v", attempts)
	}
	if after := replayPayment(t, ctx, db, terminalID); !reflect.DeepEqual(after, stored) {
		t.Fatal("replaying the result changed the stored payment")
	}
	t.Logf("terminal replayed result offsets %d and %d and committed without changing SQLite", result.Offset, republished.Offset)
}

func sendAgain(t *testing.T, ctx context.Context, brokers []string, db *queue.Queue, terminalID string) {
	t.Helper()
	producer, err := NewProducer(brokers)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		delivery.RunSender(ctx, terminalID, db, producer, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()
	defer func() {
		cancel()
		<-done
	}()
	waitForReplay(t, ctx, "the sender to save the Kafka acknowledgement", func() bool {
		return replayPayment(t, ctx, db, terminalID).SentAt != nil
	})
}

type terminalTraffic struct {
	reader               *kgo.Client
	terminalID           string
	submissions, results []*kgo.Record
}

func observeTerminal(t *testing.T, ctx context.Context, admin *kadm.Client, brokers []string, terminalID string) *terminalTraffic {
	t.Helper()
	end, err := admin.ListEndOffsets(ctx, transactionsTopic, resultsTopic)
	if err != nil {
		t.Fatal(err)
	}
	if err := end.Error(); err != nil {
		t.Fatal(err)
	}
	partitions := make(map[string]map[int32]kgo.Offset)
	for topic, offsets := range end {
		partitions[topic] = make(map[int32]kgo.Offset, len(offsets))
		for partition, offset := range offsets {
			partitions[topic][partition] = kgo.NewOffset().At(offset.Offset)
		}
	}
	if len(partitions[transactionsTopic]) == 0 || len(partitions[resultsTopic]) != 1 {
		t.Fatal("expected the infra transaction topics and one results partition")
	}
	return &terminalTraffic{
		reader:     newReplayClient(t, kgo.SeedBrokers(brokers...), kgo.ConsumePartitions(partitions)),
		terminalID: terminalID,
	}
}

func (w *terminalTraffic) readUntil(t *testing.T, ctx context.Context, description string, ready func() bool) {
	t.Helper()
	for !ready() {
		fetches := w.reader.PollRecords(ctx, 1)
		if err := fetches.Err(); err != nil {
			t.Fatalf("wait for %s: %v", description, err)
		}
		fetches.EachRecord(func(record *kgo.Record) {
			if string(record.Key) != w.terminalID {
				return
			}
			if record.Topic == transactionsTopic {
				w.submissions = append(w.submissions, record)
			} else {
				w.results = append(w.results, record)
			}
		})
	}
}

func sameKafkaEvent(a, b *kgo.Record) bool {
	return bytes.Equal(a.Key, b.Key) && bytes.Equal(a.Value, b.Value) && reflect.DeepEqual(a.Headers, b.Headers)
}

func waitForBrokerCommit(t *testing.T, ctx context.Context, admin *kadm.Client, group string, record *kgo.Record) {
	t.Helper()
	waitForReplay(t, ctx, group+" to commit "+record.Topic, func() bool {
		return brokerOffset(t, ctx, admin, group, record) > record.Offset
	})
}

func brokerOffset(t *testing.T, ctx context.Context, admin *kadm.Client, group string, record *kgo.Record) int64 {
	t.Helper()
	offsets, err := admin.FetchOffsets(ctx, group)
	if errors.Is(err, kerr.GroupIDNotFound) {
		return -1
	}
	if err != nil {
		t.Fatalf("fetch committed offsets for %s: %v", group, err)
	}
	offset, found := offsets.Lookup(record.Topic, record.Partition)
	if !found {
		return -1
	}
	if offset.Err != nil {
		t.Fatalf("read committed offset for %s: %v", group, offset.Err)
	}
	return offset.At
}

func waitForReplay(t *testing.T, ctx context.Context, description string, ready func() bool) {
	t.Helper()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if ready() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for %s: %v", description, ctx.Err())
		case <-ticker.C:
		}
	}
}

func newReplayClient(t *testing.T, options ...kgo.Opt) *kgo.Client {
	t.Helper()
	client, err := kgo.NewClient(options...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}

func replayPayment(t *testing.T, ctx context.Context, db *queue.Queue, terminalID string) queue.Payment {
	t.Helper()
	payments, err := db.List(ctx, terminalID)
	if err != nil || len(payments) != 1 {
		t.Fatalf("expected one SQLite payment, got %d (error: %v)", len(payments), err)
	}
	return payments[0]
}

func closeReplayQueue(t *testing.T, db *queue.Queue) {
	t.Helper()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}
