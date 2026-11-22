package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pos-term/term-emulator/internal/delivery"
	"github.com/pos-term/term-emulator/internal/queue"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestProducerContract(t *testing.T) {
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, "pos.transactions.v1"))
	t.Cleanup(cluster.Close)
	cluster.ControlKey(int16(kmsg.Produce), func(request kmsg.Request) (kmsg.Response, error, bool) {
		produce := request.(*kmsg.ProduceRequest)
		if produce.Acks != -1 {
			t.Errorf("produce acks = %d, want -1", produce.Acks)
		}
		for _, topic := range produce.Topics {
			for _, partition := range topic.Partitions {
				var batch kmsg.RecordBatch
				if err := batch.ReadFrom(partition.Records); err != nil || batch.ProducerID < 0 {
					t.Errorf("expected Kafka producer ID: id=%d, err=%v", batch.ProducerID, err)
				}
			}
		}
		return nil, nil, false
	})
	checkPublishedPayment(t, cluster.ListenAddrs())
}

func TestProducerWithKafka(t *testing.T) {
	brokers := os.Getenv("TEST_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("set TEST_KAFKA_BROKERS to a broker with the pos.transactions.v1 topic")
	}
	checkPublishedPayment(t, strings.Split(brokers, ","))
}

func checkPublishedPayment(t *testing.T, brokers []string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
	defer cancel()
	producer, err := NewProducer(brokers)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	db, err := queue.Open(ctx, filepath.Join(t.TempDir(), "queue.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	payment, err := db.Enqueue(ctx, "test-"+uuid.NewString(), 15050)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		delivery.RunSender(ctx, payment.TerminalID, db, producer, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	}()
	defer func() {
		cancel()
		<-done
	}()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		_, found, err := db.NextUnsent(ctx, payment.TerminalID)
		if err != nil {
			t.Fatal(err)
		}
		if !found {
			break
		}
	}
	reader, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics("pos.transactions.v1"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	for ctx.Err() == nil {
		fetches := reader.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			t.Fatal(err)
		}
		for _, record := range fetches.Records() {
			if string(record.Key) != payment.TerminalID {
				continue
			}
			wantValue := map[string]any{
				"schema_version": float64(1), "transaction_id": payment.TransactionID,
				"terminal_id": payment.TerminalID, "amount_minor": float64(15050), "currency": "RUB",
				"created_at": payment.CreatedAt, "seq": float64(1),
			}
			var got map[string]any
			if err := json.Unmarshal(record.Value, &got); err != nil || !reflect.DeepEqual(got, wantValue) {
				t.Fatalf("payment body = %s, err=%v; want %v", record.Value, err, wantValue)
			}
			wantHeaders := []kgo.RecordHeader{
				{Key: "event_type", Value: []byte("transaction.submitted")},
				{Key: "trace_id", Value: []byte(payment.TransactionID)},
			}
			if !reflect.DeepEqual(record.Headers, wantHeaders) {
				t.Fatalf("headers = %v, want %v", record.Headers, wantHeaders)
			}
			return
		}
	}
	t.Fatal("acknowledged payment was not read back from Kafka")
}

func TestPublishCancellationWhileAwaitingAck(t *testing.T) {
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, transactionsTopic))
	t.Cleanup(cluster.Close)
	arrived, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	cluster.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
		close(arrived)
		cluster.SleepControl(func() { <-release })
		return nil, nil, false
	})
	producer, err := NewProducer(cluster.ListenAddrs())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(producer.Close)
	payment := samplePayment(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- producer.Publish(ctx, payment) }()
	select {
	case <-arrived:
	case err := <-done:
		t.Fatalf("publish returned before broker received the request: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("broker never received a produce request")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("publish error = %v, want cancellation", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("publish waited for an unresponsive broker after cancellation")
	}
}

func samplePayment(t *testing.T) queue.Transaction {
	t.Helper()
	id, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}
	return queue.Transaction{
		SchemaVersion: 1, TransactionID: id.String(), TerminalID: "test-" + id.String(),
		AmountMinor: 15050, Currency: "RUB", CreatedAt: "2026-11-21T08:00:00.000Z", Seq: 42,
	}
}
