package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/pos-term/term-emulator/internal/queue"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	transactionsTopic = "pos.transactions.v1"
	deliveryTimeout   = 5 * time.Second
)

type Producer struct {
	client *kgo.Client
}

// NewProducer creates a producer without waiting for the broker to become available.
func NewProducer(brokers []string) (*Producer, error) {
	if len(brokers) == 0 {
		return nil, errors.New("configure at least one Kafka broker")
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		// Keep franz-go's default idempotency for its own retries. A timed-out
		// in-flight write can still have landed; the processor deduplicates a
		// later application retry using the original transaction ID.
		kgo.AllowIdempotentProduceCancellation(),
		kgo.DialTimeout(2*time.Second),
		kgo.ProduceRequestTimeout(3*time.Second),
		kgo.RequestTimeoutOverhead(2*time.Second),
		kgo.RecordDeliveryTimeout(deliveryTimeout),
	)
	if err != nil {
		return nil, fmt.Errorf("create Kafka producer: %w", err)
	}
	return &Producer{client: client}, nil
}

// Publish waits for a successful Kafka acknowledgement before returning nil.
func (p *Producer) Publish(ctx context.Context, payment queue.Transaction) error {
	value, err := json.Marshal(payment)
	if err != nil {
		return fmt.Errorf("encode payment: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, deliveryTimeout)
	defer cancel()
	record := &kgo.Record{
		Topic: transactionsTopic,
		Key:   []byte(payment.TerminalID),
		Value: value,
		Headers: []kgo.RecordHeader{
			{Key: "event_type", Value: []byte("transaction.submitted")},
			{Key: "trace_id", Value: []byte(payment.TransactionID)},
		},
	}
	if err := p.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("publish payment to %s: %w", transactionsTopic, err)
	}
	return nil
}

func (p *Producer) Close() {
	p.client.Close()
}
