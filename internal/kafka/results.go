package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/pos-term/term-emulator/internal/queue"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	resultsTopic        = "pos.transaction-results.v1"
	offsetCommitTimeout = 3 * time.Second
)

type ResultStore interface {
	SaveResult(ctx context.Context, terminalID, transactionID string, result queue.Result) (bool, error)
}

type ResultConsumer struct {
	client     *kgo.Client
	terminalID string
}

func NewResultConsumer(brokers []string, terminalID string) (*ResultConsumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup("terminal-"+terminalID),
		kgo.ConsumeTopics(resultsTopic),
		// A terminal can have queued results before it ever joins its group.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.DialTimeout(2*time.Second),
		kgo.RequestTimeoutOverhead(2*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("create Kafka result consumer: %w", err)
	}
	return &ResultConsumer{client: client, terminalID: terminalID}, nil
}

// Run processes results in offset order until ctx is cancelled.
func (c *ResultConsumer) Run(ctx context.Context, store ResultStore, log *slog.Logger) {
	log = log.With("terminal_id", c.terminalID)
	for ctx.Err() == nil {
		fetches := c.client.PollRecords(ctx, 1)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			c.client.AllowRebalance()
			return
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			log.Warn("fetch payment result", "topic", topic, "partition", partition, "error", err)
		})
		fetches.EachRecord(func(record *kgo.Record) {
			c.handle(ctx, store, record, log.With("partition", record.Partition, "offset", record.Offset))
		})
		c.client.AllowRebalance()
	}
}

func (c *ResultConsumer) handle(ctx context.Context, store ResultStore, record *kgo.Record, log *slog.Logger) {
	c.persist(ctx, store, record, log)
	if ctx.Err() != nil {
		return
	}
	// Keep polling after a failed commit: every earlier offset is already
	// safe to skip. If Kafka replays it, SQLite keeps the first result.
	commitCtx, cancel := context.WithTimeout(ctx, offsetCommitTimeout)
	defer cancel()
	if err := c.client.CommitRecords(commitCtx, record); err != nil && ctx.Err() == nil {
		log.Warn("commit payment result offset", "error", err)
	}
}

func (c *ResultConsumer) persist(ctx context.Context, store ResultStore, record *kgo.Record, log *slog.Logger) {
	if string(record.Key) != c.terminalID {
		return
	}
	message, err := decodeResult(record.Value)
	if err != nil {
		log.Warn("skip invalid payment result", "error", err)
		return
	}
	if message.TerminalID != c.terminalID {
		return
	}
	log = log.With("transaction_id", message.TransactionID)
	for delay := 200 * time.Millisecond; ctx.Err() == nil; delay = min(delay*2, 5*time.Second) {
		saved, err := store.SaveResult(ctx, c.terminalID, message.TransactionID, message.Result)
		if err == nil {
			if saved {
				showResult(log, message.Result)
			}
			return
		}
		if ctx.Err() != nil {
			return
		}
		log.Warn("save payment result", "error", err, "retry_in", delay)
		select {
		case <-ctx.Done():
		case <-time.After(delay):
		}
	}
}

func showResult(log *slog.Logger, result queue.Result) {
	if result.Reason != nil {
		log.Info("payment result", "status", result.Status, "reason", *result.Reason)
		return
	}
	log.Info("payment result", "status", result.Status)
}

func (c *ResultConsumer) Close() {
	c.client.AllowRebalance()
	ctx, cancel := context.WithTimeout(context.Background(), offsetCommitTimeout)
	defer cancel()
	c.client.LeaveGroupContext(ctx)
	c.client.Close()
}
