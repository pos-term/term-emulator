package cli

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/pos-term/term-emulator/internal/delivery"
	"github.com/pos-term/term-emulator/internal/kafka"
	"github.com/pos-term/term-emulator/internal/queue"
)

func runKafka(ctx context.Context, db *queue.Queue, terminalID, addresses string, output io.Writer) error {
	brokers := strings.Split(addresses, ",")
	for i, address := range brokers {
		brokers[i] = strings.TrimSpace(address)
		if brokers[i] == "" {
			return errors.New("-brokers must contain nonempty Kafka addresses")
		}
	}
	producer, err := kafka.NewProducer(brokers)
	if err != nil {
		return err
	}
	defer producer.Close()
	consumer, err := kafka.NewResultConsumer(brokers, terminalID)
	if err != nil {
		return err
	}
	defer consumer.Close()

	log := slog.New(slog.NewTextHandler(output, nil))
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	log.Info("Kafka workers started", "terminal_id", terminalID)
	done := make(chan struct{})
	go func() {
		defer close(done)
		delivery.RunSender(ctx, terminalID, db, producer, log)
	}()
	consumer.Run(ctx, db, log)
	stop()
	<-done
	return nil
}
