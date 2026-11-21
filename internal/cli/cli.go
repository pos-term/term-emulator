package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"

	"github.com/pos-term/term-emulator/internal/queue"
)

var (
	terminalIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
	amountPattern     = regexp.MustCompile(`^[0-9]+([.,][0-9]{1,2})?$`)
)

// Run executes a terminal command using the supplied input and output streams.
func Run(ctx context.Context, args []string, in io.Reader, out, prompts io.Writer) error {
	flags := flag.NewFlagSet("terminal", flag.ContinueOnError)
	flags.SetOutput(prompts)
	id := flags.String("terminal-id", "term-001", "terminal identifier")
	path := flags.String("db", ".terminal/queue.db", "SQLite queue file")
	brokers := flags.String("brokers", "localhost:9094", "comma-separated Kafka addresses for run")
	flags.Usage = func() {
		fmt.Fprintln(prompts, "Usage: terminal [flags] <submit|list|run>")
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	command := flags.Arg(0)
	if flags.NArg() != 1 || (command != "submit" && command != "list" && command != "run") {
		flags.Usage()
		return errors.New("expected the submit, list or run command")
	}
	if !terminalIDPattern.MatchString(*id) {
		return errors.New("terminal ID must contain 1–64 letters, digits, hyphens or underscores")
	}
	if *path == "" {
		return errors.New("database path cannot be empty")
	}
	db, err := queue.Open(ctx, *path)
	if err != nil {
		return err
	}
	defer db.Close()

	if command == "run" {
		return runKafka(ctx, db, *id, *brokers, prompts)
	}

	encoder := json.NewEncoder(out)
	if command == "list" {
		payments, err := db.List(ctx, *id)
		if err != nil {
			return err
		}
		for _, payment := range payments {
			if err := encoder.Encode(payment); err != nil {
				return err
			}
		}
		return nil
	}

	input := bufio.NewScanner(in)
	rawAmount, err := readLine(input, prompts, "Amount (RUB): ")
	if err != nil {
		return err
	}
	amount, err := parseAmount(rawAmount)
	if err != nil {
		return err
	}
	confirmation, err := readLine(input, prompts,
		fmt.Sprintf("Queue %d.%02d RUB? [y/N]: ", amount/100, amount%100))
	if err != nil {
		return err
	}
	if !strings.EqualFold(confirmation, "y") && !strings.EqualFold(confirmation, "yes") {
		fmt.Fprintln(prompts, "Payment cancelled.")
		return nil
	}
	payment, err := db.Enqueue(ctx, *id, amount)
	if err != nil {
		return err
	}
	return encoder.Encode(payment)
}

func readLine(input *bufio.Scanner, out io.Writer, prompt string) (string, error) {
	if _, err := fmt.Fprint(out, prompt); err != nil {
		return "", err
	}
	if !input.Scan() {
		if err := input.Err(); err != nil {
			return "", err
		}
		return "", errors.New("input ended before payment was confirmed")
	}
	return strings.TrimSpace(input.Text()), nil
}

func parseAmount(amount string) (int64, error) {
	if !amountPattern.MatchString(amount) {
		return 0, errors.New("enter a positive amount with up to two decimal places, for example 150.00")
	}
	rubles, kopecks, _ := strings.Cut(strings.ReplaceAll(amount, ",", "."), ".")
	minor, err := strconv.ParseInt(rubles+kopecks+strings.Repeat("0", 2-len(kopecks)), 10, 64)
	if err != nil || minor <= 0 {
		return 0, errors.New("amount is outside the supported positive range")
	}
	return minor, nil
}
