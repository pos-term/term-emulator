# term-emulator

> **Language:** English | [Русский](../../README.md)

POS terminal emulator on Raspberry Pi: amount entry and operation confirmation via GPIO (buttons/encoder), transaction status indication, local event queue on network failure, and publishing transactions to Kafka.

> General project information and participants: https://github.com/pos-term/.github/blob/main/i18n/README_EN.md

## Raspberry Pi OS image

Download [Raspberry Pi OS Lite (64-bit)](https://www.raspberrypi.com/software/operating-systems/). Create an `img/` directory at the repository root and keep the downloaded `.img.xz` archive and unpacked `.img` file there. Git ignores both formats in this directory.

## Running on macOS

Install Python 3 and QEMU with the `raspi4b` machine model (`brew install qemu`). First-user setup requires OpenSSL (`brew install openssl`).

On the first run, supply a login name and enter a password when OpenSSL prompts you. For a user named `terminal`, run:

```sh
./scripts/run_pi_macos --user terminal
```

Sign in when `raspberrypi login:` appears. The launcher creates a working disk under `.qemu/`. Subsequent runs reuse it:

```sh
./scripts/run_pi_macos
```

If `img/` contains multiple images, pass the path to the desired `.img` as an argument. QEMU runs offline. To shut down cleanly, run `sudo poweroff` inside the guest.

## Entering a payment and opening the local queue

With Go 1.26 or later, run these commands from the repository root:

```sh
go run ./cmd/terminal submit
go run ./cmd/terminal list
```

Enter an amount in RUB (for example, `150.00`) and confirm with `y`. The terminal stores the payment in SQLite and prints it as JSON. The `list` command reads saved payments in sequence order. The UUIDv7 and sequence number are preserved across restarts.

The defaults are `.terminal/queue.db` and terminal `term-001`. Set the path and terminal ID with flags before the command, for example `go run ./cmd/terminal -db ./demo.db -terminal-id term-002 list`.

Build the executable for 64-bit Raspberry Pi OS. After copying it to the Pi, run it with the `submit` and `list` commands.

```sh
mkdir -p .terminal
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o .terminal/terminal-linux-arm64 ./cmd/terminal
```
