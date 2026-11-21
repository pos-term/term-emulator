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

If `img/` contains multiple images, pass the path to the desired `.img` as an argument. The guest gets its address through DHCP on the QEMU user network. To shut down cleanly, run `sudo poweroff` inside the guest.

## Entering a payment and opening the local queue

With Go 1.26 or later, run these commands from the repository root:

```sh
go run ./cmd/terminal submit
go run ./cmd/terminal list
```

Enter an amount in RUB (for example, `150.00`) and confirm with `y`. The terminal stores the payment in SQLite and prints it as JSON. The `list` command reads saved payments in sequence order. The UUIDv7 and sequence number are preserved across restarts.

The defaults are `.terminal/queue.db` and terminal `term-001`. Set the path and terminal ID with flags before the command, for example `go run ./cmd/terminal -db ./demo.db -terminal-id term-002 list`.

## Sending payments and receiving results

Start Kafka and `payment-processor` from the sibling `infra` repository with `docker compose -f ../infra/docker-compose.yml up -d --build payment-processor`. Start the terminal workers in a separate console.

```sh
go run ./cmd/terminal -brokers localhost:9094 run
```

While `run` is active, use `submit` and `list` in another console with the same `-db` and `-terminal-id`. Use one `run` instance and one database per terminal ID. `-brokers` takes a comma-separated list of addresses and defaults to `localhost:9094`. Press `Ctrl+C` to stop the workers.

In `list` output, `sent_at` appears after Kafka acknowledges the event. The `result` field shows the processor's final `success` or `failed` decision. For failed payments, `reason` contains a code such as `LIMIT_EXCEEDED`. The `run` command also displays decisions as they arrive.

With the default `payment-processor` settings, use `submit` to check both outcomes. Expect `success` for `150.50` and `failed` with `LIMIT_EXCEEDED` for `100000.01`.

Build the executable for 64-bit Raspberry Pi OS and copy it to the Pi.

```sh
mkdir -p .terminal/pi
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -ldflags="-s -w" -o .terminal/pi/terminal ./cmd/terminal
```

## Testing payments in Raspberry Pi OS under QEMU

Use 64-bit Raspberry Pi OS Lite based on Debian 13 (Trixie) for the guest. On macOS, start `infra` with a Kafka address the guest can use from the broker metadata. Then serve the ARM64 executable from a separate console.

```sh
KAFKA_EXTERNAL_HOST=10.0.2.2 docker compose -f ../infra/docker-compose.yml up -d --build payment-processor
python3 -m http.server 8000 --bind 127.0.0.1 --directory .terminal/pi
```

After signing in to the guest, wait for the IPv4 address `10.0.2.15`, then check the route, Kafka port and time. The launcher passes the macOS time to systemd at boot. Compare `date -u` with the host before submitting the first payment, especially after pausing the VM.

```sh
ip -br -4 addr
ip -4 route get 10.0.2.2
timeout 5 bash -c 'exec 3<>/dev/tcp/10.0.2.2/9094'
date -u
```

If the clocks differ and NTP has yet to synchronize, set the time from the HTTP `Date` header returned by the macOS server.

```sh
host_date=$(curl -fsSI http://10.0.2.2:8000/terminal | sed -n 's/^Date: //p' | tr -d '\r')
test -n "$host_date" && sudo date -u -s "$host_date"
```

If `kcat` is installed in the guest, run `kcat -b 10.0.2.2:9094 -L` to verify that Kafka advertises `10.0.2.2:9094`. The terminal uses this address to send payments and read results.

```sh
curl -fSLo terminal http://10.0.2.2:8000/terminal
chmod +x terminal
./terminal -brokers 10.0.2.2:9094 run >terminal.log 2>&1 &
worker=$!
./terminal submit
```

Enter `150.50` and `y`, then repeat `submit` with `100000.01`. Once both decisions appear in `terminal.log`, inspect the saved payments and shut down the guest.

```sh
cat terminal.log
./terminal list
kill -TERM "$worker"
wait "$worker"
sudo poweroff
```
