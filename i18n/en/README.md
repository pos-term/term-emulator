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
