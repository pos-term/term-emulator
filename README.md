# term-emulator

> **Язык / Language:** Русский | [English](i18n/en/README.md)

Эмулятор POS-терминала на Raspberry Pi: приём суммы и подтверждения операции через GPIO (кнопки/энкодер), индикация статуса транзакции, локальная очередь событий при потере сети и публикация транзакций в Kafka.

> Общая информация по проекту и участниках: https://github.com/pos-term/.github/blob/main/profile/README.md

## Образ Raspberry Pi OS

Скачайте [Raspberry Pi OS Lite (64-bit)](https://www.raspberrypi.com/software/operating-systems/). Создайте папку `img/` в корне репозитория и сохраните туда скачанный архив `.img.xz` и распакованный образ `.img`. Оба формата в этой папке игнорируются Git.

## Запуск на macOS

Нужны Python 3 и QEMU с моделью `raspi4b` (`brew install qemu`). Для создания первого пользователя нужен OpenSSL (`brew install openssl`).

При первом запуске укажите логин и введите пароль по запросу OpenSSL. Например, для пользователя `terminal`:

```sh
./scripts/run_pi_macos --user terminal
```

Когда появится `raspberrypi login:`, войдите с этим логином и паролем. Скрипт сохранит рабочий диск в `.qemu/`. Последующие запуски используют тот же диск:

```sh
./scripts/run_pi_macos
```

Если в `img/` несколько образов, передайте путь к нужному `.img` аргументом. QEMU пока запускается без сети. Для штатного завершения выполните `sudo poweroff` внутри гостя.

