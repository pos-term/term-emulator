# term-emulator

> **Язык / Language:** Русский | [English](i18n/en/README.md)

Эмулятор POS-терминала на Raspberry Pi: приём суммы и подтверждения операции через GPIO (кнопки/энкодер), индикация статуса транзакции, локальная очередь событий при потере сети и публикация транзакций в Kafka.

> Общая информация по проекту и участниках: https://github.com/pos-term/.github/blob/main/profile/README.md

## Образ Raspberry Pi OS

Скачайте [Raspberry Pi OS Lite (64-bit)](https://www.raspberrypi.com/software/operating-systems/). Создайте папку `img/` в корне репозитория и сохраните туда скачанный архив `.img.xz` и распакованный образ `.img`. Оба формата в этой папке игнорируются Git.

