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

Если в `img/` несколько образов, передайте путь к нужному `.img` аргументом. Гость получает адрес через DHCP в пользовательской сети QEMU. Для штатного завершения выполните `sudo poweroff` внутри гостя.

## Ввод платежа и локальная очередь

С Go 1.26 или новее запустите из корня репозитория:

```sh
go run ./cmd/terminal submit
go run ./cmd/terminal list
```

В первой команде введите сумму в рублях (например, `150.00`) и подтвердите её через `y`. Терминал сохранит операцию в SQLite и выведет её в JSON. Команда `list` прочитает сохранённые операции в порядке их номеров. После перезапуска у записи остаются тот же UUIDv7 и порядковый номер.

По умолчанию используется база `.terminal/queue.db` и терминал `term-001`. Путь и идентификатор можно задать флагами перед командой, например `go run ./cmd/terminal -db ./demo.db -terminal-id term-002 list`.

## Отправка платежей и получение результатов

Поднимите Kafka и `payment-processor` из соседнего репозитория `infra` командой `docker compose -f ../infra/docker-compose.yml up -d --build payment-processor`. В отдельной консоли запустите воркеры терминала.

```sh
go run ./cmd/terminal -brokers localhost:9094 run
```

Пока работает `run`, выполняйте `submit` и `list` в другой консоли с теми же `-db` и `-terminal-id`. Используйте один экземпляр `run` и одну базу на идентификатор терминала. `-brokers` принимает адреса через запятую и по умолчанию использует `localhost:9094`. Остановить воркеры можно через `Ctrl+C`.

В выводе `list` поле `sent_at` появляется после подтверждения Kafka. Поле `result` содержит финальное решение процессора `success` или `failed`. При отказе `reason` содержит код причины, например `LIMIT_EXCEEDED`. `run` также выводит полученные решения в консоль.

При настройках `payment-processor` по умолчанию проверьте оба исхода через `submit`. Для `150.50` ожидается `success`, для `100000.01` — `failed` с причиной `LIMIT_EXCEEDED`.

Для Raspberry Pi OS 64-bit соберите бинарник и перенесите его на Pi.

```sh
mkdir -p .terminal/pi
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -ldflags="-s -w" -o .terminal/pi/terminal ./cmd/terminal
```

## Проверка платежа в Raspberry Pi OS под QEMU

Для гостя используйте Raspberry Pi OS Lite 64-bit на базе Debian 13 (Trixie). На macOS запустите `infra` с адресом Kafka, доступным гостю в метаданных брокера. Затем раздайте собранный ARM64-бинарник из отдельной консоли.

```sh
KAFKA_EXTERNAL_HOST=10.0.2.2 docker compose -f ../infra/docker-compose.yml up -d --build payment-processor
python3 -m http.server 8000 --bind 127.0.0.1 --directory .terminal/pi
```

После входа в гостя дождитесь IPv4-адреса `10.0.2.15`, затем проверьте маршрут, порт Kafka и время. Лаунчер передаёт время macOS в systemd при загрузке. Сверьте `date -u` с хостом перед первым платежом, особенно после паузы VM.

```sh
ip -br -4 addr
ip -4 route get 10.0.2.2
timeout 5 bash -c 'exec 3<>/dev/tcp/10.0.2.2/9094'
date -u
```

Если часы расходятся и NTP ещё не синхронизировался, выставьте время по HTTP-заголовку `Date` от запущенного на macOS сервера.

```sh
host_date=$(curl -fsSI http://10.0.2.2:8000/terminal | sed -n 's/^Date: //p' | tr -d '\r')
test -n "$host_date" && sudo date -u -s "$host_date"
```

Если в госте установлен `kcat`, команда `kcat -b 10.0.2.2:9094 -L` позволяет проверить, что Kafka объявляет `10.0.2.2:9094`. Следующие команды используют этот адрес для отправки и чтения результатов.

```sh
curl -fSLo terminal http://10.0.2.2:8000/terminal
chmod +x terminal
./terminal -brokers 10.0.2.2:9094 run >terminal.log 2>&1 &
worker=$!
./terminal submit
```

Введите `150.50` и `y`, затем повторите `submit` с `100000.01`. После появления решений в `terminal.log` проверьте сохранённые платежи и завершите гостя.

```sh
cat terminal.log
./terminal list
kill -TERM "$worker"
wait "$worker"
sudo poweroff
```

## Проверки доставки

```sh
go test ./...
go test -race ./...
go vet ./...
```

Из корня репозитория запустите проверку повторной доставки на локальном тестовом стенде с Kafka и `payment-processor`.

```sh
KAFKA_EXTERNAL_HOST=localhost docker compose -f ../infra/docker-compose.yml up -d --build payment-processor
TEST_KAFKA_E2E_BROKERS=localhost:9094 go test -v -count=1 -run '^TestPaymentReplayWithKafka$' ./internal/kafka
```

Тест создаёт отдельный идентификатор терминала и временную базу SQLite. Он воспроизводит остановку после подтверждения Kafka и до записи `sent_at`, сверяет обе копии платежа в Kafka и проверяет, что процессор опубликовал одно решение. Затем тест останавливает consumer после записи результата и до коммита offset, публикует копию результата и проверяет, что после перезапуска обе доставки проходят без изменений сохранённой записи.

Для ручной проверки временно остановите Kafka командой `docker compose -f ../infra/docker-compose.yml pause kafka` и выполните `submit`. Пока Kafka на паузе, `list` показывает сохранённый платёж без `sent_at`. Остановите `run`, выполните `docker compose -f ../infra/docker-compose.yml unpause kafka` и запустите `run` с теми же `-db` и `-terminal-id`. После обработки в `list` у того же `transaction_id` появится `result`.

После длительной остановки терминал может дочитать результаты в пределах срока хранения `pos.transaction-results.v1`. На стенде `infra` этот срок составляет три дня.
