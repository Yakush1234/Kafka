# Kafka study project

Учебный проект с синхронными producer и consumer на `confluent-kafka`.
Локально используется один Kafka broker в KRaft-режиме, без ZooKeeper.

## Как связаны компоненты

```text
Python producer -> 127.0.0.1:9092 -> topic study.messages
                                      | partition 0 -> consumer 1
                                      | partition 1 -> consumer 2
                                      ` partition 2 -> один из consumers
```

- **Broker** — сервер Kafka. В проекте он работает в Docker.
- **Topic** — именованный поток сообщений внутри Kafka.
- **Partition** — часть topic. Сообщения внутри одной partition упорядочены.
- **Consumer group** — consumers с одинаковым `group.id`. Каждая partition
  назначается только одному активному consumer внутри группы.
- **Offset** — позиция сообщения внутри partition. Kafka хранит прочитанные
  offsets отдельно для каждой consumer group.
- **Replication factor** — число копий partition на разных brokers. Здесь broker
  один, поэтому значение равно `1`.

Три partitions позволяют одновременно работать максимум трём consumers группы.
Четвёртый останется без partition. Если consumers меньше трёх, один процесс
может получить несколько partitions.

## 1. Подготовка

Потребуются Docker Desktop, Python 3.12+, `uv` и, опционально, `just`.

```powershell
uv sync
```

Python-клиенты используют настройки из `.env`:

```dotenv
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
KAFKA_TOPIC=study.messages
KAFKA_CONSUMER_GROUP=study-consumers
KAFKA_AUTO_OFFSET_RESET=earliest
```

`127.0.0.1:9092` доступен программам на компьютере. Внутри Docker-сети Kafka
доступна как `kafka:19092`.

## 2. Запуск Kafka

```powershell
docker compose up -d --wait
```

Или:

```powershell
just kafka-up
```

Проверка состояния и просмотр логов:

```powershell
docker compose ps
docker compose logs -f kafka
```

Первый запуск скачает официальный образ Kafka. Данные сохраняются в Docker
volume, поэтому обычный `docker compose down` их не удаляет.

## 3. Создание topic

Автосоздание topics отключено. Создадим `study.messages` с тремя partitions:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server kafka:19092 `
  --create --if-not-exists `
  --topic study.messages `
  --partitions 3 `
  --replication-factor 1
```

Короткая команда и проверка результата:

```powershell
just topic-create
just topic-describe
```

Другие полезные команды:

```powershell
# Показать topics
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server kafka:19092 --list

# Увеличить число partitions до пяти
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server kafka:19092 --alter `
  --topic study.messages --partitions 5

# Удалить topic вместе с его сообщениями
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server kafka:19092 --delete --topic study.messages
```

Число partitions можно только увеличивать. Для уменьшения topic придётся
пересоздать. После увеличения распределение новых сообщений по ключам может
измениться.

## 4. Запуск consumers

В первом терминале:

```powershell
just consumer
```

Во втором терминале выполните ту же команду:

```powershell
just consumer
```

Или без `just`:

```powershell
uv run python -m app.consumers.sync_consumer
```

Оба процесса используют группу `study-consumers`. При подключении или отключении
consumer Kafka выполняет rebalance и перераспределяет partitions. Назначения
видны в логах каждого процесса.

## 5. Запуск producer

В третьем терминале отправим 20 сообщений с интервалом 0.1 секунды:

```powershell
just producer 20
```

Или без `just`:

```powershell
uv run python -m app.producers.sync_producer 20
```

Producer ждёт подтверждения доставки перед завершением. Ключ сообщения — его
номер; Kafka использует ключ для стабильного выбора partition.

## 6. Consumer groups, offsets и lag

```powershell
# Показать все группы
just groups

# Показать состояние группы приложения
just group-describe
```

`CURRENT-OFFSET` — следующая позиция чтения, `LOG-END-OFFSET` — конец partition,
а `LAG` — число ещё не обработанных группой сообщений.

`KAFKA_AUTO_OFFSET_RESET=earliest` применяется только тогда, когда сохранённого
offset ещё нет. Впоследствии consumer продолжает с сохранённой позиции. Чтобы
прочитать данные независимо с начала, можно временно указать новое значение
`KAFKA_CONSUMER_GROUP` в `.env`.

## 7. Асинхронные producer и consumer

Async-версия использует отдельные настройки:

```dotenv
KAFKA_ASYNC_TOPIC=study.async.messages
KAFKA_ASYNC_CONSUMER_GROUP=study-async-consumers
```

Сначала создайте отдельный topic с тремя partitions:

```powershell
just async-topic-create
```

Запустите один или несколько async consumers в отдельных терминалах:

```powershell
just async-consumer
```

Затем отправьте сообщения:

```powershell
just async-producer 20
```

Эквивалентные команды без `just`:

```powershell
uv run python -m app.consumers.async_consumer
uv run python -m app.producers.async_producer 20
```

Посмотреть offsets и lag отдельной async-группы:

```powershell
just async-group-describe
```

Async consumer отключает автоматический commit. После успешной обработки он
сохраняет и подтверждает offset вручную. Если процесс завершится между обработкой
и commit, сообщение будет доставлено повторно — это семантика **at-least-once**.
Обработчик должен учитывать возможность повторной доставки.

`AIOProducer` и `AIOConsumer` не блокируют asyncio event loop во время ожидания
Kafka. Это полезно, когда рядом выполняются другие сетевые или дисковые операции.
Для простого автономного скрипта синхронная версия обычно проще.

## 8. Остановка и очистка

Consumers останавливаются сочетанием `Ctrl+C`. Остановить Kafka с сохранением
данных:

```powershell
just kafka-down
```

Полностью удалить broker и volume со всеми сообщениями и offsets:

```powershell
docker compose down -v
```

После удаления volume topic нужно создать заново.

> Конфигурация предназначена для обучения: один broker, replication factor `1`,
> соединение PLAINTEXT без аутентификации и шифрования.

## Статические проверки

Инструменты разработчика устанавливаются отдельной группой:

```powershell
uv sync --extra dev
```

Ruff проверяет стиль, подозрительные конструкции и неиспользуемый код:

```powershell
just lint
```

Mypy статически проверяет согласованность типов, не запуская программу:

```powershell
just typecheck
```

Запустить обе проверки:

```powershell
just check
```
