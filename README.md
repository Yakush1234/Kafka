# Kafka study project

Учебный проект с синхронными и асинхронными producer/consumer на
`confluent-kafka`. Локально используется один Kafka broker в KRaft-режиме, без
ZooKeeper.

## Как связаны компоненты

```text
producer -> 127.0.0.1:9092 -> topic
                                 | partition 0 -> consumer 1
                                 | partition 1 -> consumer 2
                                 ` partition 2 -> один из consumers
```

- **Broker** — сервер Kafka, запущенный в Docker.
- **Topic** — именованный поток сообщений.
- **Partition** — часть topic. Сообщения внутри одной partition упорядочены.
- **Consumer group** — consumers с одинаковым `group.id`. Каждая partition
  назначается только одному активному consumer внутри группы.
- **Offset** — позиция сообщения внутри partition. Kafka хранит прочитанные
  offsets отдельно для каждой consumer group.
- **Replication factor** — количество копий partition. В учебном кластере один
  broker, поэтому replication factor равен `1`.

Три partitions позволяют одновременно работать максимум трём consumers одной
группы. Четвёртый consumer останется без partition. Если consumers меньше трёх,
один процесс может получить несколько partitions.

## 1. Подготовка

Потребуются:

- Docker Desktop;
- Python 3.12+;
- `uv`;
- `just`.

Создайте локальный `.env` на основе `.env.example`:

```powershell
just env-init
```

Установите зависимости:

```powershell
just install
```

Основные настройки:

```dotenv
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
KAFKA_TOPIC=study.messages
KAFKA_CONSUMER_GROUP=study-consumers
KAFKA_ASYNC_TOPIC=study.async.messages
KAFKA_ASYNC_CONSUMER_GROUP=study-async-consumers
```

## 2. Запуск Kafka

Запустите broker:

```powershell
just kafka-up
```

Проверьте состояние контейнера:

```powershell
just kafka-status
```

Для просмотра логов Kafka:

```powershell
just kafka-logs
```

Первый запуск скачает официальный образ Kafka. Данные сохраняются в Docker
volume и не удаляются при обычной остановке broker.

## 3. Создание topics

Автоматическое создание topics отключено. Создайте topic для синхронных клиентов:

```powershell
just topic-create
```

Создайте отдельный topic для асинхронных клиентов:

```powershell
just async-topic-create
```

Проверьте синхронный topic и его partitions:

```powershell
just topic-describe
```

Другие команды управления topics:

```powershell
# Показать все topics
just topic-list

# Увеличить количество partitions до пяти
just topic-alter 5

# Удалить синхронный topic вместе с сообщениями
just topic-delete
```

Количество partitions можно только увеличивать. Для уменьшения topic необходимо
пересоздать. После увеличения распределение новых сообщений по ключам может
измениться.

## 4. Синхронные consumer и producer

Запустите consumer в первом терминале:

```powershell
just consumer
```

При необходимости запустите эту же команду во втором терминале. Оба процесса
войдут в группу `study-consumers`, после чего Kafka распределит между ними
partitions.

В отдельном терминале отправьте 20 сообщений:

```powershell
just producer 20
```

Producer создаёт сообщения с интервалом 0.1 секунды и перед завершением ожидает
подтверждения их доставки.

## 5. Асинхронные consumer и producer

Запустите один или несколько async consumers в отдельных терминалах:

```powershell
just async-consumer
```

Отправьте 20 сообщений в отдельный async topic:

```powershell
just async-producer 20
```

Async consumer использует ручной commit после успешной обработки сообщения. Если
процесс завершится между обработкой и commit, Kafka может доставить сообщение
повторно. Это семантика **at-least-once**, поэтому обработчик должен учитывать
возможность повторной доставки.

## 6. Consumer groups, offsets и lag

Показать все consumer groups:

```powershell
just groups
```

Состояние синхронной группы:

```powershell
just group-describe
```

Состояние асинхронной группы:

```powershell
just async-group-describe
```

`CURRENT-OFFSET` — следующая позиция чтения, `LOG-END-OFFSET` — конец partition,
а `LAG` — количество ещё не обработанных группой сообщений.

`KAFKA_AUTO_OFFSET_RESET=earliest` применяется только тогда, когда у группы нет
сохранённого offset. В дальнейшем consumer продолжает с сохранённой позиции. Для
независимого чтения с начала можно задать новое имя consumer group в `.env`.

## 7. Статические проверки

Отформатировать Python-код:

```powershell
just formatter
```

Запустить Ruff:

```powershell
just lint
```

Запустить mypy:

```powershell
just typecheck
```

Проверить форматирование, линтер и типы одной командой:

```powershell
just check
```

## 8. Остановка и очистка

Consumers останавливаются сочетанием `Ctrl+C`. Остановить Kafka с сохранением
данных:

```powershell
just kafka-down
```

Полностью удалить broker, topics, сообщения и offsets:

```powershell
just kafka-clean
```

После полной очистки topics нужно создать заново.

> Эта конфигурация предназначена для обучения: один broker, replication factor
> `1`, соединение PLAINTEXT без аутентификации и шифрования.

Полный список коротких команд можно вывести через `just --list`. Их развёрнутые
варианты без `just` находятся в [justfile](./justfile).
