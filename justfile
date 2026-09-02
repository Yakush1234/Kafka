set shell := ["powershell.exe", "-NoLogo", "-Command"]

# Start Kafka in the background
kafka-up:
    docker compose up -d --wait

# Stop Kafka, preserving its data
kafka-down:
    docker compose down

# Follow broker logs
kafka-logs:
    docker compose logs -f kafka

# Create the study topic with three partitions
topic-create:
    docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --create --if-not-exists --topic study.messages --partitions 3 --replication-factor 1

# Create a separate topic for asyncio examples
async-topic-create:
    docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --create --if-not-exists --topic study.async.messages --partitions 3 --replication-factor 1

# Show partitions of the study topic
topic-describe:
    docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --describe --topic study.messages

# List consumer groups
groups:
    docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:19092 --list

# Show offsets and lag of the application group
group-describe:
    docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:19092 --describe --group study-consumers

# Show offsets and lag of the async consumer group
async-group-describe:
    docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:19092 --describe --group study-async-consumers

# Send N messages, for example: just producer 10
producer count:
    uv run python -m app.producers.sync_producer {{count}}

# Start one consumer (run in several terminals to form one consumer group)
consumer:
    uv run python -m app.consumers.sync_consumer

# Send N messages asynchronously
async-producer count:
    uv run python -m app.producers.async_producer {{count}}

# Start an asyncio consumer
async-consumer:
    uv run python -m app.consumers.async_consumer

# Run Ruff linter
lint:
    uv run --extra dev ruff check app

# Run mypy static type checker
typecheck:
    uv run --extra dev mypy

# Run all static checks (without tests)
check: lint typecheck
