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

# Show partitions of the study topic
topic-describe:
    docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --describe --topic study.messages

# List consumer groups
groups:
    docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:19092 --list

# Show offsets and lag of the application group
group-describe:
    docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:19092 --describe --group study-consumers

# Send N messages, for example: just producer 10
producer count:
    uv run python -m app.producers.first_producer {{count}}

# Start one consumer (run in several terminals to form one consumer group)
consumer:
    uv run python -m app.consumers.first_consumer
