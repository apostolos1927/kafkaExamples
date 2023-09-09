from kafka import KafkaConsumer, TopicPartition
import json

topic_name = "test"
bootstrap_servers = ["localhost:9092", "localhost:9093", "localhost:9094"]
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

consumer.assign([TopicPartition(topic_name, 1)])

for msg in consumer:
    print(msg.value)
