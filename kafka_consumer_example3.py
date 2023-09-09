from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata
import json

topic_name = "test"
bootstrap_servers = ["localhost:9092", "localhost:9093", "localhost:9094"]
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    group_id="group1",
    enable_auto_commit=False,
)

for msg in consumer:
    topic = TopicPartition(msg.topic, msg.partition)
    om = OffsetAndMetadata(msg.offset + 1, msg.timestamp)
    print(topic)
    print(om)
    d = dict()
    d[topic] = om
    consumer.commit(d)
