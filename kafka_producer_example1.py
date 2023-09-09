import json
from kafka import *
from kafka.admin import KafkaAdminClient, NewTopic

topic_name = "test"
partitions = 3
replication = 3
admin_client = KafkaAdminClient(
    bootstrap_servers=["localhost:9092", "localhost:9093", "localhost:9094"]
)

print("List topics:", admin_client.list_topics())

if topic_name not in admin_client.list_topics():
    new_topic = NewTopic(
        name=topic_name, num_partitions=partitions, replication_factor=replication
    )
    admin_client.create_topics(new_topics=[new_topic])

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092", "localhost:9093", "localhost:9094"],
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    key_serializer=lambda x: json.dumps(x).encode("utf-8"),
    acks="all",
)


for i in range(5):
    name = input("provide name:")
    data = {name: i}
    producer.send(topic_name, value=data)
