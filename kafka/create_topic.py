"""Create a Kafka topic."""

import plac
from kafka.admin import KafkaAdminClient, NewTopic


@plac.opt("addr", "address", type=str)
@plac.opt("topic", "topic", type=str)
def main(addr: str = "127.0.0.1:9092", topic: str = "iot"):
    """Use Kafka Admin client to create a topic."""
    client = KafkaAdminClient(bootstrap_servers=addr, api_version=(3, 3))
    client.create_topics(new_topics=[NewTopic(topic, 1, 1)])
    print(f"Created topic {topic}")


if __name__ == "__main__":
    plac.call(main)
