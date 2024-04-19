from google.cloud import pubsub_v1
import json

project_id = "dataengps1"
topic_id = "my-topic"
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

with open('bcsample.json', 'r') as file:
    records = json.load(file)

for record in records:
    data_str = json.dumps(record)
    data = data_str.encode("utf-8")
    future = publisher.publish(topic_path, data)
    print(f"Published: {future.result()}")

print(f"Published {len(records)} messages to {topic_path}.")