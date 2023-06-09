import json
from sseclient import SSEClient
from confluent_kafka import Producer

kafka_conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(kafka_conf)

url = 'https://stream.wikimedia.org/v2/stream/page-create'
sse_client = SSEClient(url)

for event in sse_client:
    if event.event == 'message':
        try:
            change = json.loads(event.data)
        except ValueError:
            continue
        producer.produce('wikipedia', value=json.dumps(change).encode('utf-8'))

producer.flush()
