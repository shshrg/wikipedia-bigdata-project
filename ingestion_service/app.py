import json
from requests_sse import EventSource
from kafka import KafkaProducer

URL = 'https://stream.wikimedia.org/v2/stream/page-create'
HEADERS = {"User-Agent": "BigDataProject/1.1"}

producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

with EventSource(URL, headers=HEADERS) as stream:
    for event in stream:
        if event.type == 'message':
            try:
                data = json.loads(event.data)
            except ValueError:
                pass
            else:
                if data['meta']['domain'] == 'canary':
                    continue            
                # print(f"{data['page_title']} created by {data['performer']['user_text']}")
                producer.send('new-pages', data)
