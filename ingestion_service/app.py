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
                msg = {
                    'page_title': data['page_title'],
                    'user_name': data['performer']['user_text'],
                    'user_is_bot': data['performer']['user_is_bot'],
                    'user_edit_count': data['performer']['user_edit_count'],
                    'domain': data['meta']['domain'],
                    'dt': data['dt']
                }
                producer.send('new-pages', data)