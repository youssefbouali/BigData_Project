
import os, time, json
import pandas as pd
from kafka import KafkaProducer
KAFKA = os.environ.get('KAFKA_BOOTSTRAP', 'localhost:9092')
TOPIC = 'netflow-data'
producer = KafkaProducer(bootstrap_servers=[KAFKA], value_serializer=lambda v: json.dumps(v).encode('utf-8'))
print("[producer] starting, connecting to", KAFKA)
while True:
    files = sorted([f for f in os.listdir('/output') if f.endswith('.csv')])
    if not files:
        time.sleep(3)
        continue
    for f in files:
        path = os.path.join('/output', f)
        print("[producer] processing", path)
        try:
            df = pd.read_csv(path)
        except Exception as e:
            print("read error", e)
            os.rename(path, path + ".bad")
            continue
        for _, row in df.iterrows():
            payload = row.to_dict()
            payload['source_file'] = f
            producer.send(TOPIC, payload)
        producer.flush()
        # move processed file
        os.rename(path, path + ".processed")
    time.sleep(1)
