from kafka import KafkaProducer
import pandas as pd, json, time

producer = KafkaProducer(bootstrap_servers=['kafka:9092'])
csv_path = '/tmp/flows.csv'

while True:
    df = pd.read_csv(csv_path)
    for _, row in df.iterrows():
        producer.send('netflow-data', value=row.to_json().encode('utf-8'))
    print("✅ Données envoyées à Kafka")
    time.sleep(5)
