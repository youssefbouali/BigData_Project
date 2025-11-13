# test_producer.py
from kafka import KafkaProducer
import json, time, random

producer = KafkaProducer(bootstrap_servers='localhost9092')
while True
    data = {
        src_ip f192.168.1.{random.randint(1,255)},
        dst_ip 10.0.0.1,
        src_port random.randint(1000, 60000),
        dst_port 80,
        protocol 6,
        bytes_in random.randint(100, 100000),
        bytes_out random.randint(100, 50000),
        pkt_in random.randint(1, 100),
        pkt_out random.randint(1, 50)
    }
    producer.send('netflow-data', json.dumps(data).encode('utf-8'))
    time.sleep(0.1)