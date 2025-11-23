# /scripts/fake_netflow_producer.py
import json
import random
import time
from kafka import KafkaProducer

KAFKA_BROKERS = "kafka:9092"
TOPIC = "netflow-data"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def ip_to_int(ip):
    return int("".join([f"{int(x):02x}" for x in ip.split(".")]), 16)

def generate_flow():
    src_ip = f"192.168.{random.randint(0,255)}.{random.randint(1,254)}"
    dst_ip = f"10.0.{random.randint(0,255)}.{random.randint(1,254)}"
    return {
        "src_ip": src_ip,
        "dst_ip": dst_ip,
        "src_ip_int": ip_to_int(src_ip),
        "dst_ip_int": ip_to_int(dst_ip),
        "src_port": random.randint(1024, 65535),
        "dst_port": random.choice([80,443,22,53]),
        "protocol": random.choice([6,17]),
        "l7_proto": 7.0,
        "in_bytes": random.randint(40, 1500),
        "out_bytes": random.randint(40, 1500),
        "in_pkts": random.randint(1, 20),
        "out_pkts": random.randint(1, 20),
        "duration_ms": random.randint(10, 1000),
        "tcp_flags": random.randint(0, 255),
        "client_tcp_flags": 0,
        "server_tcp_flags": 0,
        "src_to_dst_avg_throughput": 0.0,
        "dst_to_src_avg_throughput": 0.0,
        "num_pkts_up_to_128_bytes": 0,
        "num_pkts_128_to_256_bytes": 0,
        "num_pkts_256_to_512_bytes": 0,
        "num_pkts_512_to_1024_bytes": 0,
        "num_pkts_1024_to_1514_bytes": 0,
        "attack_type": "Benign"
    }

while True:
    flow = generate_flow()
    producer.send(TOPIC, value=flow)
    print("Sent flow:", flow)
    time.sleep(0.1)  # 10 flows/sec