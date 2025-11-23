# /scripts/capture_only.py
# بس يلتقط ويرسل لـ Kafka – مفيش أي Spark ولا scapy مع Spark

from scapy.all import sniff, IP, TCP, UDP
from kafka import KafkaProducer
import json
import socket
import struct
import time

def ip_to_int(ip):
    try:
        return struct.unpack("!I", socket.inet_aton(ip))[0]
    except:
        return 0

producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks=0,
    linger_ms=1
)

topic = "packets_raw"

def handle(pkt):
    if not pkt.haslayer(IP): return
    ip = pkt[IP]

    # تجاهل باكتات Kafka
    if pkt.haslayer(TCP) and (pkt[TCP].sport == 9092 or pkt[TCP].dport == 9092):
        return

    src_port = dst_port = tcp_flags = 0
    if pkt.haslayer(TCP):
        src_port = pkt[TCP].sport
        dst_port = pkt[TCP].dport
        tcp_flags = int(pkt[TCP].flags)
    elif pkt.haslayer(UDP):
        src_port = pkt[UDP].sport
        dst_port = pkt[UDP].dport

    data = {
        "src_ip": ip.src,
        "dst_ip": ip.dst,
        "src_ip_int": ip_to_int(ip.src),
        "dst_ip_int": ip_to_int(ip.dst),
        "src_port": src_port,
        "dst_port": dst_port,
        "protocol": ip.proto,
        "pkt_len": len(pkt),
        "tcp_flags": tcp_flags,
        "ttl": ip.ttl,
        "timestamp": time.time()
    }
    producer.send(topic, value=data)
    print(f"→ {ip.src}:{src_port} → {ip.dst}:{dst_port}")

print("CAPTURE ONLY STARTED → Kafka (no Spark here!)")
sniff(prn=handle, store=False, iface="eth0")