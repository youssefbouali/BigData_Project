# /scripts/realtime_capture_kafka.py
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
    acks=0,               # أسرع
    linger_ms=1,
    batch_size=16384
)

topic = "packets_raw"

def handle(pkt):
    if not pkt.haslayer(IP): return
    ip = pkt[IP]

    # تجاهل باكتات Kafka تمامًا
    if pkt.haslayer(TCP) and (pkt[TCP].dport == 9092 or pkt[TCP].sport == 9092):
        return

    try:
        src_port = pkt[TCP].sport if pkt.haslayer(TCP) else (pkt[UDP].sport if pkt.haslayer(UDP) else 0)
        dst_port = pkt[TCP].dport if pkt.haslayer(TCP) else (pkt[UDP].dport if pkt.haslayer(UDP) else 0)
        tcp_flags = int(pkt[TCP].flags) if pkt.haslayer(TCP) else 0

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
    except: pass

print("REAL-TIME CAPTURE → Kafka STARTED (Kafka packets ignored)")
sniff(prn=handle, store=False, iface="eth0")