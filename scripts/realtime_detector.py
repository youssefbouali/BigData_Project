# /scripts/realtime_capture_kafka.py
# يلتقط الباكتات من الشبكة ويرسلها لـ Kafka
# يتجاهل تمامًا باكتات Kafka نفسها (port 9092) عشان ما يعملش loop لا نهائي

from scapy.all import sniff, IP, TCP, UDP
from kafka import KafkaProducer
import json
import socket
import struct
import time

# تحويل IP إلى Integer
def ip_to_int(ip):
    try:
        return struct.unpack("!I", socket.inet_aton(ip))[0]
    except:
        return 0

# إعداد Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    acks='all',
    retries=3,
    batch_size=16384,
    linger_ms=5
)

topic = "packets_raw"

def handle_packet(pkt):
    if not pkt.haslayer(IP):
        return

    ip = pkt[IP]
    src_ip = ip.src
    dst_ip = ip.dst

    # تجاهل كل باكتات Kafka (من وإلى port 9092)
    if pkt.haslayer(TCP):
        if pkt[TCP].sport == 9092 or pkt[TCP].dport == 9092:
            return
    if pkt.haslayer(UDP):
        if pkt[UDP].sport == 9092 or pkt[UDP].dport == 9092:
            return

    try:
        proto = ip.proto
        pkt_len = len(pkt)
        ttl = ip.ttl

        src_port = dst_port = tcp_flags = 0
        if pkt.haslayer(TCP):
            src_port = pkt[TCP].sport
            dst_port = pkt[TCP].dport
            tcp_flags = int(pkt[TCP].flags)
        elif pkt.haslayer(UDP):
            src_port = pkt[UDP].sport
            dst_port = pkt[UDP].dport

        packet_data = {
            "src_ip": src_ip,
            "dst_ip": dst_ip,
            "src_ip_int": ip_to_int(src_ip),
            "dst_ip_int": ip_to_int(dst_ip),
            "src_port": src_port,
            "dst_port": dst_port,
            "protocol": proto,
            "pkt_len": pkt_len,
            "tcp_flags": tcp_flags,
            "ttl": ttl,
            "timestamp": time.time()
        }

        producer.send(topic, value=packet_data)
        
        # طباعة خفيفة (فقط الباكتات الحقيقية)
        print(f"→ {src_ip}:{src_port} → {dst_ip}:{dst_port} (Len: {pkt_len})")

    except Exception as e:
        pass  # تجاهل أي خطأ

print("Real-time Packet Capture → Kafka STARTED")
print("Kafka traffic (port 9092) is IGNORED to prevent loop")
print("Listening on eth0...")

sniff(prn=handle_packet, store=False, iface="eth0")