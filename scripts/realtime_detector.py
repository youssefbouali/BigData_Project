# /scripts/realtime_capture_redis.py
# Captures packets and pushes them to Redis (ignores Redis traffic)

from scapy.all import sniff, IP, TCP, UDP
import redis
import json
import socket
import struct
import time

# Connect to Redis (make sure redis service exists in docker-compose)
r = redis.Redis(host='172.17.0.1', port=6379, db=0, decode_responses=False)

def ip_to_int(ip):
    try:
        return struct.unpack("!I", socket.inet_aton(ip))[0]
    except:
        return 0

def handle_packet(pkt):
    if not pkt.haslayer(IP):
        return

    ip = pkt[IP]

    # Ignore Redis traffic (port 6379) to prevent loop
    if pkt.haslayer(TCP) and (pkt[TCP].dport == 6379 or pkt[TCP].sport == 6379):
        return

    src_port = dst_port = tcp_flags = 0
    if pkt.haslayer(TCP):
        src_port = pkt[TCP].sport
        dst_port = pkt[TCP].dport
        tcp_flags = int(pkt[TCP].flags)
    elif pkt.haslayer(UDP):
        src_port = pkt[UDP].sport
        dst_port = pkt[UDP].dport

    packet_data = {
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

    # Push to Redis list (queue)
    r.rpush("packets_queue", json.dumps(packet_data))
    print(f"→ {ip.src}:{src_port} → {ip.dst}:{dst_port} (Len: {len(pkt)})")

print("REAL-TIME PACKET CAPTURE → Redis STARTED (port 6379 traffic ignored)")
sniff(prn=handle_packet, store=False, iface="eth0")