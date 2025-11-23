# /scripts/realtime_detector.py
# خفيف جدًا – بدون أي PySpark أو Spark objects
from scapy.all import sniff, IP, TCP, UDP
import socket
import struct
import time

def ip_to_int(ip):
    return struct.unpack("!I", socket.inet_aton(ip))[0]

def send_packet_data(pkt):
    if not pkt.haslayer(IP):
        return
    try:
        ip = pkt[IP]
        src_ip = ip.src
        dst_ip = ip.dst
        proto = ip.proto
        pkt_len = len(pkt)

        src_port = dst_port = tcp_flags = 0
        if pkt.haslayer(TCP):
            src_port = pkt[TCP].sport
            dst_port = pkt[TCP].dport
            tcp_flags = int(pkt[TCP].flags)
        elif pkt.haslayer(UDP):
            src_port = pkt[UDP].sport
            dst_port = pkt[UDP].dport

        # نرسل البيانات عبر UNIX socket للـ process التاني
        data = struct.pack("!IIIIIiI",
            ip_to_int(src_ip), ip_to_int(dst_ip),
            src_port, dst_port, proto,
            pkt_len, tcp_flags
        )
        sock.send(data)
    except:
        pass

# إنشاء UNIX socket للتواصل مع المعالج
sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
sock.connect("/tmp/spark_detector.sock")

print("Real-time packet capture STARTED (sending to Spark processor...)")
sniff(prn=send_packet_data, store=False, iface="eth0")