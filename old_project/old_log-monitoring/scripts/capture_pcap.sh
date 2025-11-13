#!/bin/bash
# Capture réseau avec rotation
mkdir -p /var/log/pcap
sudo tcpdump -i eth0 -s 0 -W 5 -C 100 -w /var/log/pcap/capture-%Y%m%d%H%M.pcap
