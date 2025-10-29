
#!/bin/bash
set -euo pipefail
echo "[pcap_processor] starting processing loop..."
mkdir -p /output
while true; do
  # find one pcap file in /pcap
  pcap=$(ls /pcap/*.pcap 2>/dev/null | head -n1 || true)
  if [ -n "$pcap" ]; then
    echo "[pcap_processor] found pcap: $pcap"
    base=$(basename "$pcap" .pcap)
    outflow="/output/flows_${base}.csv"
    # try to convert pcap -> nfcapd (nfpcapd) then to csv with nfdump if available
    if command -v nfpcapd >/dev/null 2>&1 ; then
      tmpdir=/tmp/nfcd_$base
      mkdir -p $tmpdir
      nfpcapd -r "$pcap" -l $tmpdir || true
      nfdump -r $tmpdir/nfcapd.* -o csv > "$outflow" || true
    fi
    # fallback: if CICFlowMeter.jar present, run it
    if [ -f /cicflowmeter/CICFlowMeter.jar ]; then
      echo "[pcap_processor] running CICFlowMeter.jar for richer CSV features"
      java -jar /cicflowmeter/CICFlowMeter.jar -r "$pcap" -f "$outflow" || true
    fi
    # if still not produced, try tshark quick CSV
    if [ ! -s "$outflow" ]; then
      echo "[pcap_processor] fallback: using tshark to extract basic flows"
      tshark -r "$pcap" -T fields -e frame.time_epoch -e ip.src -e ip.dst -e tcp.srcport -e tcp.dstport -e udp.srcport -e udp.dstport -E header=y -E separator=, > "$outflow" || true
    fi
    mv "$pcap" "/pcap/processed_$base.pcap"
    echo "[pcap_processor] produced $outflow"
  else
    sleep 5
  fi
done
