#!/bin/bash
INPUT=$1
OUTPUT="/tmp/flows.csv"

nfpcapd -r "$INPUT" -l /tmp/netflow/
nfdump -r /tmp/netflow/nfcapd.* -o csv > "$OUTPUT"
echo "✅ Export CSV terminé: $OUTPUT"
