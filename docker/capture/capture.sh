#!/bin/bash

INTERFACE=${INTERFACE:-eth0}
PCAP_DIR=/pcap
FLOW_DIR=/flows
CSV_DIR=/csv
KAFKA_BROKERS=${KAFKA_BROKERS:-kafka:9092}
TOPIC=${TOPIC:-netflow-data}

echo "Starting capture on $INTERFACE..."

# 100mb 10 files
tcpdump -i $INTERFACE -s 0 -W 10 -C 100 -w $PCAP_DIR/capture-%Y%m%d%H%M.pcap &

# wait new files
inotifywait -m $PCAP_DIR -e create -e moved_to |
    while read path action file; do
        if [[ "$file" =~ \.pcap$ ]]; then
            echo "New PCAP: $file"
            sleep 5  # wait exit

            PCAP_FILE="$PCAP_DIR/$file"
            FLOW_FILE="$FLOW_DIR/nfcapd.$(date +%Y%M%d%H%m)"
            CSV_FILE="$CSV_DIR/flows_$(date +%s).csv"

            # 1. convert PCAP → NetFlow
            nfpcapd -r "$PCAP_FILE" -l "$FLOW_DIR/" -t 60 -w

            # 2. export CSV
            nfdump -r "$FLOW_DIR"/nfcapd.* -o csv > "$CSV_FILE.tmp"
            mv "$CSV_FILE.tmp" "$CSV_FILE"

            # 3. extraction CICFlowMeter (if available)
            if [ -f /CICFlowMeter.jar ]; then
                java -jar /CICFlowMeter.jar -r "$PCAP_FILE" -f "$CSV_FILE.cic" || echo "CICFlowMeter failed, continuing without CIC features"
                # 4. merge (NetFlow + CIC)
                python3 /convert_and_send.py \
                    --netflow "$CSV_FILE" \
                    --cic "$CSV_FILE.cic" \
                    --kafka "$KAFKA_BROKERS" \
                    --topic "$TOPIC"
            else
                # 4. send only NetFlow (no CIC)
                python3 /convert_and_send.py \
                    --netflow "$CSV_FILE" \
                    --kafka "$KAFKA_BROKERS" \
                    --topic "$TOPIC"
            fi

            # clean
            rm -f "$PCAP_FILE" "$CSV_FILE.cic"
        fi
    done