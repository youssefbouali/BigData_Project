import pandas as pd
import json
import argparse
from kafka import KafkaProducer
import time

def merge_and_send(netflow_path, cic_path, kafka_brokers, topic):
    # read NetFlow CSV
    try:
        nf = pd.read_csv(netflow_path)
    except:
        print("Error reading NetFlow CSV")
        return

    # read CICFlowMeter CSV
    try:
        cic = pd.read_csv(cic_path)
    except:
        print("Using only NetFlow data")
        cic = None

    producer = KafkaProducer(
        bootstrap_servers=kafka_brokers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=5
    )

    # process each row
    for _, row in nf.iterrows():
        data = {
            "src_ip": str(row.get('SrcAddr', '')),
            "dst_ip": str(row.get('DstAddr', '')),
            "src_port": int(row.get('SrcPort', 0)),
            "dst_port": int(row.get('DstPort', 0)),
            "protocol": int(row.get('Prot', 0)),
            "bytes_in": int(row.get('InBytes', 0)),
            "bytes_out": int(row.get('OutBytes', 0)),
            "pkt_in": int(row.get('InPkts', 0)),
            "pkt_out": int(row.get('OutPkts', 0)),
            "timestamp": pd.to_datetime(row.get('First', 'now')).isoformat()
        }

        # add CIC
        if cic is not None:
            # if flow ID
            pass

        producer.send(topic, value=data)
        time.sleep(0.001)  # no crash

    producer.flush()
    producer.close()
    print(f"Sent {len(nf)} flows to Kafka")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--netflow", required=True)
    parser.add_argument("--cic")
    parser.add_argument("--kafka", default="localhost:9092")
    parser.add_argument("--topic", default="netflow-data")
    args = parser.parse_args()

    merge_and_send(args.netflow, args.cic, args.kafka, args.topic)