# =================================================
# Load Cleaned NetFlow Data into Cassandra
# Fully compliant with Cahier des Charges
# =================================================

from pyspark.sql import SparkSession

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

import pandas as pd
from datetime import datetime

def ip_to_int(ip):
    try:
        parts = ip.strip().split('.')
        if len(parts) == 4:
            return int(''.join([f"{int(p):03d}" for p in parts]))
        return 0
    except:
        return 0

spark = SparkSession.builder \
    .appName("LoadNetFlowToCassandra") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .getOrCreate()



auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')

cluster = Cluster(
    contact_points=['cassandra'],
    port=9042,
    auth_provider=auth_provider,
    connect_timeout=30,
    control_connection_timeout=30,
    idle_heartbeat_interval=30
)

session = cluster.connect()

session.execute("CREATE KEYSPACE IF NOT EXISTS netflow WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
session.set_keyspace("netflow")

session.execute("""
CREATE TABLE IF NOT EXISTS netflow.flows (
    src_ip_int INT,
    timestamp TIMESTAMP,
    dst_ip_int INT,
    src_port INT,
    dst_port INT,
    protocol INT,
    in_bytes BIGINT,
    out_bytes BIGINT,
    duration_ms BIGINT,
    label INT,
    PRIMARY KEY (src_ip_int, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
""")

insert_query = session.prepare("""
INSERT INTO netflow.flows (
    src_ip_int, timestamp, dst_ip_int, src_port, dst_port,
    protocol, in_bytes, out_bytes, duration_ms, label
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

csv_path = "/data/NF-CSE-CIC-IDS2018-v2_CLEAN_WITH_LABEL.csv"
print("Starting data loading from:", csv_path)

chunk_size = 10_000
total_rows = 0

for i, chunk in enumerate(pd.read_csv(csv_path, chunksize=chunk_size)):
    print(f"Processing chunk {i+1}...")
    chunk = chunk.dropna()
    
    for _, row in chunk.iterrows():
        try:
            ts_str = str(row['Timestamp']).split('.')[0]
            timestamp = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
        except:
            timestamp = datetime.now()

        session.execute(insert_query, (
            ip_to_int(str(row.get('IPV4_SRC_ADDR', '0.0.0.0'))),
            timestamp,
            ip_to_int(str(row.get('IPV4_DST_ADDR', '0.0.0.0'))),
            int(row.get('L4_SRC_PORT', 0)),
            int(row.get('L4_DST_PORT', 0)),
            int(row.get('PROTOCOL', 6)),
            int(row.get('IN_BYTES', 0)),
            int(row.get('OUT_BYTES', 0)),
            int(row.get('FLOW_DURATION_MILLISECONDS', 0)),
            int(row.get('Label_Binaire', 0))
        ))
        total_rows += 1
        if total_rows % 5000 == 0:
            print(f"   Loaded {total_rows:,} rows...")

print(f"All data loaded successfully! Total: {total_rows:,} rows")
cluster.shutdown()
spark.stop()