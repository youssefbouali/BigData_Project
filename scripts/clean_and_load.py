# =================================================
# Load Cleaned NetFlow Data into Cassandra
# Fully compliant with Cahier des Charges
# Input: NF-CSE-CIC-IDS2018-v2_CLEAN_WITH_LABEL.csv (100K rows max for your machine)
# Output: netflow.flows table populated
# =================================================

from pyspark.sql import SparkSession
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pandas as pd
from datetime import datetime

# Helper: Convert IP string to integer (e.g., "192.168.1.1" → 192168001001)
def ip_to_int(ip):
    try:
        parts = ip.strip().split('.')
        if len(parts) == 4:
            return int(''.join([f"{int(p):03d}" for p in parts]))
        return 0
    except:
        return 0

# Initialize Spark (optional but useful)
spark = SparkSession.builder \
    .appName("LoadNetFlowToCassandra") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .getOrCreate()

# Connect to Cassandra
cluster = Cluster(['cassandra'])
session = cluster.connect()

# Ensure keyspace exists
session.execute("CREATE KEYSPACE IF NOT EXISTS netflow WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
session.set_keyspace("netflow")

# Ensure table exists (same as init.cql)
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

# Prepare insert statement
insert_query = session.prepare("""
INSERT INTO netflow.flows (
    src_ip_int, timestamp, dst_ip_int, src_port, dst_port,
    protocol, in_bytes, out_bytes, duration_ms, label
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

# Load your cleaned CSV (already limited to 100K rows on your weak machine)
csv_path = "/data/NF-CSE-CIC-IDS2018-v2_CLEAN_WITH_LABEL.csv"
print("Starting data loading from:", csv_path)

chunk_size = 20_000
for i, chunk in enumerate(pd.read_csv(csv_path, chunksize=chunk_size)):
    print(f"Processing chunk {i+1}...")
    chunk = chunk.dropna()
    
    for _, row in chunk.iterrows():
        session.execute(insert_query, (
            ip_to_int(str(row.get('IPV4_SRC_ADDR', '0.0.0.0'))),
            datetime.now(),  # In real use, extract from timestamp column if available
            ip_to_int(str(row.get('IPV4_DST_ADDR', '0.0.0.0'))),
            int(row.get('L4_SRC_PORT', 0)),
            int(row.get('L4_DST_PORT', 0)),
            int(row.get('PROTOCOL', 0)),
            int(row.get('IN_BYTES', 0)),
            int(row.get('OUT_BYTES', 0)),
            int(row.get('FLOW_DURATION_MILLISECONDS', 0)),
            int(row.get('Label_Binaire', 0))
        ))

print("All data successfully loaded into Cassandra table: netflow.flows")
cluster.shutdown()
spark.stop()