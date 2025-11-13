from pyspark.sql import SparkSession
from cassandra.cluster import Cluster
import pandas as pd
import uuid
from datetime import datetime

# Spark + Cassandra
spark = SparkSession.builder \
    .appName("CleanAndLoad") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .getOrCreate()

# Charger les 8 Go (chunk par chunk)
chunk_size = 100_000
df_iterator = pd.read_csv("/data/raw_logs.csv", chunksize=chunk_size)

cluster = Cluster(['cassandra'])
session = cluster.connect('security')

insert_stmt = session.prepare("""
INSERT INTO security.logs (id, src_ip, dst_ip, src_port, dst_port, protocol, bytes_in, bytes_out, pkt_in, pkt_out, timestamp, label)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

for i, chunk in enumerate(df_iterator):
    print(f"Processing chunk {i}...")
    # Nettoyage basique
    chunk = chunk.dropna()
    chunk['src_ip'] = chunk['src_ip'].astype(str)
    chunk['timestamp'] = pd.to_datetime(chunk['timestamp'], errors='coerce')

    for _, row in chunk.iterrows():
        session.execute(insert_stmt, (
            uuid.uuid4(),
            row['src_ip'], row['dst_ip'], int(row['src_port']), int(row['dst_port']),
            int(row['protocol']), int(row['bytes_in']), int(row['bytes_out']),
            int(row['pkt_in']), int(row['pkt_out']),
            row['timestamp'], int(row.get('label', 0))
        ))

print("Données chargées dans Cassandra")
cluster.shutdown()
spark.stop()