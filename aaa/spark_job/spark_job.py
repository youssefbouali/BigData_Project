
# Simple PySpark structured job that reads JSON messages from Kafka (netflow-data)
# and writes them to Elasticsearch via the Python elasticsearch client, and to Cassandra via cassandra-driver.
import os, json, time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, StructType, StructField

KAFKA = os.environ.get('KAFKA_BOOTSTRAP', 'kafka:9092')
ES = os.environ.get('ES_HOST', 'elasticsearch:9200')
CASS = os.environ.get('CASSANDRA_HOST', 'cassandra')

spark = SparkSession.builder.appName('NetFlowSpark') \
    .config('spark.sql.shuffle.partitions', '1') \
    .getOrCreate()

# In this scaffold we use a micro-batch approach by reading from Kafka topic using subscribe and options
df = spark.read.format('kafka') \
    .option('kafka.bootstrap.servers', KAFKA) \
    .option('subscribe', 'netflow-data') \
    .option('startingOffsets', 'earliest') \
    .load()

df2 = df.selectExpr('CAST(value AS STRING) as json_str')

# Basic handling: parse JSON using map transformation in Python (collect small batches)
# NOTE: For production use, use proper schema parsing and writeStreams.
msgs = df2.select('json_str').limit(1000).collect()
import json, requests
from cassandra.cluster import Cluster

es_url = f'http://{ES}:9200/netflow/_doc/'
try:
    cluster = Cluster([CASS])
    session = cluster.connect()
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS netflow WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    session.set_keyspace('netflow')
    session.execute("""
        CREATE TABLE IF NOT EXISTS flows (
            id text PRIMARY KEY,
            src_ip text,
            dest_ip text,
            src_port int,
            dest_port int,
            protocol int,
            bytes_in bigint,
            bytes_out bigint,
            num_pkts_in bigint,
            num_pkts_out bigint
        );
    """)
except Exception as e:
    print('Cassandra connection error', e)

for row in msgs:
    try:
        rec = json.loads(row['json_str'])
    except Exception:
        continue
    # send to elasticsearch
    try:
        requests.post(es_url, json=rec, timeout=5)
    except Exception as e:
        print('es error', e)
    # insert to cassandra (best-effort)
    try:
        import uuid
        session.execute("""INSERT INTO flows (id, src_ip, dest_ip, src_port, dest_port, protocol, bytes_in, bytes_out, num_pkts_in, num_pkts_out) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", (str(uuid.uuid4()), rec.get('sa') or rec.get('src_ip'), rec.get('da') or rec.get('dest_ip'), int(float(rec.get('sp') or 0)), int(float(rec.get('dp') or 0)), int(float(rec.get('pr') or 0)), int(float(rec.get('ibyt') or rec.get('bytes_in') or 0)), int(float(rec.get('obyt') or rec.get('bytes_out') or 0)), int(float(rec.get('ipkt') or rec.get('num_pkts_in') or 0)), int(float(rec.get('opkt') or rec.get('num_pkts_out') or 0))))
    except Exception as e:
        print('cass insert error', e)

print('[spark_job] processed batch, exiting (in scaffold mode)')
