# scripts/clean_and_load.py
# Final version - 100% compatible with your CSV (43 columns + Label_Binaire)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_timestamp, monotonically_increasing_id
from pyspark.sql.types import IntegerType, LongType
from pyspark.sql import functions as F

# UDF to convert IP string → integer (for partitioning & fast queries)
def ip_to_int(ip):
    try:
        if ip is None:
            return 0
        parts = str(ip).strip().split('.')
        if len(parts) == 4 and all(p.isdigit() for p in parts):
            return (int(parts[0]) << 24) + (int(parts[1]) << 16) + (int(parts[2]) << 8) + int(parts[3])
        return 0
    except:
        return 0

ip_to_int_udf = F.udf(ip_to_int, IntegerType())

# Spark Session with Cassandra connector
spark = SparkSession.builder \
    .appName("NetFlow-CIC-IDS2018-to-Cassandra") \
    .master("spark://spark-master:7077") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.default.parallelism", "100") \
    .getOrCreate()

print("=== Reading your CSV file ===")
# ضع هنا المسار الصحيح لملفك داخل الكونتينر
CSV_PATH = "/data/NF-CSE-CIC-IDS2018-v2_CLEAN_WITH_LABEL.csv"

df = spark.read.option("header", "true").option("inferSchema", "false").csv(CSV_PATH)

print(f"Raw rows   : {df.count():,}")
print("Sample of columns we found:")
df.printSchema()

# Clean + Transform
df_clean = df \
    .na.drop(subset=["L4_SRC_PORT", "L4_DST_PORT", "PROTOCOL", "IN_BYTES", "FLOW_DURATION_MILLISECONDS"]) \
    .withColumn("src_port", col("L4_SRC_PORT").cast("int")) \
    .withColumn("dst_port", col("L4_DST_PORT").cast("int")) \
    .withColumn("protocol", col("PROTOCOL").cast("int")) \
    .withColumn("in_bytes", col("IN_BYTES").cast("long")) \
    .withColumn("out_bytes", col("OUT_BYTES").cast("long")) \
    .withColumn("in_pkts", col("IN_PKTS").cast("long")) \
    .withColumn("out_pkts", col("OUT_PKTS").cast("long")) \
    .withColumn("duration_ms", col("FLOW_DURATION_MILLISECONDS").cast("long")) \
    .withColumn("tcp_flags", col("TCP_FLAGS").cast("int")) \
    .withColumn("label", col("Label_Binaire").cast("int")) \
    .withColumn("src_ip_int", ip_to_int_udf(col("IPV4_SRC_ADDR"))) \
    .withColumn("dst_ip_int", ip_to_int_udf(col("IPV4_DST_ADDR"))) \
    .withColumn("flow_start_time", F.current_timestamp())  # for partitioning by time

# Final selection (matches your Cassandra table schema)
df_final = df_clean.select(
    "src_ip_int",
    "dst_ip_int", 
    "src_port",
    "dst_port",
    "protocol",
    "in_bytes",
    "out_bytes",
    "in_pkts",
    "out_pkts",
    "duration_ms",
    "tcp_flags",
    "flow_start_time",
    "label"
).withColumn("flow_id", monotonically_increasing_id())

print(f"Cleaned rows ready for Cassandra: {df_final.count():,}")

# Write to Cassandra (table must exist - see CQL below)
print("Writing to Cassandra → netflow.flows ...")
df_final.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="flows", keyspace="netflow") \
    .mode("append") \
    .save()

print("DONE! All data successfully loaded into Cassandra")
spark.stop()