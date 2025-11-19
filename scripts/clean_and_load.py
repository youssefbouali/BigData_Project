# scripts/clean_and_load.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, to_timestamp
from pyspark.sql.types import IntegerType
import re

# UDF pour convertir IP → int
def ip_to_int(ip):
    try:
        parts = str(ip).strip().split('.')
        if len(parts) == 4 and all(p.isdigit() for p in parts):
            return int(''.join(f"{int(p):03d}" for p in parts))
        return 0
    except:
        return 0

ip_to_int_udf = udf(ip_to_int, IntegerType())

# Spark Session avec connecteur Cassandra
spark = SparkSession.builder \
    .appName("LoadNetFlowToCassandra") \
    .master("spark://spark-master:7077") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .getOrCreate()

print("Lecture du fichier CSV...")
df = spark.read.option("header", "true").csv("/data/NF-CSE-CIC-IDS2018-v2_CLEAN_WITH_LABEL.csv")

print(f"Nombre de lignes brutes : {df.count():,}")

# Nettoyage + transformation
df_clean = df.dropna() \
    .withColumn("timestamp", to_timestamp(col("Timestamp"))) \
    .withColumn("src_ip_int", ip_to_int_udf(col("IPV4_SRC_ADDR"))) \
    .withColumn("dst_ip_int", ip_to_int_udf(col("IPV4_DST_ADDR"))) \
    .withColumn("src_port", col("L4_SRC_PORT").cast("int")) \
    .withColumn("dst_port", col("L4_DST_PORT").cast("int")) \
    .withColumn("protocol", col("PROTOCOL").cast("int")) \
    .withColumn("in_bytes", col("IN_BYTES").cast("long")) \
    .withColumn("out_bytes", col("OUT_BYTES").cast("long")) \
    .withColumn("duration_ms", col("FLOW_DURATION_MILLISECONDS").cast("long")) \
    .withColumn("label", col("Label_Binaire").cast("int"))

# Sélection finale
df_final = df_clean.select(
    "src_ip_int", "timestamp", "dst_ip_int", "src_port", "dst_port",
    "protocol", "in_bytes", "out_bytes", "duration_ms", "label"
)

print(f"Écriture dans Cassandra netflow.flows ... ({df_final.count():,} lignes)")

df_final.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="flows", keyspace="netflow") \
    .mode("append") \
    .save()

print("Données chargées avec succès dans Cassandra !")
spark.stop()