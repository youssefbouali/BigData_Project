# scripts/clean_and_load.py
# Version finale 100% fonctionnelle avec le dataset NF-CSE-CIC-IDS2018-v2
# Tous les commentaires sont en français

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, current_timestamp, monotonically_increasing_id
from pyspark.sql.types import IntegerType, LongType, DoubleType
import pyspark.sql.functions as F

# ------------------------------------------------------------------
# Fonction UDF : convertit une adresse IP (string) → entier 32 bits
# ------------------------------------------------------------------
def ip_to_int(ip):
    try:
        if not ip or ip in ('', 'nan', None):
            return 0
        octets = str(ip).strip().split('.')
        if len(octets) != 4:
            return 0
        return (int(octets[0]) << 24) + (int(octets[1]) << 16) + (int(octets[2]) << 8) + int(octets[3])
    except:
        return 0

ip_to_int_udf = F.udf(ip_to_int, IntegerType())

# ------------------------------------------------------------------
# Création de la session Spark avec connecteur Cassandra
# ------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("Chargement NF-CSE-CIC-IDS2018-v2 → Cassandra") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.default.parallelism", "200") \
    .getOrCreate()

print("=== Lecture du fichier CSV ===")
CSV_PATH = "/data/NF-CSE-CIC-IDS2018-v2_CLEAN_WITH_LABEL.csv"

df = spark.read.option("header", "true") \
               .option("inferSchema", "false") \
               .option("delimiter", ",") \
               .option("quote", "\"") \
               .option("escape", "\"") \
               .csv(CSV_PATH)

print(f"Nombre de lignes brutes : {df.count():,}")
print("Colonnes détectées :", df.columns)

# ------------------------------------------------------------------
# Nettoyage et transformation des types
# ------------------------------------------------------------------
df_clean = df \
    .na.drop(subset=["IPV4_SRC_ADDR", "IPV4_DST_ADDR", "L4_SRC_PORT", "L4_DST_PORT", "PROTOCOL"]) \
    .withColumn("src_ip", col("IPV4_SRC_ADDR")) \
    .withColumn("dst_ip", col("IPV4_DST_ADDR")) \
    .withColumn("src_ip_int", ip_to_int_udf(col("IPV4_SRC_ADDR"))) \
    .withColumn("dst_ip_int", ip_to_int_udf(col("IPV4_DST_ADDR"))) \
    .withColumn("src_port", col("L4_SRC_PORT").cast("int")) \
    .withColumn("dst_port", col("L4_DST_PORT").cast("int")) \
    .withColumn("protocol", col("PROTOCOL").cast("int")) \
    .withColumn("l7_proto", col("L7_PROTO").cast("double")) \
    .withColumn("in_bytes", col("IN_BYTES").cast("long")) \
    .withColumn("out_bytes", col("OUT_BYTES").cast("long")) \
    .withColumn("in_pkts", col("IN_PKTS").cast("long")) \
    .withColumn("out_pkts", col("OUT_PKTS").cast("long")) \
    .withColumn("duration_ms", col("FLOW_DURATION_MILLISECONDS").cast("long")) \
    .withColumn("tcp_flags", col("TCP_FLAGS").cast("int")) \
    .withColumn("client_tcp_flags", col("CLIENT_TCP_FLAGS").cast("int")) \
    .withColumn("server_tcp_flags", col("SERVER_TCP_FLAGS").cast("int")) \
    .withColumn("src_to_dst_avg_throughput", col("SRC_TO_DST_AVG_THROUGHPUT").cast("double")) \
    .withColumn("dst_to_src_avg_throughput", col("DST_TO_SRC_AVG_THROUGHPUT").cast("double")) \
    .withColumn("num_pkts_up_to_128_bytes", col("NUM_PKTS_UP_TO_128_BYTES").cast("long")) \
    .withColumn("num_pkts_128_to_256_bytes", col("NUM_PKTS_128_TO_256_BYTES").cast("long")) \
    .withColumn("num_pkts_256_to_512_bytes", col("NUM_PKTS_256_TO_512_BYTES").cast("long")) \
    .withColumn("num_pkts_512_to_1024_bytes", col("NUM_PKTS_512_TO_1024_BYTES").cast("long")) \
    .withColumn("num_pkts_1024_to_1514_bytes", col("NUM_PKTS_1024_TO_1514_BYTES").cast("long")) \
    .withColumn("label", when(col("Label") == "Benign", 0).otherwise(1).cast("int")) \
    .withColumn("attack_type", col("Attack")) \
    .withColumn("flow_start_time", current_timestamp())

# ------------------------------------------------------------------
# Ajout d'un identifiant unique et sélection finale des colonnes
# ------------------------------------------------------------------
df_final = df_clean \
    .withColumn("flow_id", monotonically_increasing_id()) \
    .select(
        "flow_id", "src_ip", "dst_ip", "src_ip_int", "dst_ip_int",
        "src_port", "dst_port", "protocol", "l7_proto",
        "in_bytes", "out_bytes", "in_pkts", "out_pkts",
        "duration_ms", "tcp_flags", "client_tcp_flags", "server_tcp_flags",
        "src_to_dst_avg_throughput", "dst_to_src_avg_throughput",
        "num_pkts_up_to_128_bytes", "num_pkts_128_to_256_bytes",
        "num_pkts_256_to_512_bytes", "num_pkts_512_to_1024_bytes",
        "num_pkts_1024_to_1514_bytes",
        "flow_start_time", "label", "attack_type"
    )

print(f"Nombre de lignes prêtes pour Cassandra : {df_final.count():,}")

# ------------------------------------------------------------------
# Écriture dans Cassandra
# ------------------------------------------------------------------
print("=== Écriture dans Cassandra (netflow.flows) en cours... ===")
df_final.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="flows", keyspace="netflow") \
    .mode("append") \
    .save()

print("Chargement terminé avec succès !")
spark.stop()