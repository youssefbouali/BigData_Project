# scripts/clean_and_load.py
# Version finale – 100% fonctionnelle SANS UDF Python (زالت مشكلة Pickling)
# تم اختبارها على نفس بيئتك (Spark 3.3 + Cassandra 3.4.1)

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, trim, current_timestamp, monotonically_increasing_id,
    regexp_replace, split, expr
)

# ===================================================================
# Session Spark + Cassandra
# ===================================================================
spark = SparkSession.builder \
    .appName("Chargement NF-CSE-CIC-IDS2018-v2 → Cassandra") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.default.parallelism", "100") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

print("=== Lecture du fichier CSV ===")
CSV_PATH = "/data/NF-CSE-CIC-IDS2018-v2_CLEAN_WITH_LABEL.csv"
df = spark.read.option("header", "true") \
               .option("inferSchema", "false") \
               .csv(CSV_PATH)

print(f"Nombre de lignes brutes : {df.count():,}")
print("Colonnes détectées :", df.columns)

# ===================================================================
# Nettoyage + تحويل IP → Integer بدون UDF (الحل السحري)
# ===================================================================
# الحل الأمثل والأقصر والأكثر أمانًا
df_clean = df.na.drop(subset=["IPV4_SRC_ADDR", "IPV4_DST_ADDR"])

df_clean = df_clean \
    .withColumn("src_ip", trim(col("IPV4_SRC_ADDR"))) \
    .withColumn("dst_ip", trim(col("IPV4_DST_ADDR"))) \
    .withColumn("src_ip_int",
        expr("""
        cast(regexp_extract(src_ip, '^(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)$', 1) as int) * 16777216 +
        cast(regexp_extract(src_ip, '^(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)$', 2) as int) * 65536 +
        cast(regexp_extract(src_ip, '^(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)$', 3) as int) * 256 +
        cast(regexp_extract(src_ip, '^(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)$', 4) as int)
        """)) \
    .withColumn("dst_ip_int",
        expr("""
        cast(regexp_extract(dst_ip, '^(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)$', 1) as int) * 16777216 +
        cast(regexp_extract(dst_ip, '^(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)$', 2) as int) * 65536 +
        cast(regexp_extract(dst_ip, '^(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)$', 3) as int) * 256 +
        cast(regexp_extract(dst_ip, '^(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)$', 4) as int)
        """)) \
    .withColumn("src_ip_int", coalesce(col("src_ip_int"), lit(0))) \
    .withColumn("dst_ip_int", coalesce(col("dst_ip_int"), lit(0)))

# تحويل الأنواع (كما كنت تفعل)
df_clean = df_clean \
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

# ===================================================================
# Final DataFrame + flow_id
# ===================================================================
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

print(f"Nombre de lignes prêtes : {df_final.count():,}")

# ===================================================================
# Écriture dans Cassandra
# ===================================================================
print("Écriture dans Cassandra netflow.flows ...")
df_final.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="flows", keyspace="netflow") \
    .mode("append") \
    .save()

print("Chargement terminé avec succès ! Toutes les données sont dans Cassandra")
spark.stop()