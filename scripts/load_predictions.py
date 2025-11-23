# ===================================================================
# scripts/load_predictions.py
# تحميل predictions.csv إلى Cassandra في جدول netflow.predictions
# ===================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, to_timestamp

# ===================================================================
# Session Spark + Cassandra
# ===================================================================
spark = SparkSession.builder \
    .appName("Load predictions.csv → Cassandra") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .config("spark.sql.shuffle.partitions", "50") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

print("=== Lecture du fichier predictions.csv ===")

CSV_PATH = "/data/predictions.csv"

df = spark.read.option("header", "true") \
               .option("inferSchema", "false") \
               .csv(CSV_PATH)

print(f"Nombre de lignes : {df.count():,}")
print("Colonnes détectées :", df.columns)

# ===================================================================
# Nettoyage léger + conversion types
# ===================================================================
df_clean = df \
    .withColumn("src_ip", trim(col("src_ip"))) \
    .withColumn("dst_ip", trim(col("dst_ip"))) \
    .withColumn("src_port", col("src_port").cast("int")) \
    .withColumn("dst_port", col("dst_port").cast("int")) \
    .withColumn("protocol", col("protocol").cast("int")) \
    .withColumn("is_anomaly", col("is_anomaly").cast("int")) \
    .withColumn("anomaly_score", col("anomaly_score").cast("double")) \
    .withColumn("ingestion_time", to_timestamp(col("timestamp")))  # تحويل ISO8601 → TIMESTAMP

# حذف العمود القديم
df_clean = df_clean.drop("timestamp")

print("Aperçu du dataframe :")
df_clean.show(5)

# ===================================================================
# Écriture dans Cassandra
# ===================================================================
print("Écriture dans Cassandra netflow.predictions ...")

df_clean.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="predictions", keyspace="netflow") \
    .mode("append") \
    .save()

print("=== Import terminé avec succès ! ===")

spark.stop()
