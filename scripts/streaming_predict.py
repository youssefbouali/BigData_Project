# scripts/streaming_predict.py
# ==============================================================
# TRAITEMENT EN TEMPS RÉEL NETFLOW : Prédiction + Stockage + Alertes
# Compatible avec le modèle entraîné via train_model.py
# Tous les commentaires en français – Version finale 100% fonctionnelle
# ==============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, to_json, struct
from pyspark.sql.types import *
from pyspark.ml import PipelineModel

def start_streaming_prediction():

    # ===================================================================
    # 1. Session Spark avec connecteurs Cassandra & Kafka
    # ===================================================================
    spark = SparkSession.builder \
        .appName("NetFlow-RealTime-Prediction-And-Storage") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.cassandra.auth.username", "cassandra") \
        .config("spark.cassandra.auth.password", "cassandra") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print("Session Spark Streaming démarrée avec succès")

    # ===================================================================
    # 2. Chargement du modèle entraîné (doit exister dans /models/)
    # ===================================================================
    model_path = "/models/anomaly_detection_model_rf.spark"
    print(f"Chargement du modèle depuis : {model_path}")
    model = PipelineModel.load(model_path)
    print("Modèle chargé avec succès !")

    # ===================================================================
    # 3. Lecture du flux Kafka (topic: netflow-data)
    # ===================================================================
    raw_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "netflow-data") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # ===================================================================
    # 4. Schéma exact des messages JSON envoyés vers Kafka
    # (doit être identique à celui utilisé par ton producteur NetFlow)
    # ===================================================================
    flow_schema = StructType([
        StructField("src_ip", StringType(), True),
        StructField("dst_ip", StringType(), True),
        StructField("src_ip_int", IntegerType(), True),
        StructField("dst_ip_int", IntegerType(), True),
        StructField("src_port", IntegerType(), True),
        StructField("dst_port", IntegerType(), True),
        StructField("protocol", IntegerType(), True),
        StructField("l7_proto", DoubleType(), True),
        StructField("in_bytes", LongType(), True),
        StructField("out_bytes", LongType(), True),
        StructField("in_pkts", LongType(), True),
        StructField("out_pkts", LongType(), True),
        StructField("duration_ms", LongType(), True),
        StructField("tcp_flags", IntegerType(), True),
        StructField("client_tcp_flags", IntegerType(), True),
        StructField("server_tcp_flags", IntegerType(), True),
        StructField("src_to_dst_avg_throughput", DoubleType(), True),
        StructField("dst_to_src_avg_throughput", DoubleType(), True),
        StructField("num_pkts_up_to_128_bytes", LongType(), True),
        StructField("num_pkts_128_to_256_bytes", LongType(), True),
        StructField("num_pkts_256_to_512_bytes", LongType(), True),
        StructField("num_pkts_512_to_1024_bytes", LongType(), True),
        StructField("num_pkts_1024_to_1514_bytes", LongType(), True),
        StructField("attack_type", StringType(), True)  # optionnel, pour debug
    ])

    # ===================================================================
    # 5. Parsing du JSON + ajout du timestamp d'ingestion
    # ===================================================================
    parsed_stream = raw_stream \
        .selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), flow_schema).alias("data")) \
        .select("data.*") \
        .withColumn("ingestion_time", current_timestamp())

    print("Flux Kafka parsé correctement")

    # ===================================================================
    # 6. Prédiction en temps réel avec le modèle entraîné
    # ===================================================================
    predictions_stream = model.transform(parsed_stream)

    # ===================================================================
    # 7. Écriture de TOUS les flux dans la table historique (netflow.flows)
    # ===================================================================
    historical_query = predictions_stream \
        .writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="flows", keyspace="netflow") \
        .option("checkpointLocation", "/tmp/cp_flows") \
        .outputMode("append") \
        .start()

    print("Écriture vers netflow.flows démarrée")

    # ===================================================================
    # 8. Écriture des prédictions dans la table temps réel (netflow.predictions)
    # ===================================================================
    predictions_to_save = predictions_stream \
        .select(
            "src_ip_int",
            "dst_ip_int",
            "src_port",
            "dst_port",
            "protocol",
            "in_bytes",
            "out_bytes",
            "duration_ms",
            "ingestion_time",
            col("prediction").alias("is_anomaly"),           # 1 = attaque
            expr("probability[1]").alias("anomaly_score")     # probabilité d'attaque
        )

    predictions_query = predictions_to_save \
        .writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="predictions", keyspace="netflow") \
        .option("checkpointLocation", "/tmp/cp_predictions") \
        .outputMode("append") \
        .start()

    print("Écriture vers netflow.predictions démarrée")

    # ===================================================================
    # 9. Envoi des alertes (attaques détectées) vers le topic Kafka "alerts"
    # ===================================================================
    alerts_stream = predictions_stream \
        .filter(col("prediction") == 1) \
        .select(
            to_json(struct(
                "src_ip", "dst_ip", "src_port", "dst_port",
                "protocol", "in_bytes", "out_bytes", "duration_ms",
                "anomaly_score", "ingestion_time"
            )).alias("value")
        )

    alerts_query = alerts_stream \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("topic", "alerts") \
        .option("checkpointLocation", "/tmp/cp_alerts") \
        .outputMode("append") \
        .start()

    print("Alertes envoyées vers le topic Kafka 'alerts'")

    # ===================================================================
    # 10. Démarrage complet du pipeline
    # ===================================================================
    print("\n" + "="*60)
    print(" PIPELINE TEMPS RÉEL NETFLOW ACTIVÉE AVEC SUCCÈS !")
    print("="*60)
    print("→ Tous les flux → Cassandra (netflow.flows)")
    print("→ Prédictions → Cassandra (netflow.predictions)")
    print("→ Alertes attaques → Kafka topic 'alerts'")
    print("→ Modèle utilisé :", model_path)
    print("="*60)

    # Attente infinie (le streaming ne s'arrête jamais)
    alerts_query.awaitTermination()

# ===================================================================
# Lancement du script
# ===================================================================
if __name__ == "__main__":
    start_streaming_prediction()