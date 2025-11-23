from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassificationModel

# --- Initialisation Spark ---
spark = (
    SparkSession.builder
        .appName("NetFlow Anomaly Prediction")
        .master("local[*]")
        .getOrCreate()
)

# --- Chargement du modèle déjà entraîné ---
model_path = "/model/anomaly_detection_model_rf.spark"
loaded_model = RandomForestClassificationModel.load(model_path)
print("Modèle chargé ✔")

# --- Colonnes exactes utilisées pendant l'entraînement ---
feature_columns_exact = [
    'L4_SRC_PORT', 'L4_DST_PORT', 'PROTOCOL', 'L7_PROTO', 'IN_BYTES', 'IN_PKTS', 'OUT_BYTES',
    'OUT_PKTS', 'TCP_FLAGS', 'CLIENT_TCP_FLAGS', 'SERVER_TCP_FLAGS', 'FLOW_DURATION_MILLISECONDS',
    'DURATION_IN', 'DURATION_OUT', 'MIN_TTL', 'MAX_TTL', 'LONGEST_FLOW_PKT', 'SHORTEST_FLOW_PKT',
    'MIN_IP_PKT_LEN', 'MAX_IP_PKT_LEN', 'SRC_TO_DST_SECOND_BYTES', 'DST_TO_SRC_SECOND_BYTES',
    'RETRANSMITTED_IN_BYTES', 'RETRANSMITTED_IN_PKTS', 'RETRANSMITTED_OUT_BYTES', 'RETRANSMITTED_OUT_PKTS',
    'SRC_TO_DST_AVG_THROUGHPUT', 'DST_TO_SRC_AVG_THROUGHPUT', 'NUM_PKTS_UP_TO_128_BYTES',
    'NUM_PKTS_128_TO_256_BYTES', 'NUM_PKTS_256_TO_512_BYTES', 'NUM_PKTS_512_TO_1024_BYTES',
    'NUM_PKTS_1024_TO_1514_BYTES', 'TCP_WIN_MAX_IN', 'TCP_WIN_MAX_OUT', 'ICMP_TYPE', 'ICMP_IPV4_TYPE',
    'DNS_QUERY_ID', 'DNS_QUERY_TYPE', 'DNS_TTL_ANSWER', 'FTP_COMMAND_RET_CODE'
]

# --- Exemple de paquet NetFlow anormal ---
fake_data = [
    (12345, 80, 6, 80.0, 20000, 150, 25000, 120, 31, 31, 31, 1000, 400, 600,
     32, 64, 1500, 60, 60, 1514, 20000.0, 25000.0, 500, 5, 600, 8, 2000000.0,
     2500000.0, 40, 20, 10, 5, 2, 8192, 16384, 0, 0, 0, 0, 0, 0.0)
]

# --- Création du DataFrame Spark ---
fake_df = spark.createDataFrame(fake_data, feature_columns_exact)
fake_df.show(truncate=False)

# --- Vectorisation ---
assembler = VectorAssembler(inputCols=feature_columns_exact, outputCol="features", handleInvalid="skip")
fake_assembled = assembler.transform(fake_df)

# --- Prédiction ---
prediction = loaded_model.transform(fake_assembled)
prediction.select("prediction", "probability").show(truncate=False)

pred = prediction.select("prediction").first()[0]
prob = prediction.select("probability").first()[0][1]

print(f"\nPrédiction : {'ATTACK' if pred == 1 else 'BENIGN'} (proba attack = {prob:.4f})")
if pred == 1:
    print("⚠️  ALERTE : Trafic suspect détecté !")
else:
    print("✅ Trafic bénin")
