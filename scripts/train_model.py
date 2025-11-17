# train_model.py
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
import time

spark = SparkSession.builder \
    .appName("TrainModel-LowPerf") \
    .master("local[2]") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

# Chargement
df = spark.read.csv("/data/NF-CSE-CIC-IDS2018-v2_CLEAN_WITH_LABEL.csv", header=True, inferSchema=True)

# ÉCHANTILLONNAGE FORCÉ (CRUCIAL SUR TA MACHINE)
#df = df.limit(100000)  # 100K MAX → < 5 min garanti
#print(f"Dataset réduit à {df.count():,} lignes pour entraînement rapide")

# Label binaire
from pyspark.sql.functions import when, col
df = df.withColumn("Label_Binaire", when(col("Attack") == "Benign", 0).otherwise(1))

# Features
feature_cols = [c for c in df.columns if c not in ["Attack", "Label_Binaire"]]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
data = assembler.transform(df).select("features", "Label_Binaire").cache()

# Modèle allégé pour machine faible
rf = RandomForestClassifier(
    labelCol="Label_Binaire",
    featuresCol="features",
    numTrees=50,       # réduit de 100 → 50
    maxDepth=10,       # réduit de 12 → 10
    seed=42
)

print("Démarrage entraînement (max 5 min)...")
start = time.time()
model = rf.fit(data)
end = time.time()
print(f"Modèle entraîné en {(end-start)/60:.2f} minutes")

# Sauvegarde
model.save("/models/anomaly_model_low_perf.spark")
print("Modèle sauvegardé → prêt pour streaming !")

spark.stop()