# scripts/train_model.py
# ======================================================
# VERSION FINALE OPTIMISÉE : Comme le Notebook Kaggle
# MAIS lit depuis Cassandra + adapté aux machines modestes
# Tous les commentaires en français – 100% fonctionnel
# ======================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as _sum
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
import matplotlib.pyplot as plt
import pandas as pd
import os

# -------------------------------
# 1. Session Spark optimisée pour machine modeste + Cassandra
# -------------------------------
spark = SparkSession.builder \
    .appName("Train-Model-From-Cassandra-Like-Kaggle") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

print("Session Spark démarrée – Mode machine modeste activé")

# -------------------------------
# 2. Chargement depuis Cassandra
# -------------------------------
print("Chargement des données depuis Cassandra → netflow.flows ...")
df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="flows", keyspace="netflow") \
    .load()

total = df.count()
print(f"Total lignes dans Cassandra : {total:,}")

# ÉCHANTILLONNAGE FORCÉ (comme dans Kaggle) – MAX 80 000 lignes
MAX_ROWS = 80_000
if total > MAX_ROWS:
    df = df.limit(MAX_ROWS)
    print(f"Dataset réduit à {MAX_ROWS:,} lignes (comme dans Kaggle)")
df.cache()

# -------------------------------
# 3. Nettoyage (exactement comme dans Kaggle)
# -------------------------------
print("Nettoyage des données en cours...")
df_clean = df.drop("flow_id", "src_ip", "dst_ip", "src_ip_int", "dst_ip_int", "flow_start_time")

# Suppression des lignes avec valeurs manquantes
print("Suppression des valeurs manquantes...")
df_clean = df_clean.na.drop()
print(f"Lignes après nettoyage : {df_clean.count():,}")

# -------------------------------
# 4. Création du Label_Binaire (0 = Benign, 1 = Attaque)
# -------------------------------
df_labeled = df_clean.withColumn(
    "Label_Binaire",
    when(col("attack_type") == "Benign", 0).otherwise(1)
).drop("attack_type")

print("Distribution des classes :")
df_labeled.groupBy("Label_Binaire").count().show()

# -------------------------------
# 5. SAUVEGARDE DU CSV NETTOYÉ (comme dans Kaggle – très utile !)
# -------------------------------
#output_path = "/data/NF-CSE-CIC-IDS2018-v2_CLEAN_WITH_LABEL.csv"
#df_labeled.coalesce(1).write.csv("/data/cleaned_output", header=True, mode="overwrite")
#print(f"CSV nettoyé sauvegardé → {output_path} (prêt à télécharger)")

# -------------------------------
# 6. Sélection automatique des features numériques
# -------------------------------
exclude_cols = ["Label_Binaire", "label"]
feature_columns = [c for c in df_labeled.columns if c not in exclude_cols]

print(f"{len(feature_columns)} features sélectionnées pour le modèle")

# -------------------------------
# 7. VectorAssembler + Modèle
# -------------------------------
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features", handleInvalid="skip")
rf = RandomForestClassifier(labelCol="Label_Binaire", featuresCol="features",
                            numTrees=80, maxDepth=12, subsamplingRate=0.8, seed=42)

pipeline = Pipeline(stages=[assembler, rf])

train_df, test_df = df_labeled.randomSplit([0.8, 0.2], seed=42)
print(f"Train : {train_df.count():,} | Test : {test_df.count():,}")

print("Entraînement du modèle en cours (comme dans Kaggle)...")
model = pipeline.fit(train_df)
print("Modèle entraîné avec succès !")

# -------------------------------
# 8. Prédictions & Évaluation
# -------------------------------
predictions = model.transform(test_df)

accuracy = MulticlassClassificationEvaluator(labelCol="Label_Binaire", metricName="accuracy").evaluate(predictions)
auc = BinaryClassificationEvaluator(labelCol="Label_Binaire", metricName="areaUnderROC").evaluate(predictions)

print(f"\nPrécision (Accuracy) : {accuracy:.4f}")
print(f"AUC-ROC : {auc:.4f}")

# -------------------------------
# 9. Importance des features (Top 10)
# -------------------------------
rf_model = model.stages[-1]
importances = rf_model.featureImportances.toArray()
feat_imp = sorted(zip(feature_columns, importances), key=lambda x: x[1], reverse=True)[:10]

print("\nTop 10 features les plus importantes :")
for feat, imp in feat_imp:
    print(f" • {feat} : {imp:.4f}")

# -------------------------------
# 10. Sauvegarde du modèle
# -------------------------------
model_path = "/models/2anomaly_detection_model_rf.spark"
os.makedirs("/models", exist_ok=True)
model.save(model_path)
print(f"Modèle sauvegardé → {model_path}")

# -------------------------------
# 11. Export prédictions pour Power BI
# -------------------------------
pred_pd = predictions.select("Label_Binaire", "prediction").toPandas()
pred_pd["Label"] = pred_pd["Label_Binaire"].apply(lambda x: "Benign" if x == 0 else "Attaque")
pred_pd["Prédiction"] = pred_pd["prediction"].apply(lambda x: "Benign" if x == 0 else "Attaque")
pred_pd.to_csv("/data/predictions_finales.csv", index=False)
print("Prédictions exportées → /data/predictions_finales.csv")

# -------------------------------
# 12. Graphique Top 10 types d'attaques
# -------------------------------
attack_counts = df.groupBy("attack_type").count().orderBy(col("count").desc()).limit(10).toPandas()
plt.figure(figsize=(10, 6))
plt.barh(attack_counts["attack_type"], attack_counts["count"], color="crimson")
plt.xlabel("Nombre de flux")
plt.title("Top 10 Types d'Attaques")
plt.gca().invert_yaxis()
plt.tight_layout()
plt.savefig("/data/top_10_attacks.png", dpi=150)
print("Graphique sauvegardé → /data/top_10_attacks.png")

# -------------------------------
# Fin
# -------------------------------
spark.stop()
print("\nPipeline terminé avec succès !")
print("Tu as maintenant :")
print(" → CSV nettoyé")
print(" → Modèle entraîné")
print(" → Prédictions + Graphiques")
print(" → Tout est comme dans Kaggle, mais depuis Cassandra !")