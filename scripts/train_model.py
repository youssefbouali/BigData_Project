# scripts/train_model.py
# ======================================================
# FULL TRAINING PIPELINE FROM CASSANDRA → ML MODEL
# Compliant with Cahier des Charges – Big Data Cybersecurity Project
# Reads from: netflow.flows (Cassandra)
# Saves model to: /models/anomaly_detection_model.spark
# All comments in English
# Optimized for low-performance machine (100K rows max)
# ======================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
import matplotlib.pyplot as plt
import pandas as pd
import os

# -------------------------------
# 1. Spark Session with Cassandra connector
# -------------------------------
spark = SparkSession.builder \
    .appName("TrainAnomalyDetectionModel-FromCassandra") \
    .config("2025-11-17spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

print("Spark session started with Cassandra connection")

# -------------------------------
# 2. Load data directly from Cassandra (netflow.flows)
# -------------------------------
print("Loading data from Cassandra table: netflow.flows ...")
df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="flows", keyspace="netflow") \
    .load()

print(f"Total rows loaded from Cassandra: {df.count():,}")

# -------------------------------
# 3. Limit to 100,000 rows → guaranteed < 5 min on your machine
# -------------------------------
MAX_ROWS = 100_000
df = df.limit(MAX_ROWS)
print(f"Dataset limited to {MAX_ROWS:,} rows for training (safe for low-performance machine)")

# -------------------------------
# 4. Create binary label (0 = Benign, 1 = Attack)
#    Note: In your cleaned data, label is already 0 or 1 → but we ensure it
# -------------------------------
df = df.withColumn("Label_Binaire", col("label").cast("integer"))

# Drop original label if needed (keep only binary)
df = df.drop("label")

# -------------------------------
# 5. Select numeric features (all except IPs and timestamp)
# -------------------------------
feature_columns = [
    "src_port", "dst_port", "protocol",
    "in_bytes", "out_bytes", "duration_ms"
    # Add more numeric columns if present in your table
    # Example: "pkt_in", "pkt_out", "min_pkt_size", etc.
]

# Auto-detect all numeric columns (safe method)
numeric_cols = [field.name for field in df.schema.fields 
                if field.name not in ["src_ip_int", "dst_ip_int", "timestamp"]
                and str(field.dataType) in ["IntegerType", "LongType", "DoubleType", "FloatType"]]

feature_columns = numeric_cols
print(f"Selected {len(feature_columns)} numeric features: {feature_columns}")

# -------------------------------
# 6. Assemble features into a single vector
# -------------------------------
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features", handleInvalid="skip")
df_assembled = assembler.transform(df)

# Final dataset for ML
df_final = df_assembled.select("features", "Label_Binaire").cache()
print("Feature vector assembled")

# -------------------------------
# 7. Train / Test split
# -------------------------------
train_df, test_df = df_final.randomSplit([0.8, 0.2], seed=42)
print(f"Training set: {train_df.count():,} rows")
print(f"Test set: {test_df.count():,} rows")

# -------------------------------
# 8. Train Random Forest (light config for your machine)
# -------------------------------
rf = RandomForestClassifier(
    labelCol="Label_Binaire",
    featuresCol="features",
    numTrees=50,           # Reduced from 100 → faster
    maxDepth=10,           # Reduced from 12
    subsamplingRate=0.8,
    seed=42
)

print("Starting model training...")
model = rf.fit(train_df)
print("Model training completed successfully!")

# -------------------------------
# 9. Predictions on test set
# -------------------------------
predictions = model.transform(test_df)
print("Sample predictions:")
predictions.select("Label_Binaire", "prediction", "probability").show(10, truncate=False)

# -------------------------------
# 10. Evaluation
# -------------------------------
acc_evaluator = MulticlassClassificationEvaluator(labelCol="Label_Binaire", predictionCol="prediction", metricName="accuracy")
auc_evaluator = BinaryClassificationEvaluator(labelCol="Label_Binaire", rawPredictionCol="rawPrediction", metricName="areaUnderROC")

accuracy = acc_evaluator.evaluate(predictions)
auc = auc_evaluator.evaluate(predictions)

print(f"Accuracy: {accuracy:.4f}")
print(f"AUC-ROC: {auc:.4f}")

# -------------------------------
# 11. Feature Importance
# -------------------------------
importances = model.featureImportances
importance_list = [(col_name, float(imp)) for col_name, imp in zip(feature_columns, importances)]
importance_list.sort(key=lambda x: x[1], reverse=True)

print("\nTop 10 Most Important Features:")
for col, imp in importance_list[:10]:
    print(f"  • {col}: {imp:.4f}")

# -------------------------------
# 12. Save the trained model
# -------------------------------
model_output_path = "/models/anomaly_detection_model_rf.spark"
model.save(model_output_path)
print(f"Model saved successfully → {model_output_path}")

# -------------------------------
# 13. Export predictions for Power BI
# -------------------------------
pred_pd = predictions.select("Label_Binaire", "prediction").toPandas()
pred_pd["Label"] = pred_pd["Label_Binaire"].apply(lambda x: "Benign" if x == 0 else "Attack")
pred_pd["Prediction"] = pred_pd["prediction"].apply(lambda x: "Benign" if x == 0 else "Attack")
pred_pd.to_csv("/data/predictions_from_cassandra.csv", index=False)
print("Predictions exported → /data/predictions_from_cassandra.csv (ready for Power BI)")

# -------------------------------
# 14. Visualization: Top 10 Attack Sources (by src_ip_int count)
# -------------------------------
top_sources = df.groupBy("src_ip_int").count().orderBy(col("count").desc()).limit(10)
top_sources_pd = top_sources.toPandas()

plt.figure(figsize=(10, 6))
plt.barh(range(len(top_sources_pd)), top_sources_pd["count"], color='crimson')
plt.yticks(range(len(top_sources_pd)), [f"IP_{ip}" for ip in top_sources_pd["src_ip_int"]])
plt.xlabel("Number of Flows")
plt.title("Top 10 Source IPs by Flow Count")
plt.gca().invert_yaxis()
plt.tight_layout()
plt.savefig("/data/top_attack_sources.png")
plt.show()

# -------------------------------
# 15. Stop Spark
# -------------------------------
spark.stop()
print("Training pipeline completed successfully!")
print("You can now use the model in your Spark Streaming job.")