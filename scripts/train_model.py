from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator

spark = SparkSession.builder \
    .appName("TrainAnomalyModel") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .getOrCreate()

# Lire depuis Cassandra
df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="logs", keyspace="security") \
    .load()

# Features
feature_cols = ['src_port', 'dst_port', 'protocol', 'bytes_in', 'bytes_out', 'pkt_in', 'pkt_out']
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=50)

pipeline = Pipeline(stages=[assembler, rf])
model = pipeline.fit(df)

# Sauvegarder
model.write().overwrite().save("/model/anomaly_model.spark")
print("Modèle entraîné et sauvegardé")
spark.stop()