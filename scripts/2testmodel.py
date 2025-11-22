from pyspark.sql import SparkSession
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler

# 1. Spark Session
spark = SparkSession.builder \
    .appName("NetFlow-Anomaly-Prediction") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# 2. تحميل الموديل (لأنك دربته بدون Pipeline)
model_path = "/model/anomaly_detection_model_rf.spark"
model = RandomForestClassificationModel.load(model_path)
print("Modèle Random Forest chargé avec succès !")

# 3. الأعمدة بالضبط كما في التدريب (مهم جدًا!)
feature_columns = ['L4_SRC_PORT','L4_DST_PORT','PROTOCOL','L7_PROTO','IN_BYTES','OUT_BYTES',
                   'IN_PKTS','OUT_PKTS','TCP_FLAGS','CLIENT_TCP_FLAGS','SERVER_TCP_FLAGS',
                   'DURATION_IN','DURATION_OUT','SRC_TO_DST_AVG_THROUGHPUT','DST_TO_SRC_AVG_THROUGHPUT',
                   # أضف باقي الأعمدة الموجودة في الداتاسيت بعد حذف الـ IPs و Label و Attack
                   ]

# 4. مثال واحد (استخدم نفس أسماء الأعمدة)
data = [(12345, 80, 6, 0.0, 5000, 3000, 50, 48, 24, 24, 0, 1250, 0, 4000000.0, 2400000.0, ...)]  # كمل القيم

df = spark.createDataFrame(data, schema=feature_columns)

# 5. VectorAssembler يدويًا (لأن الموديل لا يحتوي عليه)
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features", handleInvalid="skip")
df_vec = assembler.transform(df)

# 6. التنبؤ
predictions = model.transform(df_vec)
predictions.select("prediction", "probability").show(truncate=False)

spark.stop()