from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType, LongType

# إنشاء SparkSession
spark = SparkSession.builder \
    .appName("TrafficStreaming") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# تعريف schema للبيانات
schema = StructType() \
    .add("location", StringType()) \
    .add("cars_count", IntegerType()) \
    .add("average_speed", DoubleType()) \
    .add("timestamp", LongType())

# قراءة البيانات من Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "traffic_sensors") \
    .option("startingOffsets", "latest") \
    .load()

# تحويل البيانات من JSON
traffic_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# ✅ هذا هو الجزء الجديد — حفظ النتائج في PostgreSQL
traffic_df.writeStream \
    .foreachBatch(lambda batch_df, batch_id: 
        batch_df.write
            .format("jdbc")
            .option("url", "jdbc:postgresql://localhost:5432/traffic_db")  # اسم قاعدة البيانات
            .option("dbtable", "traffic_data")  # اسم الجدول
            .option("user", "postgres")  # اسم المستخدم في PostgreSQL
            .option("password", "bayan1241")  # ← اكتبي كلمة المرور الخاصة بقاعدة البيانات
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save()
    ) \
    .outputMode("append") \
    .start() \
    .awaitTermination()
