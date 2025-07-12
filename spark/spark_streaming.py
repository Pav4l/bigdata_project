from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, window, sum as spark_sum, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType

# === Схема ===
item_schema = StructType([
    StructField("item_id", StringType()),
    StructField("category", StringType()),
    StructField("item_name", StringType()),
    StructField("price", DoubleType()),
    StructField("quantity", IntegerType())
])

check_schema = StructType([
    StructField("store_id", IntegerType()),
    StructField("store_name", StringType()),
    StructField("cashier_id", IntegerType()),
    StructField("cashier_name", StringType()),
    StructField("check_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("items", ArrayType(item_schema))
])

# === Spark session ===
spark = SparkSession.builder \
    .appName("StreamingAnalyticsByWindow") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# === Kafka чтение ===
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "receipts") \
    .load()

# === Распаковка JSON ===
json_df = raw_df.selectExpr("CAST(value AS STRING) AS json_str")
data_df = json_df.select(from_json(col("json_str"), check_schema).alias("data"))

flat_df = data_df.selectExpr(
    "data.store_name",
    "data.check_id",
    "data.timestamp",
    "explode(data.items) as item"
)

items_df = flat_df.select(
    col("store_name"),
    col("check_id"),
    col("item.item_name").alias("item_name"),
    col("item.category").alias("category"),
    col("item.price").alias("price"),
    col("item.quantity").alias("quantity"),
    col("timestamp").cast("timestamp").alias("ts")
).withWatermark("ts", "10 seconds") \
 .withColumn("item_total", col("price") * col("quantity"))

# === Обработка каждой пачки ===
def process_batch(batch_df, batch_id):
    print(f"\n\n===== ⏱️ Batch {batch_id} START =====")

    # 1. 💰 Топ-5 магазинов по выручке
    revenue_df = batch_df.groupBy(
        window("ts", "10 seconds"),
        "store_name"
    ).agg(
        spark_sum("item_total").alias("store_revenue")
    ).orderBy(col("window"), col("store_revenue").desc())

    print("\n💰 Топ-5 магазинов по выручке:")
    revenue_df.limit(5).show(truncate=False)

    # 2. 🔥 Топ-10 аномальных чеков (>500)
    check_df = batch_df.groupBy(
        "check_id", "store_name", "ts"
    ).agg(
        spark_sum("item_total").alias("check_total")
    ).filter("check_total > 500") \
     .orderBy(col("ts"), col("check_total").desc())

    print("\n🔥 Топ-10 аномальных чеков (> 500 руб):")
    check_df.limit(10).show(truncate=False)

    # 3. 🏆 Топ-10 популярных товаров
    popular_df = batch_df.groupBy(
        window("ts", "10 seconds"),
        "category",
        "item_name"
    ).agg(
        count("*").alias("count")
    ).orderBy(col("window"), col("count").desc())

    print("\n🏆 Топ-10 популярных товаров:")
    popular_df.limit(10).show(truncate=False)

    print(f"===== ⏱️ Batch {batch_id} END =====\n\n")

# === Запуск стрима ===
query = items_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .start()

query.awaitTermination()