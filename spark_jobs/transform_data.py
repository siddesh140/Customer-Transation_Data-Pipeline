from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum as _sum

spark = SparkSession.builder \
    .appName("Transform Data") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Read from Delta Lake
df = spark.read.format("delta").load("/opt/spark/delta-lake/customer_transactions")

# 1. Remove duplicates
df = df.dropDuplicates(["transaction_id"])

# 2. Filter invalid transactions
df = df.filter(col("amount") > 0)

# 3. Extract date
df = df.withColumn("transaction_date", to_date(col("timestamp")))

# 4. Aggregation
result = df.groupBy("customer_id", "transaction_date") \
    .agg(_sum("amount").alias("daily_total"))

# Show output
result.show(5, truncate=False)

# Save transformed data (optional - for next step)
result.write.mode("overwrite").parquet("/opt/spark/delta-lake/transformed_data")

spark.stop()