
# Create Spark session with Delta support
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Delta Load") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
# Read CSV
df = spark.read.csv("/opt/spark/transactions_raw.csv", header=True, inferSchema=True)
# Write to Delta Lake
df.write.format("delta") \
    .mode("overwrite") \
    .save("/opt/spark/delta-lake/customer_transactions")

print("Data successfully written to Delta Lake!")

spark.stop()