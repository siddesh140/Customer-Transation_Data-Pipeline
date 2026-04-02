from pyspark.sql import SparkSession
from cassandra.cluster import Cluster

spark = SparkSession.builder \
    .appName("Load to ScyllaDB") \
    .getOrCreate()

# Read transformed data
df = spark.read.parquet("/opt/spark/delta-lake/transformed_data")

# Convert to pandas (small data only - fine for assignment)
pdf = df.toPandas()

# Connect to ScyllaDB
cluster = Cluster(["scylladb"])
session = cluster.connect("mykeyspace")

# Insert data
for _, row in pdf.iterrows():
    session.execute("""
        INSERT INTO daily_customer_totals (customer_id, transaction_date, daily_total)
        VALUES (%s, %s, %s)
    """, (row['customer_id'], row['transaction_date'], float(row['daily_total'])))

print("Data loaded into ScyllaDB!")

spark.stop()