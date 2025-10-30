# transform_data.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_trunc, count

# --- Configuration ---
MONGO_CONNECTION_STRING = os.environ.get("MONGO_CONNECTION_STRING")
DB_NAME = "kc_311_db"
RAW_COLLECTION = "raw_requests"
MART_COLLECTION = "mart_daily_summary"

def main():
    if not MONGO_CONNECTION_STRING:
        raise ValueError("MONGO_CONNECTION_STRING environment variable not set!")

    spark = SparkSession.builder \
        .appName("KC311MongoTransformation") \
        .config("spark.mongodb.input.uri", f"{MONGO_CONNECTION_STRING}.{DB_NAME}.{RAW_COLLECTION}") \
        .config("spark.mongodb.output.uri", f"{MONGO_CONNECTION_STRING}.{DB_NAME}.{MART_COLLECTION}") \
        .getOrCreate()

    print("Reading raw data from MongoDB into Spark...")
    df = spark.read.format("mongodb").load()

    print("Transforming data...")
    mart_df = df.select(
        col("creation_date").cast("timestamp"),
        col("category"),
        col("status")
    ).filter(col("category").isNotNull()) \
    .withColumn("request_date", date_trunc("day", col("creation_date")))
    .groupBy("request_date", "category", "status") \
    .agg(count("*" ).alias("number_of_requests"))

    print(f"Writing transformed data to '{MART_COLLECTION}' collection...")
    mart_df.write.format("mongodb").mode("overwrite").save()

    print("Transformation complete.")
    spark.stop()

if __name__ == "__main__":
    main()
