from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("IMF ETL - Join and Load to Postgres") \
    .getOrCreate()

values_df = spark.read.parquet("../data/processed/imf_values_clean.parquet")
indicators_df = spark.read.parquet("../data/processed/imf_indicators_clean.parquet")

# Join tables
joined_df = values_df.join(indicators_df, values_df["indicator"] == indicators_df["id"], "inner")

# drop duplicate columns
joined_df = joined_df.drop(indicators_df["id"])  

# Define PostgreSQL properties
pg_url = "jdbc:postgresql://localhost:5432/your_db"
pg_properties = {
    "user": "your_user",
    "password": "your_password",
    "driver": "org.postgresql.Driver"
}

# Write to PostgreSQL
joined_df.write.jdbc(url=pg_url, table="imf_combined", mode="overwrite", properties=pg_properties)

spark.stop()
