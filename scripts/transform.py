from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("IMF ETL - Extract and Clean") \
    .config("spark.driver.extraClassPath", "<path-to-sqlite-jdbc.jar>") \
    .getOrCreate()

# SQLite DB path
db_path = "../IMF_DB.db"
jdbc_url = f"jdbc:sqlite:{db_path}"

# Read tables using JDBC
imf_values_df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "imf_values") \
    .load()

imf_indicators_df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "imf_indicators") \
    .load()

# Clean: remove rows with nulls
imf_values_clean = imf_values_df.dropna(subset=["value"])
imf_indicators_clean = imf_indicators_df.dropna()

# Save as cleaned parquet files 
imf_values_clean.write.mode("overwrite").parquet("../data/processed/imf_values_clean.parquet")
imf_indicators_clean.write.mode("overwrite").parquet("../data/processed/imf_indicators_clean.parquet")

spark.stop()
