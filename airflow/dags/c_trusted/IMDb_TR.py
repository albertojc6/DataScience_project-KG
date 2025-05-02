from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from dags.utils.postgres_utils import PostgresManager
from dags.utils.other_utils import setup_logging
import os

# Configure logging
log = setup_logging(__name__)

def quality_titleBasics(postgres_manager: PostgresManager):
    """
    Applies quality checks and constraints to the title.basics dataset.
    """
    # Initialize Spark session
    spark = SparkSession.builder \
        .config("spark.jars", os.getenv('JDBC_URL')) \
        .appName("TrustedTitleBasics") \
        .getOrCreate()

    # Input and output tables
    input_table = "fmtted_IMDb_titleBasics"
    output_table = "trusted_IMDb_titleBasics"

    # Read the formatted table from PostgreSQL
    df = postgres_manager.read_table(spark, input_table)

    # Apply constraints
    log.info("Applying constraints to title.basics dataset...")

    # 1. runtimeMinutes must be >= 0
    df = df.withColumn("runtimeMinutes", F.when(F.col("runtimeMinutes") >= 0, F.col("runtimeMinutes")).otherwise(None))

    # 2. isAdult must be 0 or 1
    df = df.withColumn("isAdult", F.when(F.col("isAdult").isin(0, 1), F.col("isAdult")).otherwise(None))

    # 3. startYear and endYear must be reasonable
    current_year = int(F.current_date().substr(1, 4).cast("int"))
    df = df.withColumn("startYear", F.when((F.col("startYear") >= 1800) & (F.col("startYear") <= current_year), F.col("startYear")).otherwise(None))
    df = df.withColumn("endYear", F.when((F.col("endYear").cast("int") >= 1800) & (F.col("endYear").cast("int") <= current_year), F.col("endYear")).otherwise(None))

    # 4. genres must be valid
    valid_genres = ["Action", "Drama", "Comedy", "Horror", "Romance", "Thriller", "Documentary"]
    df = df.withColumn("genres", F.when(F.array_contains(F.col("genres"), valid_genres), F.col("genres")).otherwise(None))

    # Remove duplicates
    df = df.dropDuplicates(["tconst"])

    # Write the cleaned data to PostgreSQL
    log.info("Writing trusted title.basics dataset to PostgreSQL...")
    postgres_manager.write_dataframe(df, output_table)

    # Stop Spark session
    spark.stop()
    log.info("Spark session closed.")