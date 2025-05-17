from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from dags.utils.postgres_utils import PostgresManager
from dags.utils.other_utils import setup_logging
from dags.c_trusted.quality_utils import *
import os

# Configure logging
log = setup_logging(__name__)

def quality_TMDb(postgres_manager: PostgresManager):
    """
    Improves and analyzes quality of the TMDb crewData dataset.
    """
    # Input and output tables
    input_table = "fmtted_TMDb_crewData"
    output_table = "trusted_TMDb_crewData"

    # Initialize Spark session
    spark = SparkSession.builder \
        .config("spark.jars", os.getenv('JDBC_URL')) \
        .appName(f"Trusted_{input_table}") \
        .getOrCreate()

    # 1. Read the table from PostgreSQL
    df = postgres_manager.read_table(spark, input_table)
    df.show(5)

    # 2. Generate Data Profiles (descriptive statistics)
    log.info(f"Generating Data Profiles for {input_table}...")
    results = descriptive_profile(df)
    profile_print = print_dataset_profile(results)
    print(f"\nDataset profile for {input_table}:\n{'=' * 40}\n{profile_print}")

    # 3. Computation of Data Quality Metrics
    log.info(f"Computing Data Quality Metrics for {input_table}...")
    Q_cm_Att = compute_column_completeness(df)
    output_lines = ["\nColumn completeness report:"]
    output_lines.append(f" {'-' * 36} \n{'Column':<25} | {'Missing (%)':>10} \n{'-' * 36}")
    for row in Q_cm_Att.collect():
        missing_pct = f"{row['missing_ratio'] * 100:.2f}%"
        output_lines.append(f"{row['column']:<25} | {missing_pct:>10} \n{'-' * 36}")
    Q_cm_rel = compute_relation_completeness(df)
    print(f"\nRelation's Completeness (ratio of complete rows): {Q_cm_rel:.4f}")
    print("\n".join(output_lines))
    Q_r = df.count() / df.dropDuplicates().count()
    print(f"\nRelation's Redundancy (ratio of duplicates): {Q_r:.4f}")

    # 4. Apply Constraints
    log.info(f"Applying constraints to {input_table}...")
    df = apply_constraints_TMDb(df)

    # Remove duplicates
    df = df.dropDuplicates()

    # Write the cleaned data to PostgreSQL
    log.info(f"Writing trusted {input_table} dataset to PostgreSQL...")
    postgres_manager.write_dataframe(df, output_table)

    # Stop Spark session
    spark.stop()
    log.info("Spark session closed.")


def apply_constraints_TMDb(df: DataFrame) -> DataFrame:
    """
    Apply constraints specific to the TMDb dataset.
    """
    # imdb_id and tmdb_id must not be null
    df = df.filter(F.col("imdb_id").isNotNull() & F.col("tmdb_id").isNotNull())

    # imdb_id must start with 'nm'
    df = df.filter(F.col("imdb_id").startswith("nm"))

    # popularity must be >= 0
    df = df.filter(F.col("popularity") >= 0)
    df = df.filter(F.col("known_for_popularity") >= 0)

    # gender must be one of the valid values
    valid_genders = ["Female", "Male", "Non-binary"]
    df = df.filter(F.col("gender").isin(valid_genders))

    # adult must be 'true' or 'false'
    df = df.filter(F.col("adult").isin(["true", "false"]))

    # Filtrar filas donde known_for no empiece con 'tt'
    df = df.filter(F.col("known_for").startswith("tt"))

    # Eliminar la columna original_name si existe
    if "original_name" in df.columns:
        df = df.drop("original_name")
    return df