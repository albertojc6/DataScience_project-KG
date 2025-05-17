from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from dags.utils.postgres_utils import PostgresManager
from dags.utils.other_utils import setup_logging
from dags.c_trusted.quality_utils import *
import os
from pyspark.sql.functions import year, current_date, array_intersect, array, lit, size

# Configure logging
log = setup_logging(__name__)

def quality_IMDb(postgres_manager: PostgresManager):
    """
    Improves and analyzes quality of the IMDb title.basics dataset.
    """
    # Input and output tables
    input_table = "fmtted_IMDb_titleBasics"
    output_table = "trusted_IMDb_titleBasics"

    # Initialize Spark session
    spark = SparkSession.builder \
        .config("spark.jars", os.getenv('JDBC_URL')) \
        .appName(f"Trusted_{input_table}") \
        .getOrCreate()

    try:
        # 1. Read the table from PostgreSQL
        log.info(f"Reading table {input_table} from PostgreSQL...")
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
        df = apply_constraints_IMDb(df)
        print(f"DataFrame after constraints ")
        df.show(5, truncate=False)
        # Remove duplicates
        df = df.dropDuplicates(["tconst"])

        # Write the cleaned data to PostgreSQL
        log.info(f"Writing trusted {input_table} dataset to PostgreSQL...")
        postgres_manager.write_dataframe(df, output_table)

    except Exception as e:
        log.error(f"Pipeline failed for {input_table}: {str(e)}", exc_info=True)
        raise
    finally:
        spark.stop()
        log.info("Spark session closed.")

def apply_constraints_IMDb(df: F.DataFrame) -> F.DataFrame:
    """
    Apply constraints specific to the IMDb title.basics dataset.
    Filters out rows that violate constraints instead of setting values to NULL.
    """
    current_year = year(current_date())
    

    # Apply filters sequentially
    df = df.filter (
        (F.col("runtimeMinutes") >= 0) &  # runtimeMinutes must be >= 0
        (F.col("isAdult").isin(False, True)) &   # isAdult must be 0 or 1
        (F.col("startYear") >= 1800) & (F.col("startYear") <= current_year) &  # Valid startYear
        (F.col("endYear").isNull() |  # Allow NULL endYear OR valid year
            ((F.col("endYear").cast("int") >= 1800) & (F.col("endYear").cast("int") <= current_year) )
         )  # Valid endYear
    )  # Close df.filter
    # Eliminar la columna originalTitle si existe
    if "originalTitle" in df.columns:
        df = df.drop("originalTitle")
    return df