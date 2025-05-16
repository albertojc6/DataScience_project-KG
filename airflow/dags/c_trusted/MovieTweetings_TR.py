from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from dags.utils.postgres_utils import PostgresManager
from dags.utils.other_utils import setup_logging
from dags.c_trusted.quality_utils import *
import os

# Configure logging
log = setup_logging(__name__)

def quality_MovieTweetings(postgres_manager: PostgresManager):
    """
    Improves and analyzes quality of the MovieTweetings datasets (movies, users, ratings).
    """
    datasets = [
        {"input_table": "fmtted_MovTweet_movies", "output_table": "trusted_MovTweet_movies", "constraints": apply_constraints_movies},
        {"input_table": "fmtted_MovTweet_users", "output_table": "trusted_MovTweet_users", "constraints": apply_constraints_users},
        {"input_table": "fmtted_MovTweet_ratings", "output_table": "trusted_MovTweet_ratings", "constraints": apply_constraints_ratings}
    ]

    for dataset in datasets:
        input_table = dataset["input_table"]
        output_table = dataset["output_table"]
        constraints = dataset["constraints"]

        log.info(f"Processing dataset: {input_table}...")

        # Initialize Spark session
        spark = SparkSession.builder \
            .config("spark.jars", os.getenv('JDBC_URL')) \
            .appName(f"Trusted_{input_table}") \
            .getOrCreate()

        try:
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
            df = constraints(df)

            # Remove duplicates
            df = df.dropDuplicates()

            # Write the cleaned data to PostgreSQL
            log.info(f"Writing trusted {input_table} dataset to PostgreSQL...")
            postgres_manager.write_dataframe(df, output_table)

        except Exception as e:
            log.error(f"Pipeline failed for {input_table}: {str(e)}", exc_info=True)
            raise
        finally:
            spark.stop()
            log.info("Spark session closed.")

def apply_constraints_movies(df: F.DataFrame) -> F.DataFrame:
    """
    Apply constraints specific to the movies dataset.
    """
    # movie_id must not be null
    df = df.filter(F.col("movie_id").isNotNull())
    # movie_id must start with 'tt'
    df = df.filter(F.col("movie_id").startswith("tt"))
    # Eliminar la columna movie_year si existe
    if "movie_year" in df.columns:
        df = df.drop("movie_year")
    return df

def apply_constraints_users(df: F.DataFrame) -> F.DataFrame:
    """
    Apply constraints specific to the users dataset.
    """
    # user_id and twitter_id must not be null
    df = df.filter(F.col("user_id").isNotNull() & F.col("twitter_id").isNotNull())

    return df

def apply_constraints_ratings(df: F.DataFrame) -> F.DataFrame:
    """
    Apply constraints specific to the ratings dataset.
    """
    # user_id and movie_id must not be null
    df = df.filter(F.col("user_id").isNotNull() & F.col("movie_id").isNotNull())

    # rating must be between 0 and 10
    df = df.filter((F.col("rating") >= 0) & (F.col("rating") <= 10))

    # rating_timestamp must be in format 'yyyy-MM-dd HH:mm:ss'
    df = df.withColumn(
        "rating_timestamp",
        F.date_format(F.to_timestamp(F.col("rating_timestamp")), "yyyy-MM-dd HH:mm:ss")
    )

    return df