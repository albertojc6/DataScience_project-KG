from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from dags.utils.postgres_utils import PostgresManager
from dags.utils.other_utils import setup_logging
from dags.c_trusted.quality_utils import *
import os
from pyspark.sql.functions import year, current_date

# Configure logging
log = setup_logging(__name__)

def quality_IMDb(postgres_manager: PostgresManager):
    """
    Improves and analyzes quality of the IMDb datasets: title.basics, title.crew, name.basics.
    """
    datasets = [
        {
            "input_table": "fmtted_IMDb_titleBasics",
            "output_table": "trusted_IMDb_titleBasics",
            "constraints": apply_constraints_titlebasics
        },
        {
            "input_table": "fmtted_IMDb_titleCrew",
            "output_table": "trusted_IMDb_titleCrew",
            "constraints": apply_constraints_titlecrew
        },
        {
            "input_table": "fmtted_IMDb_nameBasics",
            "output_table": "trusted_IMDb_nameBasics",
            "constraints": apply_constraints_namebasics
        }
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
            df = constraints(df)
            df.show(5, truncate=False)


            # Write the cleaned data to PostgreSQL
            log.info(f"Writing trusted {input_table} dataset to PostgreSQL...")
            postgres_manager.write_dataframe(df, output_table)
        except Exception as e:
            log.error(f"Pipeline failed for {input_table}: {str(e)}", exc_info=True)
            raise
        finally:
            spark.stop()
            log.info("Spark session closed.")





def apply_constraints_titlebasics(df: F.DataFrame) -> F.DataFrame:
    """
    Constraints for IMDb title.basics.
    """
    current_year = year(current_date())
    


    df = df.filter (
        (F.col("runtimeMinutes") >= 0) &  # runtimeMinutes must be >= 0
        (F.col("isAdult").isin(False, True)) &   # isAdult must be Boolean
        (F.col("startYear") >= 1800) & (F.col("startYear") <= current_year) &  # Valid startYear
        (F.col("endYear").isNull() |  # Allow NULL endYear OR valid year
            ((F.col("endYear").cast("int") >= 1800) & (F.col("endYear").cast("int") <= current_year) )
         )  
    )  
    
    # remove variable, redundant
    if "originalTitle" in df.columns:
        df = df.drop("originalTitle")
    return df

def apply_constraints_titlecrew(df: F.DataFrame) -> F.DataFrame:
    """
    Constraints for IMDb title.crew.
    """
 
    # tconst must not be null and must start with 'tt'
    df = df.filter(F.col("tconst").isNotNull() & F.col("tconst").startswith("tt"))

    # to verify that all directors and writers of a movie start with "nm", first we need to have them in an array (without braces)
    # remove curly braces and convert directors and writers to arrays if not null
    df = df.withColumn(
        "directors_clean",
        F.regexp_replace(F.col("directors"), "[{}]", "")
    ).withColumn(
        "writers_clean",
        F.regexp_replace(F.col("writers"), "[{}]", "")
    )

    df = df.withColumn(
        "directors_arr",
        F.when(F.col("directors_clean").isNotNull(), F.split(F.col("directors_clean"), ",")).otherwise(F.array())
    ).withColumn(
        "writers_arr",
        F.when(F.col("writers_clean").isNotNull(), F.split(F.col("writers_clean"), ",")).otherwise(F.array())
    )

    # directors: all elements must start with 'nm' (if not empty)
    df = df.filter(
        (F.size(F.col("directors_arr")) == 0) | 
        F.expr("aggregate(directors_arr, true, (acc, x) -> acc and (x is null or x like 'nm%'))")
    )

    # writers: all elements must start with 'nm' (if not empty)
    df = df.filter(
        (F.size(F.col("writers_arr")) == 0) | 
        F.expr("aggregate(writers_arr, true, (acc, x) -> acc and (x is null or x like 'nm%'))")
    )

    # directors and writers cannot both be null at the same time
    df = df.filter(~(F.col("directors").isNull() & F.col("writers").isNull()))

    # drop helper columns
    df = df.drop("directors_arr", "writers_arr","directors_clean", "writers_clean")

    return df


def apply_constraints_namebasics(df: F.DataFrame) -> F.DataFrame:
    """
    Constraints for IMDb name.basics.
    """
    
    current_year = year(current_date())

    # nconst must not be null and must start with 'nm'
    df = df.filter(F.col("nconst").isNotNull() & F.col("nconst").startswith("nm"))

    # birthYear/deathYear must be null or between 1800 and current year
    df = df.filter(
        (F.col("birthYear").isNull() | ((F.col("birthYear") >= 1800) & (F.col("birthYear") <= current_year))) &
        (F.col("deathYear").isNull() | ((F.col("deathYear") >= 1800) & (F.col("deathYear") <= current_year)))
    )

    # if both not null, birthYear <= deathYear
    df = df.filter(
        F.col("birthYear").isNull() | F.col("deathYear").isNull() | (F.col("birthYear") <= F.col("deathYear"))
    )

    # clean and convert knownForTitles to array if not already
    df = df.withColumn(
        "knownForTitles_clean",
        F.regexp_replace(F.col("knownForTitles"), "[{}]", "")
    ).withColumn(
        "knownForTitles_arr",
        F.when(F.col("knownForTitles_clean").isNotNull(), F.split(F.col("knownForTitles_clean"), ",")).otherwise(F.array())
    )

    # knownForTitles: all elements must start with 'tt' (if not empty)
    df = df.filter(
        (F.size(F.col("knownForTitles_arr")) == 0) |
        F.expr("aggregate(knownForTitles_arr, true, (acc, x) -> acc and (x is null or x like 'tt%'))")
    )
    # drop helper columns
    df = df.drop("knownForTitles_arr", "knownForTitles_clean")


    return df
