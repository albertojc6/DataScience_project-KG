from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType, MapType, DoubleType
from dags.utils.postgres_utils import PostgresManager
from dags.utils.other_utils import setup_logging
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from unidecode import  unidecode
import subprocess
import os

# Configure logging
log = setup_logging(__name__)


def value_formatting(df):
    """
    Defines correct format for all string values, so as to gain consistency.
    """
    # Collapse multiple spaces and trim all string columns -> consistency!
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for col in string_cols:
        df = df.withColumn(col, F.trim(F.regexp_replace(F.col(col), r"\s+", " ")))

    # - Homogenize NA indicators
    na_values = ['', 'NA', 'N/A', 'NaN', 'NULL']
    for col in string_cols:
        df = df.withColumn(col, F.when(F.col(col).isin(na_values), None).otherwise(F.col(col)))
    
    return df

def format_TMDb(postgres_manager: PostgresManager):
    """
    Formats TMDb data according to a common relational data model, using PostgreSQL

    Args:
        postgres_manager: object to facilitate the interaction with PostgreSQL server
    """

    # Clean up temporary files from previous stage
    subprocess.run(["rm", "-rf", f'/tmp/TMDb/'], check=True)

    spark = None
    log.info("Formatting the Movie_Crew dataset from TMDb ...")
    try:
        spark = SparkSession.builder \
        .config("spark.jars", os.getenv('JDBC_URL')) \
        .appName("TMDbFormatter") \
        .getOrCreate()

        # Create Schemas for reading properly the data
        value_schema = StructType([
            StructField("imdb_id", StringType(), False),
            StructField("tmdb_data", StructType([
                StructField("id", IntegerType(), False),
                StructField("name", StringType(), True),
                StructField("original_name", StringType(), True),
                StructField("media_type", StringType(), True),
                StructField("adult", BooleanType(), True),
                StructField("popularity", DoubleType(), True),
                StructField("gender", IntegerType(), True),
                StructField("known_for_department", StringType(), True),
                StructField("profile_path", StringType(), True),
                StructField("known_for", ArrayType(
                    StructType([
                        StructField("id", IntegerType()),
                        StructField("popularity", DoubleType())
                    ])
                ), True)
            ]), False)
        ])
        map_schema = MapType(StringType(), value_schema)

        links_schema = StructType([
            StructField("movieId", IntegerType()),
            StructField("imdbId", StringType()), 
            StructField("tmdbId", StringType())
        ])
                
        # 1. Read JSON and CSV files
        crewData_path = f"{os.getenv('HDFS_FS_URL')}/data/landing/TMDb/crew_data.json"
        json_df = spark.sparkContext.wholeTextFiles(crewData_path).toDF(["path", "content"])

        parsed_df = json_df.select(
            F.from_json(F.col("content"), map_schema).alias("parsed_data")
        ).select(F.explode("parsed_data").alias("key", "value"))

        # Extract the required fields from the exploded data
        crew_df = parsed_df.select(
            F.col("value.imdb_id").alias("imdb_id"),
            F.col("value.tmdb_data").alias("tmdb_data")
        )

        links_path = f"{os.getenv('HDFS_FS_URL')}/data/landing/TMDb/links.csv"
        links_df = spark.read.csv(links_path, header=True, schema=links_schema)

        # 2. Order results: flatten crew_data
        crew_df = crew_df.select(
            "imdb_id",
            F.col("tmdb_data.id").alias("tmdb_id"),
            F.col("tmdb_data.name"),
            F.col("tmdb_data.original_name"),
            F.col("tmdb_data.media_type"),
            F.col("tmdb_data.adult"),
            F.col("tmdb_data.popularity"),
            F.col("tmdb_data.gender"),
            F.col("tmdb_data.known_for_department"),
            F.col("tmdb_data.profile_path"),
            F.explode(F.col("tmdb_data.known_for")).alias("known_for")
        )
        # Extract the id and popularity from the known_for struct
        crew_df = crew_df.withColumn("known_for_tmdb_id", F.col("known_for.id"))
        crew_df = crew_df.withColumn("known_for_popularity", F.col("known_for.popularity"))

        # Add 'tt' prefix to imdbId (e.g., "0114709" → "tt0114709")
        links_df = links_df.withColumn("imdbId", F.concat(F.lit("tt"), F.col("imdbId")))

        # Join exploded data with links_df to get IMDb IDs
        joined_df = crew_df.join(
            links_df,
            crew_df.known_for_tmdb_id == links_df.tmdbId,
            "left"  # Use "inner" to exclude unmatched entries
        )

        # Replace TMDB ID with IMDb ID (or keep TMDB ID if no match)
        joined_df = joined_df.withColumn(
            "known_for",
            F.coalesce(F.col("imdbId"), F.col("known_for_tmdb_id"))
        )

        # Keep the flattened structure
        df = joined_df.select(
            "imdb_id",
            "tmdb_id",
            "name",
            "original_name",
            "media_type",
            "adult",
            "popularity",
            "gender",
            "known_for_department",
            "profile_path",
            "known_for",
            "known_for_popularity"
        )

        # 3. Value Formatting
        df_fmt = value_formatting(df)

        # Explicit gender codes
        gender_map = {0: "NotSet", 1: "Female", 2: "Male", 3: "Non-binary"}

        # Convert dictionary to a Spark map expression
        mapping_expr = F.create_map([F.lit(x) for x in sum(gender_map.items(), ())])
        # Apply the mapping
        df_fmt = df_fmt.withColumn('gender', mapping_expr.getItem(F.col('gender')))

        # 4. Variable Formatting
        # - name, original_name --> homogenize strings
        unidecode_udf = F.udf(lambda s: unidecode(s) if s else None, StringType()) # Remove diacritics/accents (e.g., À -> A, é -> e)
        df_fmt = df_fmt.withColumn("name", unidecode_udf(F.col("name")))
        df_fmt = df_fmt.withColumn("original_name", unidecode_udf(F.col("original_name")))

        # 5. Visualize formatting results
        log.info("Final Schema:")
        df_fmt.printSchema()
        log.info("Sample Data:")
        df_fmt.show(5, truncate=False)
        print(df_fmt.count())

        # 5. Write to PostgreSQL
        table_name = f"fmtted_TMDb_crewData"
        postgres_manager.write_dataframe(df_fmt, table_name)

    except Exception as e:
        log.error(f"Pipeline failed: {str(e)}", exc_info=True)
        raise
    finally:
        if spark:
            spark.stop()
        log.info("Spark session closed.")
