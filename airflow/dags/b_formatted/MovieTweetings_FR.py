from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from dags.utils.postgres_utils import PostgresManager
from dags.utils.other_utils import setup_logging
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from unidecode import unidecode
import subprocess
import os

# Configure logging
log = setup_logging(__name__)

def format_MovieTweetings(postgres_manager: PostgresManager):
    """
    Formats MovieTweetings data according to a common relational data model, using PostgreSQL

    Args:
        postgres_manager: object to facilitate the interaction with PostgreSQL server
    """

    # Clean up temporary files from previous stage
    subprocess.run(["rm", "-rf", f'/tmp/MovieTweetings/'], check=True)

    hdfs_landing = f"{os.getenv('HDFS_FS_URL')}/data/landing/MovieTweetings/"

    # Format each dataset from MovieTweetings with its corresponding processing
    format_movies(hdfs_landing, postgres_manager)
    format_users(hdfs_landing, postgres_manager)
    format_ratings(hdfs_landing, postgres_manager)


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

def format_movies(landing_path: str, postgres_manager: PostgresManager):
    """
    Formats movies datset from MovieTweetings

    Args:
        landing_path: the directory where movies.parquet is located
        postgres_manager: object to facilitate the interaction with PostgreSQL server
    """

    spark = None
    log.info("Formatting the movies dataset from MovieTweetings ...")
    try:
        # Connection to Spark
        spark = SparkSession.builder \
            .config("spark.jars", os.getenv('JDBC_URL')) \
            .appName("MoviesFormatter") \
            .getOrCreate()

        # 1. Read data from RDD in HDFS and create schema
        schema =  StructType([
            StructField("movie_id", StringType(), False), # PK
            StructField("movie_title", StringType(), True),
            StructField("genres", StringType(), True)
        ])

        movies_path = landing_path + "movies.parquet"
        df = spark.read.schema(schema).parquet(movies_path)

        # 2. Variable Formatting
        # - movie_title --> movie_title (movie_year): split and Data Types Conversion!
        df = df.withColumn("movie_year", F.regexp_extract(F.col("movie_title"), r"\((\d{4})\)$", 1).cast("int")) \
                .withColumn("movie_title", F.regexp_replace(F.col("movie_title"), r"\s*\(\d{4}\)$", "").cast("string"))
        
        # - movie_title --> homogenize strings
        unidecode_udf = F.udf(lambda s: unidecode(s) if s else None, StringType())
        df = df.withColumn("movie_title", unidecode_udf(F.col("movie_title"))) # Remove diacritics/accents (e.g., À -> A, é -> e)

        # - genres --> genre1|genre2|genre3: split into an array column [genre1, genre2, genre3]
        df = df.withColumn("genres", F.split(F.col("genres"), "\\|"))

        # - movie_id --> add "tt" for homogenization with IMDB movie identfiers
        df = df.withColumn("movie_id", F.concat(F.lit("tt"), df["movie_id"]))

        # 3. Value Formatting
        df = value_formatting(df)

        # 4. Reorganize columns
        df = df.select("movie_id", "movie_title", "movie_year", "genres")

        # 5. Visualize formatting results
        log.info("Final Schema:")
        df.printSchema()
        log.info("Sample Data:")
        df.show(5, truncate=False)

        # 6. Write to PostgreSQL
        table_name = f"fmtted_MovTweet_movies"
        postgres_manager.write_dataframe(df, table_name)

    except Exception as e:
        log.error(f"Pipeline failed: {str(e)}", exc_info=True)
        raise
    finally:
        if spark:
            spark.stop()
        log.info("Spark session closed.")

def format_users(landing_path: str, postgres_manager: PostgresManager):
    """
    Formats users database from MovieTweetings

    Args:
        landing_path: the directory where users.parquet is located
        postgres_manager: object to facilitate the interaction with PostgreSQL server
    """

    spark = None
    log.info("Formatting the users dataset from MovieTweetings ...")
    try:
        # Connection to Spark
        spark = SparkSession.builder \
            .config("spark.jars", os.getenv('JDBC_URL')) \
            .appName("UsersFormatter") \
            .getOrCreate()

        # 1. Read data from RDD in HDFS and create schema
        schema =  StructType([
            StructField("user_id", StringType(), False), # PK
            StructField("twitter_id", StringType(), True)
            ])

        users_path = landing_path + "users.parquet"
        df = spark.read.schema(schema).parquet(users_path)

        # 2. Value Formatting
        df = value_formatting(df)

        # 3. Visualize formatting results
        log.info("Final Schema:")
        df.printSchema()
        log.info("Sample Data:")
        df.show(5, truncate=False)

        # 4. Write to PostgreSQL
        table_name = f"fmtted_MovTweet_users"
        postgres_manager.write_dataframe(df, table_name)

    except Exception as e:
        log.error(f"Pipeline failed: {str(e)}", exc_info=True)
        raise
    finally:
        if spark:
            spark.stop()
        log.info("Spark session closed.")

def format_ratings(landing_path: str, postgres_manager: PostgresManager):
    """
    Formats ratings datset from MovieTweetings

    Args:
        landing_path: the directory where ratings.parquet is located
        postgres_manager: object to facilitate the interaction with PostgreSQL server
    """

    spark = None
    log.info("Formatting the ratings dataset from MovieTweetings ...")
    try:
        # Connection to Spark
        spark = SparkSession.builder \
            .config("spark.jars", os.getenv('JDBC_URL')) \
            .appName("RatingsFormatter") \
            .getOrCreate()

        # 1. Read data from RDD in HDFS and create schema
        schema =  StructType([
            StructField("user_id", StringType(), False), # PK
            StructField("movie_id", StringType(), False), # PK
            StructField("rating", StringType(), True),
            StructField("rating_timestamp", StringType(), True)
        ])

        ratings_path = landing_path + "ratings.parquet"
        df = spark.read.schema(schema).parquet(ratings_path)

        # 2. Variable Formatting
        # - rating --> convert from String to Integer
        df = df.withColumn("rating", F.col("rating").cast(IntegerType()))

        # - rating_timestamp --> convert UNIX timestamp to proper timestamp type
        df = df.withColumn("rating_timestamp", F.from_unixtime(F.col("rating_timestamp")).cast(TimestampType()))

        # - movie_id --> add "tt" for homogenization with IMDB movie identfiers
        df = df.withColumn("movie_id", F.concat(F.lit("tt"), df["movie_id"]))

        # 3. Value Formatting
        df = value_formatting(df)

        # 4. Visualize formatting results   
        log.info("Final Schema:")
        df.printSchema()
        log.info("Sample Data:")
        df.show(5, truncate=False)

        # 5. Write to PostgreSQL
        table_name = f"fmtted_MovTweet_ratings"
        postgres_manager.write_dataframe(df, table_name)

    except Exception as e:
        log.error(f"Pipeline failed: {str(e)}", exc_info=True)
        raise
    finally:
        if spark:
            spark.stop()
        log.info("Spark session closed.")