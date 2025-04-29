from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType
from dags.utils.postgres_utils import PostgresManager
from dags.utils.other_utils import setup_logging
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from unidecode import  unidecode
import subprocess
import os

# Configure logging
log = setup_logging(__name__)

def format_IMDb(postgres_manager: PostgresManager):
    """
    Formats IMDb data according to a common relational data model, using PostgreSQL

    Args:
        postgres_manager: object to facilitate the interaction with PostgreSQL server
    """

    # Clean up temporary files from previous stage
    subprocess.run(["rm", "-rf", f'/tmp/IMDb/'], check=True)

    hdfs_landing = f"{os.getenv('HDFS_FS_URL')}/data/landing/IMDb/"

    # Format each dataset from IMDb with its corresponding processing
    format_titleBasics(hdfs_landing, postgres_manager)
    format_titleCrew(hdfs_landing, postgres_manager)
    format_nameBasics(hdfs_landing, postgres_manager)


def value_formatting(df):
    """
    Defines correct format for all string values, so as to gain consistency.
    """
    # Collapse multiple spaces and trim all string columns -> consistency!
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for col in string_cols:
        df = df.withColumn(col, F.trim(F.regexp_replace(F.col(col), r"\s+", " ")))

    # - Homogenize NA indicators
    na_values = ['', 'NA', 'N/A', 'NaN', 'NULL', '\\N']
    for col in string_cols:
        df = df.withColumn(col, F.when(F.col(col).isin(na_values), None).otherwise(F.col(col)))
    
    return df

def format_titleBasics(landing_path: str, postgres_manager: PostgresManager):
    """
    Formats title_basics datset from IMDb

    Args:
        landing_path: the directory where title.basics.tsv.gz is located
        postgres_manager: object to facilitate the interaction with PostgreSQL server
    """

    spark = None
    log.info("Formatting the titleBasics dataset from IMDb ...")
    try:
        # Connection to Spark
        spark = SparkSession.builder \
            .config("spark.jars", os.getenv('JDBC_URL')) \
            .appName("titleBasicsFormatter") \
            .getOrCreate()

        # 1. Read data from RDD in HDFS and create schema
        schema =  StructType([
            StructField("tconst", StringType(), False),          # PK: alphanumeric unique identifier of the title
            StructField("titleType", StringType(), True),        # the type/format of the title (e.g. movie, tvseries)
            StructField("primaryTitle", StringType(), True),     # the more popular title
            StructField("originalTitle", StringType(), True),    # original title, in the original language
            StructField("isAdult", IntegerType(), True),         # 0: non-adult title; 1: adult title
            StructField("startYear", IntegerType(), True),       # release year of a title
            StructField("endYear", StringType(), True),          # TV Series end year. '\N' for all other title types
            StructField("runtimeMinutes", IntegerType(), True),  # runtime of the title, in minutes
            StructField("genres", StringType(), True) # includes up to three genres associated with the title
        ])

        titleBasics_path = landing_path + "title.basics.tsv.gz"
        df = spark.read.format("csv") \
                        .option("header", True) \
                        .option("sep", "\t") \
                        .schema(schema) \
                        .load(titleBasics_path)
        
        # 2. Variable Formatting
        # - primaryTitle and originalTitle --> homogenize strings
        unidecode_udf = F.udf(lambda s: unidecode(s) if s else None, StringType()) # Remove diacritics/accents (e.g., À -> A, é -> e)
        df = df.withColumn("primaryTitle", unidecode_udf(F.col("primaryTitle")))
        df = df.withColumn("originalTitle", unidecode_udf(F.col("originalTitle")))

        # - isAdult --> convert to Boolean
        df = df.withColumn("isAdult", F.col("isAdult") == 1)

        # - genres --> genre1|genre2|genre3: split into an array column [genre1, genre2, genre3]
        df = df.withColumn("genres", F.split(F.col("genres"), ","))

        # 3. Value Formatting
        df = value_formatting(df)

        # 4. Visualize formatting results
        log.info("Final Schema:")
        df.printSchema()
        log.info("Sample Data:")
        df.show(5, truncate=False)

        # 5. Write to PostgreSQL
        table_name = f"fmtted_IMDb_titleBasics"
        short_df = df.limit(100000) # OJO
        postgres_manager.write_dataframe(short_df, table_name)

    except Exception as e:
        log.error(f"Pipeline failed: {str(e)}", exc_info=True)
        raise
    finally:
        if spark:
            spark.stop()
        log.info("Spark session closed.")


def format_titleCrew(landing_path: str, postgres_manager: PostgresManager):
    """
    Formats titleCrew datset from IMDb

    Args:
        landing_path: the directory where title.crew.tsv.gz is located
        postgres_manager: object to facilitate the interaction with PostgreSQL server
    """

    spark = None
    log.info("Formatting the titleCrew dataset from IMDb ...")
    try:
        # Connection to Spark
        spark = SparkSession.builder \
            .config("spark.jars", os.getenv('JDBC_URL')) \
            .appName("titleCrewFormatter") \
            .getOrCreate()

        # 1. Read data from RDD in HDFS and create schema
        schema = StructType([
            StructField("tconst", StringType(), False),    # PK: alphanumeric unique identifier of the title
            StructField("directors", StringType(), True), # director(s) of the given title
            StructField("writers", StringType(), True)    # writer(s) of the given title
        ])

        titleCrew_path = landing_path + "title.crew.tsv.gz"
        df = spark.read.format("csv") \
                        .option("header", True) \
                        .option("sep", "\t") \
                        .schema(schema) \
                        .load(titleCrew_path)
        
        # 2. Value Formatting
        df = value_formatting(df)

        # 3. Variable Formatting
        # directors, writers -> convert string columns to arrays
        df = df.withColumn("directors", F.split("directors", ",")) \
               .withColumn("writers", F.split("writers", ","))

        # 4. Visualize formatting results
        log.info("Final Schema:")
        df.printSchema()
        log.info("Sample Data:")
        df.show(5, truncate=False)

        # 5. Write to PostgreSQL
        table_name = f"fmtted_IMDb_titleCrew"
        short_df = df.limit(100000) # OJO
        postgres_manager.write_dataframe(short_df, table_name)

    except Exception as e:
        log.error(f"Pipeline failed: {str(e)}", exc_info=True)
        raise
    finally:
        if spark:
            spark.stop()
        log.info("Spark session closed.")

def format_nameBasics(landing_path: str, postgres_manager: PostgresManager):
    """
    Formats nameBasics datset from IMDb

    Args:
        landing_path: the directory where name.basics.tsv.gz is located
        postgres_manager: object to facilitate the interaction with PostgreSQL server
    """

    spark = None
    log.info("Formatting the nameBasics dataset from IMDb ...")
    try:
        # Connection to Spark
        spark = SparkSession.builder \
            .config("spark.jars", os.getenv('JDBC_URL')) \
            .appName("nameBasicsFormatter") \
            .getOrCreate()

        # 1. Read data from RDD in HDFS and create schema
        schema = StructType([
            StructField("nconst", StringType(), False),            # PK: alphanumeric unique identifier of the name/person
            StructField("primaryName", StringType(), True),        # name by which the person is most often credited
            StructField("birthYear", StringType(), True),          # (YYYY or null)
            StructField("deathYear", StringType(), True),          # (YYYY, '\N', or null)   
            StructField("primaryProfession", StringType(), True),  # the top-3 professions of the person
            StructField("knownForTitles", StringType(), True)      # titles the person is known for
        ])

        nameBasics_path = landing_path + "name.basics.tsv.gz"
        df = spark.read.format("csv") \
                        .option("header", True) \
                        .option("sep", "\t") \
                        .schema(schema) \
                        .load(nameBasics_path)
        
        # 2. Value Formatting
        df = value_formatting(df)

        # 3. Variable Formatting
        # - birthYear, deathYear --> convert to IntegerType
        df = df.withColumn("birthYear", F.col("birthYear").cast("integer")) \
               .withColumn("deathYear", F.col("deathYear").cast("integer"))
        
        # - primaryProfession, knownForTitles --> convert to arrays of strings
        df = df.withColumn("primaryProfession", F.split(F.col("primaryProfession"), ",")) \
               .withColumn("knownForTitles", F.split(F.col("knownForTitles"), ","))

        # 4. Visualize formatting results
        log.info("Final Schema:")
        df.printSchema()
        log.info("Sample Data:")
        df.show(5, truncate=False)

        # 5. Write to PostgreSQL
        table_name = f"fmtted_IMDb_nameBasics"
        short_df = df.limit(100000) # OJO
        postgres_manager.write_dataframe(short_df, table_name)

    except Exception as e:
        log.error(f"Pipeline failed: {str(e)}", exc_info=True)
        raise
    finally:
        if spark:
            spark.stop()
        log.info("Spark session closed.")