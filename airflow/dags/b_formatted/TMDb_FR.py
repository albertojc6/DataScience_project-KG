from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType, LongType, FloatType, MapType, DateType
from dags.utils.postgres_utils import PostgresManager
from dags.utils.other_utils import setup_logging
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from unidecode import  unidecode
import subprocess
import json
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
        # Connection to Spark
        spark = SparkSession.builder \
            .config("spark.jars", os.getenv('JDBC_URL')) \
            .appName("TMDbFormatter") \
            .getOrCreate()

        # 1. Read JSON data (dynamic keys at root)
        crewData_path = f"{os.getenv('HDFS_FS_URL')}/data/landing/TMDb/crew_data.json"
        df = spark.read \
            .option("multiLine", True) \
            .option("mode", "PERMISSIVE") \
            .json(crewData_path)
        # Obs: "multiline" to read records that can span multiple lines. UTF-8 encoding is default.

        df.printSchema()
        df.show(5)

    except Exception as e:
        log.error(f"Pipeline failed: {str(e)}", exc_info=True)
        raise
    finally:
        if spark:
            spark.stop()
        log.info("Spark session closed.")

