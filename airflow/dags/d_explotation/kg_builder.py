from rdflib import Graph, URIRef, Literal, Namespace
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, struct
from pyspark.sql.types import StringType
import requests
import time

# Configuration
GRAPHDB_ENDPOINT = "http://graphdb:7200/repositories/moviekg/statements"
BATCH_SIZE = 1000
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

# Namespaces
EX = Namespace("http://example.org/moviekg/")
XSD = Namespace("http://www.w3.org/2001/XMLSchema#")

# Initialize Spark
spark = SparkSession.builder \
    .appName("KG-Population") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()


def insert_to_graphdb(turtle_data):
    """Insert Turtle data into GraphDB with retry logic"""
    headers = {"Content-Type": "application/x-turtle"}
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(
                GRAPHDB_ENDPOINT,
                headers=headers,
                data=turtle_data,
                timeout=30
            )
            if response.status_code == 204:
                return True
            else:
                print(f"⚠️ Attempt {attempt + 1} failed: {response.status_code} - {response.text}")
        except Exception as e:
            print(f"⚠️ Attempt {attempt + 1} failed: {str(e)}")

        time.sleep(RETRY_DELAY * (attempt + 1))

    print(f"❌ Failed to insert batch after {MAX_RETRIES} attempts")
    return False


def process_minibatch(df, process_function):
    """Process Spark DataFrame minibatch and insert to GraphDB"""

    @udf(StringType())
    def row_to_turtle(row):
        g = Graph()
        g.bind("ex", EX)
        g.bind("xsd", XSD)
        try:
            process_function(g, row)
            return g.serialize(format="turtle")
        except Exception as e:
            print(f"⚠️ Row processing error: {str(e)}")
            return None

    # Process batch
    turtle_df = df.withColumn("turtle", row_to_turtle(struct(*df.columns)))
    valid_batches = turtle_df.filter("turtle IS NOT NULL").select("turtle")

    # Insert to GraphDB
    for turtle_row in valid_batches.collect():
        if turtle_row["turtle"]:
            success = insert_to_graphdb(turtle_row["turtle"])
            if not success:
                print("❌ Batch insertion failed, skipping...")


# Row Processing Functions
def process_title_basics(g, row):
    movie_uri = URIRef(f"{EX}movie/{row.tconst}")
    g.add((movie_uri, EX.imdbId, Literal(row.tconst)))
    g.add((movie_uri, EX.title, Literal(row.primaryTitle)))
    g.add((movie_uri, EX.originalTitle, Literal(row.originalTitle)))
    g.add((movie_uri, EX.isAdult, Literal(bool(row.isAdult), datatype=XSD.boolean)))
    if row.startYear != "\\N":
        g.add((movie_uri, EX.startYear, Literal(int(row.startYear), datatype=XSD.integer)))
    if row.endYear != "\\N":
        g.add((movie_uri, EX.endYear, Literal(int(row.endYear), datatype=XSD.integer)))
    if row.runtimeMinutes != "\\N":
        g.add((movie_uri, EX.runtimeMinutes, Literal(int(row.runtimeMinutes), datatype=XSD.integer)))

    if row.genres != "\\N":
        for genre in row.genres.split(','):
            genre_uri = URIRef(f"{EX}genre/{genre}")
            g.add((genre_uri, EX.genreName, Literal(genre)))
            g.add((movie_uri, EX.hasGenre, genre_uri))


def process_name_basics(g, row):
    person_uri = URIRef(f"{EX}person/{row.nconst}")
    g.add((person_uri, EX.personName, Literal(row.primaryName)))
    if row.birthYear != "\\N":
        g.add((person_uri, EX.birthYear, Literal(int(row.birthYear), datatype=XSD.integer)))
    if row.deathYear != "\\N":
        g.add((person_uri, EX.deathYear, Literal(int(row.deathYear), datatype=XSD.integer)))
    if row.primaryProfession != "\\N":
        for prof in row.primaryProfession.split(','):
            g.add((person_uri, EX.primaryProfession, Literal(prof)))
    if row.knownForTitles != "\\N":
        for tconst in row.knownForTitles.split(','):
            g.add((person_uri, EX.knownFor, URIRef(f"{EX}movie/{tconst}")))


def process_title_crew(g, row):
    movie_uri = URIRef(f"{EX}movie/{row.tconst}")
    if row.directors != "\\N":
        for director in row.directors.split(','):
            g.add((movie_uri, EX.directedBy, URIRef(f"{EX}person/{director}")))
    if row.writers != "\\N":
        for writer in row.writers.split(','):
            g.add((movie_uri, EX.writtenBy, URIRef(f"{EX}person/{writer}")))


def process_movietweetings_movies(g, row):
    movie_uri = URIRef(f"{EX}movie/{row.movie_id}")
    g.add((movie_uri, EX.title, Literal(row.movie_title)))
    if row.genres:
        for genre in row.genres.split('|'):
            genre_uri = URIRef(f"{EX}genre/{genre}")
            g.add((genre_uri, EX.genreName, Literal(genre)))
            g.add((movie_uri, EX.hasGenre, genre_uri))


def process_movietweetings_users(g, row):
    user_uri = URIRef(f"{EX}user/{row.user_id}")
    g.add((user_uri, EX.userId, Literal(row.user_id)))
    if row.twitter_id:
        g.add((user_uri, EX.twitterId, Literal(row.twitter_id)))


def process_movietweetings_ratings(g, row):
    rating_uri = URIRef(f"{EX}rating/{row.user_id}_{row.movie_id}_{row.rating_timestamp}")
    g.add((rating_uri, EX.ratingValue, Literal(float(row.rating), datatype=XSD.float)))
    if row.rating_timestamp:
        g.add((rating_uri, EX.ratingTimestamp, Literal(row.rating_timestamp, datatype=XSD.dateTime)))
    g.add((rating_uri, EX.ratedBy, URIRef(f"{EX}user/{row.user_id}")))
    g.add((rating_uri, EX.ratedMovie, URIRef(f"{EX}movie/{row.movie_id}")))


def process_tmdb(g, row):
    movie_uri = URIRef(f"{EX}movie/{row.imdb_id}")
    g.add((movie_uri, EX.tmdbId, Literal(row.tmdb_id)))
    if row.name:
        g.add((movie_uri, EX.title, Literal(row.name)))
    if row.original_name:
        g.add((movie_uri, EX.originalTitle, Literal(row.original_name)))
    if row.adult:
        g.add((movie_uri, EX.isAdult, Literal(bool(row.adult), datatype=XSD.boolean)))
    if row.popularity:
        g.add((movie_uri, EX.popularity, Literal(float(row.popularity), datatype=XSD.float)))


# Main Pipeline
def run_incremental_pipeline():
    # Process IMDb in batches
    for source, process_func in [
        ("/trusted/imdb/title_basics", process_title_basics),
        ("/trusted/imdb/name_basics", process_name_basics),
        ("/trusted/imdb/title_crew", process_title_crew)
    ]:
        print(f"📥 Processing {source}")
        df = spark.read.parquet(source)
        for i in range(0, df.count(), BATCH_SIZE):
            batch = df.limit(BATCH_SIZE).offset(i)
            process_minibatch(batch, process_func)
            print(f"✅ Processed batch {i // BATCH_SIZE + 1} of {source}")

    # Process MovieTweetings
    for source, process_func in [
        ("/trusted/movietweetings/movies", process_movietweetings_movies),
        ("/trusted/movietweetings/users", process_movietweetings_users),
        ("/trusted/movietweetings/ratings", process_movietweetings_ratings)
    ]:
        print(f"📥 Processing {source}")
        df = spark.read.parquet(source)
        for i in range(0, df.count(), BATCH_SIZE):
            batch = df.limit(BATCH_SIZE).offset(i)
            process_minibatch(batch, process_func)
            print(f"✅ Processed batch {i // BATCH_SIZE + 1} of {source}")

    # Process TMDB
    print("📥 Processing TMDB")
    tmdb_df = spark.read.parquet("/trusted/tmdb")
    for i in range(0, tmdb_df.count(), BATCH_SIZE):
        batch = tmdb_df.limit(BATCH_SIZE).offset(i)
        process_minibatch(batch, process_tmdb)
        print(f"✅ Processed TMDB batch {i // BATCH_SIZE + 1}")


if __name__ == "__main__":
    print("🚀 Starting KG population pipeline")
    run_incremental_pipeline()
    print("🎉 KG population completed successfully")
    spark.stop()