from rdflib import Graph, URIRef, Literal, Namespace
import pandas as pd
import numpy as np
from rdflib.namespace import RDF
from pyspark.sql import SparkSession
import time
import requests
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf, struct
import logging

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
                logging.info(f"⚠️ Attempt {attempt + 1} failed: {response.status_code} - {response.text}")
        except Exception as e:
            logging.info(f"⚠️ Attempt {attempt + 1} failed: {str(e)}")

        time.sleep(RETRY_DELAY * (attempt + 1))

    logging.info(f"❌ Failed to insert batch after {MAX_RETRIES} attempts")
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
            logging.info(f"⚠️ Row processing error: {str(e)}")
            return None

    # Process batch
    turtle_df = df.withColumn("turtle", row_to_turtle(struct(*df.columns)))
    valid_batches = turtle_df.filter("turtle IS NOT NULL").select("turtle")

    # Insert to GraphDB
    for turtle_row in valid_batches.collect():
        if turtle_row["turtle"]:
            success = insert_to_graphdb(turtle_row["turtle"])
            if not success:
                logging.info("❌ Batch insertion failed, skipping...")

def create_graph_TMDB(g, row):
    if row['imdb_id'].startswith('nm'):
        person_uri = URIRef(f"{EX}Person/{row['imdb_id']}")
        g.add((person_uri,RDF.type,EX.Person))

        g.add((person_uri, EX.personId, Literal(row['imdb_id'], datatype=XSD.string)))
        g.add((person_uri, EX.name, Literal(row['name'], datatype=XSD.string)))
        g.add((person_uri, EX.gender, Literal(row['gender'], datatype=XSD.string)))
        g.add((person_uri, EX.popularity, Literal(row['popularity'], datatype=XSD.float)))

        if row['known_for'].startswith('tt'):
            movie_uri = URIRef(f"{EX}Movie/{row['known_for']}")
            g.add((movie_uri, RDF.type, EX.Movie))

            g.add((movie_uri, EX.isAdult, Literal(row['adult'], datatype=XSD.boolean)))

            known_for_uri = URIRef(f"{EX}Known_for/{row['imdb_id']}_{row['known_for']}")
            g.add((known_for_uri, RDF.type, EX.Known_for))

            g.add((person_uri, EX.isKnownfor, known_for_uri))
            g.add((known_for_uri, EX.known_for_movie, movie_uri))

            g.add((known_for_uri, EX.known_for_popularity, Literal(row['known_for_popularity'], datatype=XSD.float)))

    return g


def create_graph_Rating(g, row):
    user_uri = URIRef(f"{EX}User/{row['user_id']}")
    g.add((user_uri, RDF.type, EX.User))
    g.add((user_uri, EX.userId, Literal(row['user_id'], datatype=XSD.string)))
    if row['movie_id'].startswith('tt'):
        movie_uri = URIRef(f"{EX}Movie/{row['movie_id']}")
        g.add((movie_uri, RDF.type, EX.Movie))
        if pd.notna(row['rating']):
            movie_rating_uri = URIRef(f"{EX}Rating/{row['user_id']}_{row['movie_id']}")
            g.add((movie_rating_uri, RDF.type, EX.Rating))

            g.add((movie_rating_uri, EX.rating_date, Literal(row['rating_timestamp'], datatype=XSD.datetime)))
            g.add((movie_rating_uri, EX.rating_score, Literal(row['rating'], datatype=XSD.integer)))

            g.add((movie_rating_uri, EX.rating_movie, movie_uri))
            g.add((user_uri, EX.rates_movie, movie_rating_uri))

    return g


def create_graph_Movies(g, row):
    if row['movie_id'].startswith('tt'):
        movie_uri = URIRef(f"{EX}Movie/{row['movie_id']}")
        g.add((movie_uri, RDF.type, EX.Movie))

        g.add((movie_uri, EX.title, Literal(row['movie_title'], datatype=XSD.string)))
        g.add((movie_uri, EX.movieId, Literal(row['movie_id'], datatype=XSD.string)))

        genres = row['genres']
        # Check if 'genres' is a list and is not empty
        if isinstance(genres, np.ndarray):
            for genre in genres:
                # It's also good practice to check the genre itself isn't empty
                if genre and pd.notna(genre):
                    genre_uri = URIRef(f"{EX}Genre/{genre}")
                    g.add((genre_uri, RDF.type, EX.Genre))

                    g.add((genre_uri, EX.genre_name, Literal(genre, datatype=XSD.string)))
                    g.add((movie_uri, EX.has_genre, genre_uri))

    return g


def create_graph_name(g, row):
    if row['nconst'].startswith('nm'):
        person_uri = URIRef(f"{EX}Person/{row['nconst']}")
        g.add((person_uri, RDF.type, EX.Person))

        g.add((person_uri, EX.personId, Literal(row['nconst'], datatype=XSD.string)))
        g.add((person_uri, EX.name, Literal(row['primaryName'], datatype=XSD.string)))

        if pd.notna(row['birthYear']):
            g.add((person_uri, EX.birthYear, Literal(int(row['birthYear']), datatype=XSD.integer)))

        if pd.notna(row['deathYear']):
            g.add((person_uri, EX.deathYear, Literal(int(row['deathYear']), datatype=XSD.integer)))

        professions = row['primaryProfession']
        if isinstance(professions, np.ndarray):
            for i, profession in enumerate(professions):
                profession_uri = URIRef(f"{EX}Profession/{profession}")
                g.add((profession_uri, RDF.type, EX.Profession))

                g.add((profession_uri, EX.profession_name, Literal(professions[i], datatype=XSD.string)))

                if i == 0:
                    g.add((person_uri, EX.primary_profession, profession_uri))

                    known_for_titles = row['knownForTitles']

                    if isinstance(known_for_titles, np.ndarray):

                        for known_for_title in known_for_titles:
                            movie_uri = URIRef(f"{EX}Movie/{known_for_title}")
                            g.add((movie_uri, RDF.type, EX.Movie))
                            known_for_uri = URIRef(f"{EX}Known_for/{row['nconst']}_{known_for_title}")
                            g.add((known_for_uri, RDF.type, EX.Known_for))

                            g.add((person_uri, EX.isKnownfor, known_for_uri))
                            g.add((known_for_uri, EX.known_for_movie, movie_uri))

                else:
                    g.add((person_uri, EX.alternative_profession, profession_uri))

    return g


def create_graph_titles(g, row):
    if row['tconst'].startswith('tt'):
        movie_uri = URIRef(f"{EX}Movie/{row['tconst']}")
        g.add((movie_uri, RDF.type, EX.Movie))

        g.add((movie_uri, EX.title, Literal(row['primaryTitle'], datatype=XSD.string)))
        g.add((movie_uri, EX.movieId, Literal(row['tconst'], datatype=XSD.string)))
        g.add((movie_uri, EX.startYear, Literal(row['startYear'], datatype=XSD.integer)))
        g.add((movie_uri, EX.runtime, Literal(row['runtimeMinutes'], datatype=XSD.integer)))

        genres = row['genres']
        # Check if 'genres' is a list and is not empty
        if isinstance(genres, np.ndarray):
            for genre in genres:
                # It's also good practice to check the genre itself isn't empty
                if genre and pd.notna(genre):
                    genre_uri = URIRef(f"{EX}Genre/{genre}")
                    g.add((genre_uri, RDF.type, EX.Genre))

                    g.add((genre_uri, EX.genre_name, Literal(genre, datatype=XSD.string)))
                    g.add((movie_uri, EX.has_genre, genre_uri))

    return g


def create_graph_crew(g, row):
    if row['tconst'].startswith('tt'):
        movie_uri = URIRef(f"{EX}Movie/{row['tconst']}")
        g.add((movie_uri, RDF.type, EX.Movie))

        directors = row['directors']
        writers = row['writers']

        if isinstance(directors, np.ndarray):
            for director in directors:
                person_uri = URIRef(f"{EX}Person/{director}")
                g.add((person_uri, RDF.type, EX.Person))
                participates_uri = URIRef(f"{EX}Participates/{director}_{row['tconst']}")
                g.add((participates_uri, RDF.type, EX.Participates))
                profession_uri = URIRef(f"{EX}Profession/director")
                g.add((profession_uri, RDF.type, EX.Profession))

                g.add((participates_uri, EX.participates_role, profession_uri))
                g.add((participates_uri, EX.participates_person, person_uri))
                g.add((participates_uri, EX.participates_movie, movie_uri))

        if isinstance(writers, np.ndarray):
            for writer in writers:
                person_uri = URIRef(f"{EX}Person/{writer}")
                g.add((person_uri, RDF.type, EX.Person))
                participates_uri = URIRef(f"{EX}Participates/{writer}_{row['tconst']}")
                g.add((participates_uri, RDF.type, EX.Participates))
                profession_uri = URIRef(f"{EX}Profession/writer")
                g.add((profession_uri, RDF.type, EX.Profession))

                g.add((participates_uri, EX.participates_role, profession_uri))
                g.add((participates_uri, EX.participates_person, person_uri))
                g.add((participates_uri, EX.participates_movie, movie_uri))

    return g

def process_source(source,process_func):
    logging.info(f"📥 Processing {source}")
    df = spark.read.parquet(source)
    for i in range(0, df.count(), BATCH_SIZE):
        batch = df.limit(BATCH_SIZE).offset(i)
        process_minibatch(batch, process_func)
        logging.info(f"✅ Processed batch {i // BATCH_SIZE + 1} of {source}")


def to_graph_IMDB_title():
    source=''
    process_func = lambda x: x
    process_source(source,process_func)