from rdflib import Graph, URIRef, Literal, Namespace
import pandas as pd

# Initialize graph and namespaces
g = Graph()
EX = Namespace("http://example.org/moviekg/")
g.bind("ex", EX)


def process_imdb():
    # Title Basics
    title_basics = pd.read_csv("trusted/title_basics.csv")
    for _, row in title_basics.iterrows():
        movie_uri = URIRef(f"{EX}movie/{row['tconst']}")
        g.add((movie_uri, EX.imdbId, Literal(row['tconst'])))
        g.add((movie_uri, EX.title, Literal(row['primaryTitle'])))
        g.add((movie_uri, EX.originalTitle, Literal(row['originalTitle'])))
        g.add((movie_uri, EX.isAdult, Literal(bool(row['isAdult']), datatype=XS.boolean)))
        g.add((movie_uri, EX.startYear, Literal(row['startYear'])))
        if not pd.isna(row['endYear']):
            g.add((movie_uri, EX.endYear, Literal(row['endYear'])))
        g.add((movie_uri, EX.runtimeMinutes, Literal(row['runtimeMinutes'])))

        for genre in row['genres'].split(','):
            genre_uri = URIRef(f"{EX}genre/{genre}")
            g.add((genre_uri, EX.genreName, Literal(genre)))
            g.add((movie_uri, EX.hasGenre, genre_uri))

    # Name Basics
    name_basics = pd.read_csv("trusted/name_basics.csv")
    for _, row in name_basics.iterrows():
        person_uri = URIRef(f"{EX}person/{row['nconst']}")
        g.add((person_uri, EX.personName, Literal(row['primaryName'])))
        if not pd.isna(row['birthYear']):
            g.add((person_uri, EX.birthYear, Literal(row['birthYear'])))
        if not pd.isna(row['deathYear']):
            g.add((person_uri, EX.deathYear, Literal(row['deathYear'])))
        for prof in row['primaryProfession'].split(','):
            g.add((person_uri, EX.primaryProfession, Literal(prof)))
        for tconst in row['knownForTitles'].split(','):
            g.add((person_uri, EX.knownFor, URIRef(f"{EX}movie/{tconst}")))

    # Title Crew
    title_crew = pd.read_csv("trusted/title_crew.csv")
    for _, row in title_crew.iterrows():
        movie_uri = URIRef(f"{EX}movie/{row['tconst']}")
        for director in row['directors'].split(','):
            g.add((movie_uri, EX.directedBy, URIRef(f"{EX}person/{director}")))
        for writer in row['writers'].split(','):
            g.add((movie_uri, EX.writtenBy, URIRef(f"{EX}person/{writer}")))


def process_movietweetings():
    # Movies
    movies = pd.read_csv("trusted/movietweetings_movies.csv")
    for _, row in movies.iterrows():
        movie_uri = URIRef(f"{EX}movie/{row['movie_id']}")
        g.add((movie_uri, EX.title, Literal(row['movie_title'])))
        for genre in row['genres'].split('|'):
            genre_uri = URIRef(f"{EX}genre/{genre}")
            g.add((genre_uri, EX.genreName, Literal(genre)))
            g.add((movie_uri, EX.hasGenre, genre_uri))

    # Users
    users = pd.read_csv("trusted/movietweetings_users.csv")
    for _, row in users.iterrows():
        user_uri = URIRef(f"{EX}user/{row['user_id']}")
        g.add((user_uri, EX.userId, Literal(row['user_id'])))
        g.add((user_uri, EX.twitterId, Literal(row['twitter_id'])))

    # Ratings
    ratings = pd.read_csv("trusted/movietweetings_ratings.csv")
    for _, row in ratings.iterrows():
        rating_uri = URIRef(f"{EX}rating/{row['user_id']}_{row['movie_id']}_{row['rating_timestamp']}")
        g.add((rating_uri, EX.ratingValue, Literal(row['rating'])))
        g.add((rating_uri, EX.ratingTimestamp, Literal(row['rating_timestamp'], datatype=XS.dateTime)))
        g.add((rating_uri, EX.ratedBy, URIRef(f"{EX}user/{row['user_id']}")))
        g.add((rating_uri, EX.ratedMovie, URIRef(f"{EX}movie/{row['movie_id']}")))


def process_tmdb():
    tmdb_data = pd.read_csv("trusted/tmdb.csv")
    for _, row in tmdb_data.iterrows():
        movie_uri = URIRef(f"{EX}movie/{row['imdb_id']}")
        g.add((movie_uri, EX.tmdbId, Literal(row['tmdb_id'])))
        g.add((movie_uri, EX.title, Literal(row['name'])))
        g.add((movie_uri, EX.originalTitle, Literal(row['original_name'])))
        g.add((movie_uri, EX.isAdult, Literal(bool(row['adult']), datatype=XS.boolean)))
        g.add((movie_uri, EX.popularity, Literal(row['popularity'])))


# Execute pipeline
process_imdb()
process_movietweetings()
process_tmdb()

# Save KG
g.serialize(destination="knowledge_graph.ttl", format="turtle")