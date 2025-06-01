from rdflib import Graph, Namespace
import requests

def upload_base_schema():
    # Namespaces
    EX = Namespace("http://example.org/moviekg/")
    RDF = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
    RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")
    XSD = Namespace("http://www.w3.org/2001/XMLSchema#")

    # Initialize graph and bind namespaces
    g = Graph()
    g.bind("ex", EX)
    g.bind("rdf", RDF)
    g.bind("rdfs", RDFS)
    g.bind("xsd", XSD)

    # Define classes
    g.add((EX.Movie, RDF.type, RDFS.Class))
    g.add((EX.Person, RDF.type, RDFS.Class))
    g.add((EX.User, RDF.type, RDFS.Class))
    g.add((EX.Rating, RDF.type, RDFS.Class))
    g.add((EX.Genre, RDF.type, RDFS.Class))

    # Define properties
    properties = [
        (EX.imdbId, EX.Movie, XSD.string),
        (EX.tmdbId, EX.Movie, XSD.string),
        (EX.title, EX.Movie, XSD.string),
        (EX.originalTitle, EX.Movie, XSD.string),
        (EX.isAdult, EX.Movie, XSD.boolean),
        (EX.startYear, EX.Movie, XSD.integer),
        (EX.endYear, EX.Movie, XSD.integer),
        (EX.runtimeMinutes, EX.Movie, XSD.integer),
        (EX.hasGenre, EX.Movie, EX.Genre),
        (EX.genreName, EX.Genre, XSD.string),
        (EX.directedBy, EX.Movie, EX.Person),
        (EX.writtenBy, EX.Movie, EX.Person),
        (EX.personName, EX.Person, XSD.string),
        (EX.birthYear, EX.Person, XSD.integer),
        (EX.deathYear, EX.Person, XSD.integer),
        (EX.primaryProfession, EX.Person, XSD.string),
        (EX.knownFor, EX.Person, EX.Movie),
        (EX.userId, EX.User, XSD.string),
        (EX.twitterId, EX.User, XSD.string),
        (EX.ratingValue, EX.Rating, XSD.float),
        (EX.ratingTimestamp, EX.Rating, XSD.dateTime),
        (EX.ratedBy, EX.Rating, EX.User),
        (EX.ratedMovie, EX.Rating, EX.Movie),
        (EX.popularity, EX.Movie, XSD.float)
    ]

    for prop, domain, range in properties:
        g.add((prop, RDF.type, RDF.Property))
        g.add((prop, RDFS.domain, domain))
        g.add((prop, RDFS.range, range))

    # Upload to GraphDB
    GRAPHDB_ENDPOINT = "http://localhost:7200/repositories/moviekg/statements"
    headers = {"Content-Type": "application/x-turtle"}
    response = requests.post(
        GRAPHDB_ENDPOINT,
        headers=headers,
        data=g.serialize(format="turtle")
    )

    if response.status_code == 204:
        print("✅ Schema uploaded successfully")
    else:
        print(f"❌ Schema upload failed: {response.status_code} - {response.text}")