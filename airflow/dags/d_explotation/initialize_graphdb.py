from rdflib import Graph, Namespace
import requests
from dags.d_explotation.schema_upload import upload_base_schema
import logging

logging.basicConfig(level=logging.INFO)

REPO_ID = "moviekg"
GDB_REST_URL = "http://localhost:7200/rest/repositories"

def already_exists_repo():
    response = requests.get(GDB_REST_URL)

    if response.status_code == 200:
        repos = response.json()
        exists = any(repo["id"] == REPO_ID for repo in repos)

        if exists:
            print("hey")
            logging.info(f"Repository '{REPO_ID}' already exists.")
        else:
            logging.info(f"Repository '{REPO_ID}' does not exist.")
    else:
        raise Exception(f"Failed to fetch repository list: {response.status_code}. Check GraphDB status")

    return exists

def create_repository():
    with open("moviekg-config.ttl", "rb") as f:
        files = {
            "config": ("moviekg-config.ttl", f, "application/x-turtle"),
        }

        response = requests.post(
            "http://localhost:7200/rest/repositories",
            files=files
        )

        if response.status_code == 201:
            print("Repository created successfully.")
        elif response.status_code == 409:
            print("Repository already exists.")
        else:
            print(f"Error creating repository: {response.status_code}, {response.text}")

def initialize_graphDB():

    exists = already_exists_repo() # Check if needs to be initialized
    if not exists:
        create_repository()
        upload_base_schema()


