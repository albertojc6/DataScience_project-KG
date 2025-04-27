from dags.utils.other_utils import setup_logging
from dags.utils.hdfs_utils import HDFSClient
from pathlib import Path
import pandas as pd
import requests
import json
import os
import io

# Configure logging
log = setup_logging(__name__)

def load_TMDb(hdfs_client: HDFSClient, use_local = False):
    """
    Ingests data from The Movie Database (TMDb) via API, for additional movie crew information.
    Only loads data from crew involved in movie that are rated in some ingested tweet.

    Args:
        hdfs_client: object for interacting with HDFS easily.
        use_local: parameter to allow faster ingestions while testing
    """

    # Path configuration: temporal and hdfs directories
    tmp_dir = Path("/tmp/TMDb")
    tmp_dir.mkdir(parents=True, exist_ok=True)
    tmp_TMDb_file = tmp_dir / "crew_data.json"

    hdfs_dir = "/data/landing"
    TMDb_file = Path(hdfs_dir) / "TMDb/crew_data.json"

    # if file already has instances, only check for addition with API not, locally
    use_local = False if hdfs_client.exists(TMDb_file) else use_local
    if not use_local:
        # Define necessary data: filter crew that is involved in movies rated in some available tweet (i.e. in ratings.parquet)
        ratings_dir = "/data/landing/MovieTweetings/ratings.parquet"
        crew_dir = "/data/landing/IMDb/title.crew.tsv.gz"
        try:
            log.info("Filtering movie's crew from movies already rated")

            # 1. Read file contents into memory buffers
            rating_reader = hdfs_client.read_file(ratings_dir)
            rating_buffer = io.BytesIO(rating_reader)

            crew_reader = hdfs_client.read_file(crew_dir)
            crew_buffer = io.BytesIO(crew_reader)

            # 2. Read Parquet and tsv.gz from buffers
            df_ratings = pd.read_parquet(rating_buffer, engine='pyarrow')
            log.info(df_ratings.head())

            df_crew = pd.read_csv(crew_buffer, sep="\t", compression="gzip", dtype=str)
            log.info(df_crew.head())

            # 3. Filter crew so as to reduce API requests
            std_movieId = [f"tt{movie}" for movie in df_ratings['movie_id'].unique()]
            df_filtered = df_crew[df_crew["tconst"].isin(std_movieId)]

            # 4. Select unique people id's so as to make the requests
            unq_directors = df_filtered["directors"].fillna("").str.split(",").explode().unique()
            unq_writers = df_filtered["writers"].fillna("").str.split(",").explode().unique()
            
            unique_crew = set(unq_directors).union(unq_writers)
        except Exception as e:
            log.error(f"Error reading files: {str(e)}")
            raise
        
        # Retrieve data from API
        try:
            headers = {
                "accept": "application/json",
                "Authorization": f"Bearer {os.getenv('TMDb_API_Token')}"
            }

            def fetch_person_data(person_id: str, headers: dict) -> dict:
                """
                Fetch person data from TMDb API
                """
                url = f"https://api.themoviedb.org/3/find/{person_id}?external_source=imdb_id"
                try:
                    response = requests.get(url, headers=headers, timeout=10)
                    data = response.json()
                    if data.get("person_results"):
                        return {
                            "imdb_id": person_id,
                            "tmdb_data": data["person_results"][0]  # Take first result
                        }
                except requests.exceptions.RequestException as e:
                    log.warning(f"Failed to fetch {person_id}: {str(e)}")
                return None

            # Load existing data if the file exists
            TMDb_data = {}
            if hdfs_client.exists(TMDb_file):
                try:
                    # Read file contents via HDFS client
                    file_content = hdfs_client.read_file(TMDb_file)
                    TMDb_data = json.loads(file_content)
                except json.JSONDecodeError:
                    log.warning(f"File {TMDb_file} is corrupted. Starting fresh.")
                except Exception as e:
                    log.error(f"Error reading {TMDb_file}: {str(e)}")
                    raise
            
            existing_ids = set(TMDb_data.keys())
            new_crew = [pid for pid in unique_crew if pid not in existing_ids]

            # Fetch data only for new person IDs
            for person_id in new_crew:
                person_data = fetch_person_data(person_id, headers)
                if person_data:
                    TMDb_data[person_id] = person_data
            
            # Save the updated data in temporal file
            with open(tmp_TMDb_file, 'w') as f:
                json.dump(TMDb_data, f, indent=2)
            
            log.info(f"Total records: {len(TMDb_data)}. Newly added: {len(new_crew)}. Saved to {tmp_TMDb_file}")

        except Exception as e:
            print(f"Error reading files: {str(e)}")
            raise

    else:
        # Relative data from local filesystem
        tmp_dir = Path(__file__).parent / "local_data/TMDb"
        if not (tmp_dir / "crew_data.json").exists():
            log.warning(f"Missing local file. Retrieving from API...")
            load_TMDb(hdfs_client, use_local=False)
        log.info(f"Found {tmp_dir}!")

    try:
        # Store in HDFS
        log.info("Transferring files to HDFS...")
        hdfs_client.copy_from_local(str(tmp_dir), hdfs_dir)
        log.info(f"Transferred {tmp_dir} to HDFS at {hdfs_dir}")
    except Exception as e:
        log.error(f"Error transferring data to HDFS: {e}")
