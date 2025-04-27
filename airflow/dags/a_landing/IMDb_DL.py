from dags.utils.other_utils import setup_logging
from dags.utils.hdfs_utils import HDFSClient
import concurrent.futures
from pathlib import Path
import requests
import os

# Configure logging
log = setup_logging(__name__)

def load_IMDb(hdfs_client: HDFSClient, use_local: bool = False):
    """
    Loads some datasets from the IMDb, which consists of additional movie and authors' information

    Args:
        hdfs_client: object for interacting with HDFS easily.
        use_local: parameter to allow faster ingestions while testing
    """

    # Path coniguration: temporal and hdfs directories
    tmp_dir = Path("/tmp/IMDb")
    tmp_dir.mkdir(parents=True, exist_ok=True)

    hdfs_dir = "/data/landing/IMDb"
    
    files = ["name.basics.tsv.gz", "title.basics.tsv.gz", "title.crew.tsv.gz"]
    if not use_local:
        # Download files from web
        base_url = os.getenv("IMDb_URL")

        def download_file(file_name: str) -> None:
            """Download a single file using streaming to handle large content."""

            api_url = f"{base_url}/{file_name}"
            try:
                # Stream the download to handle large files efficiently
                response = requests.get(api_url, stream=True)
                response.raise_for_status()  # Raise error for bad status codes

                output_path = tmp_dir / file_name
                with open(output_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                log.info(f"Downloaded {file_name} successfully.")
            except requests.exceptions.RequestException as e:
                log.error(f"Failed to download {file_name}: {str(e)}")
            except IOError as e:
                log.error(f"Failed to write {file_name}: {str(e)}")

        log.info("Starting parallel downloads from IMDb...")
        # Use ThreadPoolExecutor to parallelize downloads
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            executor.map(download_file, files)
    
    else:
        # Relative path from DAG file to local data
        tmp_dir = Path(__file__).parent / "local_data"

        # Verify local files exist
        log.info("Checking for local files...")
        missing_files = [f for f in files if not (tmp_dir / f).exists()]
        if missing_files:
            log.warning(f"Missing local files: {missing_files}. Retrieving from web!")
            load_IMDb(hdfs_client, use_local = False)
            return
        log.info("All local files present.")

    # Store in HDFS
    log.info("Transferring files to HDFS...")
    hdfs_client.copy_from_local(str(tmp_dir), hdfs_dir)
    log.info(f"Transferred {tmp_dir} to HDFS at {hdfs_dir}")