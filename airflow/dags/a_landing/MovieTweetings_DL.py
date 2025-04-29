from dags.utils.other_utils import setup_logging
from dags.utils.hdfs_utils import HDFSClient
from pathlib import Path
import pandas as pd
import requests
import hashlib
import os

# Configure logging
log = setup_logging(__name__)

def calculate_file_hash(content):
    """Calculate MD5 hash of file content"""
    return hashlib.md5(content).hexdigest()

def load_MovieTweetings(hdfs_client: HDFSClient):
    """
    Loads the MovieTweetings Dataset, which consists of ratings extracted from tweets: users.dat, items.dat & ratings.dat
    Allow for incremental data ingestion, with datasets' hash comparison

    Args:
        hdfs_client: object for interacting with HDFS easily.
    """

    # Path coniguration: temporal and hdfs directories
    tmp_dir = Path("/tmp/MovieTweetings")
    tmp_dir.mkdir(parents=True, exist_ok=True)

    hdfs_dir = "/data/landing/MovieTweetings"

    # Use a directory for hash storage
    hash_dir = Path("/tmp/hashes")
    hash_dir.mkdir(exist_ok=True)
    
    # URL for retrieving 200k ratings
    base_url = os.getenv("MovieTweetings_URL")
    files = {
        "movies.dat": ["movie_id", "movie_title", "genres"],
        "ratings.dat": ["user_id", "movie_id", "rating", "rating_timestamp"],
        "users.dat": ["user_id", "twitter_id"]
    }
    
    for f in files.keys():
        # First check if file exists in HDFS
        hash_file = hash_dir / f"{f}.md5"
        
        # Download current version from source
        api_url = f"{base_url}/{f}"
        response = requests.get(api_url)

        if response.status_code != 200:
            log.warning(f"Failed to download {f}. Status code: {response.status_code}")
            continue
        
        # Calculate hash of current content
        curr_content = response.content
        curr_hash = calculate_file_hash(curr_content)
        
        # Check if file has changed
        if hash_file.exists():
            with open(hash_file, 'r') as hf:
                stored_hash = hf.read().strip()
                if curr_hash == stored_hash:
                    log.info(f"File {f} hasn't changed since last run. Skipping processing.")
                    continue
            
        # File has changed, process it
        log.info(f"Changes detected in {f}. Processing...")
        output_file = tmp_dir / f
        
        with open(output_file, "wb") as local_f:
            local_f.write(curr_content)
            log.info(f"File {f} downloaded!")
        
        try:
            # Read data as csv, with .dat separator and column names
            df = pd.read_csv(output_file, sep='::', header=None, names=files[f], encoding='utf-8', engine = 'python',
                            quoting=3, escapechar='\\', on_bad_lines='skip', comment = '#', dtype=str)
            
            # Convert to a suitable format: Parquet
            f_parquet = output_file.with_suffix('.parquet')
            df.to_parquet(f_parquet, engine="pyarrow", compression='gzip')
            log.info(f"Converted {f} to {f_parquet}")
            
            # Print memory usage optimization
            dat_size = os.path.getsize(output_file)
            parquet_size = os.path.getsize(f_parquet)
            log.info(f"Size reduction: {dat_size} bytes → {parquet_size} bytes ({(1 - parquet_size/dat_size)*100:.1f}% saved)")
            
            # Store in HDFS
            hdfs_client.copy_from_local(str(f_parquet), hdfs_dir)
            log.info(f"Transferred {f_parquet} to HDFS")
            
            # Save the hash for next run comparison
            with open(hash_file, 'w') as hf:
                hf.write(curr_hash)
                log.info(f"Updated hash for {f} for future change detection")
                
        except Exception as e:
            log.error(f"Error processing {f}: {e}")
            raise