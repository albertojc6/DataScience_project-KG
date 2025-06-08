import pandas as pd
import requests
import numpy as np
from collections import Counter
import time
import argparse

# Configuration
GRAPHDB_ENDPOINT = "http://localhost:7200/repositories/moviekg"
EX = "http://example.org/moviekg/"
BATCH_SIZE = 200  # SPARQL batch size
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds
SAMPLE_SIZE = 100  # Movies to sample per cluster for detailed analysis

def run_sparql_query(query):
    """Execute SPARQL query with retry logic"""
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(
                GRAPHDB_ENDPOINT,
                data={"query": query},
                headers={"Accept": "application/sparql-results+json"},
                timeout=120
            )
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Attempt {attempt+1} failed: {response.status_code}")
        except Exception as e:
            print(f"Attempt {attempt+1} failed: {str(e)}")
        time.sleep(RETRY_DELAY * (attempt + 1))
    return None

def get_cluster_properties(cluster_df):
    """Analyze key properties for a cluster"""
    movie_ids = cluster_df['movie_id'].tolist()
    if not movie_ids:
        return {}
    
    # Prepare batch queries
    properties = {
        'genres': Counter(),
        'years': [],
        'runtimes': [],
        'ratings': [],
        'rating_count': 0
    }
    
    # Process in batches
    for i in range(0, len(movie_ids), BATCH_SIZE):
        batch_ids = movie_ids[i:i+BATCH_SIZE]
        id_list = ", ".join(f'"{mid}"' for mid in batch_ids)
        
        # Property query: genres, years, runtimes
        property_query = f"""
            PREFIX ex: <{EX}>
            SELECT ?movieId ?genre ?year ?runtime ?ratingScore
            WHERE {{
                ?movie ex:movieId ?movieId .
                FILTER(?movieId IN ({id_list}))
                OPTIONAL {{ ?movie ex:has_genre/ex:genre_name ?genre . }}
                OPTIONAL {{ ?movie ex:startYear ?year . }}
                OPTIONAL {{ ?movie ex:runtime ?runtime . }}
                OPTIONAL {{
                    ?rating ex:rating_movie ?movie ;
                            ex:rating_score ?ratingScore .
                }}
            }}
        """
        results = run_sparql_query(property_query)
        if not results:
            continue
            
        for binding in results['results']['bindings']:
            # Process genres
            if 'genre' in binding:
                genre = binding['genre']['value']
                properties['genres'][genre] += 1
                
            # Process year
            if 'year' in binding:
                try:
                    year = int(binding['year']['value'])
                    properties['years'].append(year)
                except:
                    pass
                    
            # Process runtime
            if 'runtime' in binding:
                try:
                    runtime = int(binding['runtime']['value'])
                    properties['runtimes'].append(runtime)
                except:
                    pass
                    
            # Process ratings
            if 'ratingScore' in binding:
                try:
                    rating = float(binding['ratingScore']['value'])
                    properties['ratings'].append(rating)
                    properties['rating_count'] += 1
                except:
                    pass

    # Calculate statistics
    stats = {}
    
    # Genre stats
    if properties['genres']:
        stats['top_genres'] = properties['genres'].most_common(5)
        stats['genre_coverage'] = len(properties['genres']) / len(movie_ids)
    else:
        stats['top_genres'] = []
        stats['genre_coverage'] = 0
        
    # Year stats
    if properties['years']:
        stats['avg_year'] = np.mean(properties['years'])
        stats['min_year'] = min(properties['years'])
        stats['max_year'] = max(properties['years'])
    else:
        stats['avg_year'] = None
        
    # Runtime stats
    if properties['runtimes']:
        stats['avg_runtime'] = np.mean(properties['runtimes'])
        stats['runtime_coverage'] = len(properties['runtimes']) / len(movie_ids)
    else:
        stats['avg_runtime'] = None
        stats['runtime_coverage'] = 0
        
    # Rating stats
    if properties['ratings']:
        stats['avg_rating'] = np.mean(properties['ratings'])
        stats['rating_coverage'] = properties['rating_count'] / len(movie_ids)
    else:
        stats['avg_rating'] = None
        stats['rating_coverage'] = 0
        
    return stats

def generate_cluster_profile(input_csv, output_file):
    """Generate detailed cluster profile report"""
    # Load clustering results
    df = pd.read_csv(input_csv, names=['movie_entity', 'cluster'])
    
    # Extract movie IDs
    df['movie_id'] = df['movie_entity'].str.extract(r'Movie/(tt\d+)')
    
    # Filter invalid clusters
    valid_df = df[df['cluster'].apply(lambda x: str(x).isdigit())]
    valid_df['cluster'] = valid_df['cluster'].astype(int)
    
    # Basic stats
    cluster_sizes = valid_df['cluster'].value_counts().sort_index()
    report = [
        "Enhanced Cluster Profiling Report",
        "=" * 50,
        f"Total movies: {len(valid_df)}",
        f"Number of clusters: {len(cluster_sizes)}",
        f"\nCluster Size Distribution:",
        cluster_sizes.to_string(),
        "\nDetailed Cluster Analysis:"
    ]
    
    # Analyze each cluster
    for cluster_id, size in cluster_sizes.items():
        cluster_df = valid_df[valid_df['cluster'] == cluster_id]
        
        # Sample movies for efficiency
        sample_df = cluster_df.sample(min(SAMPLE_SIZE, len(cluster_df)))
        
        report.append(f"\n{'=' * 50}")
        report.append(f"Cluster {cluster_id} (Size: {size})")
        report.append(f"Sampled {len(sample_df)} movies for analysis")
        
        # Get cluster properties
        properties = get_cluster_properties(sample_df)
        
        # Year analysis
        if properties['avg_year']:
            report.append(f"\n  -- Temporal Analysis --")
            report.append(f"  Average Year: {properties['avg_year']:.1f}")
            report.append(f"  Year Range: {properties['min_year']}-{properties['max_year']}")
        
        # Genre analysis
        if properties['top_genres']:
            report.append(f"\n  -- Genre Analysis (Coverage: {properties['genre_coverage']:.1%}) --")
            for genre, count in properties['top_genres']:
                report.append(f"  {genre}: {count} movies")
        
        # Runtime analysis
        if properties['avg_runtime']:
            report.append(f"\n  -- Runtime Analysis (Coverage: {properties['runtime_coverage']:.1%}) --")
            report.append(f"  Average Runtime: {properties['avg_runtime']:.1f} minutes")
        
        # Rating analysis
        if properties['avg_rating']:
            report.append(f"\n  -- Rating Analysis (Coverage: {properties['rating_coverage']:.1%}) --")
            report.append(f"  Average Rating: {properties['avg_rating']:.2f}/10")
        else:
            report.append(f"\n  No rating data available")
        
        # Sample movie titles
        sample_ids = sample_df['movie_id'].tolist()[:5]
        titles = []
        for i in range(0, len(sample_ids), BATCH_SIZE):
            batch = sample_ids[i:i+BATCH_SIZE]
            id_list = ", ".join(f'"{mid}"' for mid in batch)
            
            title_query = f"""
                PREFIX ex: <{EX}>
                SELECT ?movieId ?title
                WHERE {{
                    ?movie ex:movieId ?movieId ;
                           ex:title ?title .
                    FILTER(?movieId IN ({id_list}))
                }}
            """
            results = run_sparql_query(title_query)
            if results:
                for binding in results['results']['bindings']:
                    titles.append(f"{binding['title']['value']} ({binding['movieId']['value']})")
        
        report.append(f"\n  -- Sample Movies --")
        for title in titles[:5]:
            report.append(f"  • {title}")

    # Write report
    with open(output_file, 'w') as f:
        f.write("\n".join(report))
    print(f"Report generated: {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Cluster Profiling Tool')
    parser.add_argument('--input', required=True, help='Input CSV file path')
    parser.add_argument('--output', default='cluster_profile.txt', help='Output report file')
    args = parser.parse_args()
    
    generate_cluster_profile(args.input, args.output)