import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import os
import tempfile
import logging
import requests
from scipy.stats import linregress
from dags.utils.hdfs_utils import HDFSClient

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize HDFS client
hdfs_client = HDFSClient()
hdfs_analysis_dir = "/data/analysis"

# GraphDB endpoint configuration
GRAPHDB_ENDPOINT = "http://graphdb:7200/repositories/moviekg"
HEADERS = {
    'Accept': 'application/sparql-results+json',
    'Content-Type': 'application/x-www-form-urlencoded'
}

def run_sparql_query(query):
    """Execute SPARQL query and return results as DataFrame"""
    response = requests.post(
        GRAPHDB_ENDPOINT,
        headers=HEADERS,
        data={"query": query},
        timeout=120
    )
    response.raise_for_status()
    data = response.json()
    
    if not data['results']['bindings']:
        return pd.DataFrame()
    
    cols = data['head']['vars']
    rows = []
    for binding in data['results']['bindings']:
        row = [binding.get(col, {}).get('value', None) for col in cols]
        rows.append(row)
    
    logger.info(f"Query returned {len(rows)} rows")
    return pd.DataFrame(rows, columns=cols)

def save_and_upload_plot(fig, filename):
    """Save plot locally and upload to HDFS"""
    temp_dir = tempfile.mkdtemp()
    local_path = os.path.join(temp_dir, filename)
    fig.savefig(local_path, bbox_inches='tight', dpi=300)
    plt.close(fig)
    
    hdfs_path = f"{hdfs_analysis_dir}/{filename}"
    hdfs_client.copy_from_local(local_path, hdfs_path)
    logger.info(f"Uploaded {filename} to HDFS")

def execute_analysis():
    """Main function to execute all analyses"""
    analyses = [
        ("Profession-Gender Distribution", 
        """
        PREFIX ex: <http://example.org/moviekg/>
        SELECT ?professionName ?gender (COUNT(?person) as ?count)
        WHERE {
            ?person a ex:Person ;
                    ex:gender ?gender ;
                    ex:primary_profession ?profession .
            ?profession ex:profession_name ?professionName .
        }
        GROUP BY ?professionName ?gender
        """, 
        lambda df: create_profession_gender_plot(df)),
        
        ("Genre-Temporal Evolution", 
        """
        PREFIX ex: <http://example.org/moviekg/>
        SELECT ?decade ?genreName (COUNT(?movie) as ?count)
        WHERE {
            ?movie a ex:Movie ;
                   ex:startYear ?year ;
                   ex:has_genre ?genre .
            ?genre ex:genre_name ?genreName .
            BIND (FLOOR(?year/10)*10 AS ?decade)
            FILTER (?year > 1900)
        }
        GROUP BY ?decade ?genreName
        ORDER BY ?decade
        """, 
        lambda df: create_genre_evolution_plots(df)),
        
        ("Director-Writer Collaboration", 
        """
        PREFIX ex: <http://example.org/moviekg/>
        SELECT ?directorName ?writerName (COUNT(?movie) as ?collabCount)
        WHERE {
            ?movie a ex:Movie .
            
            ?dirParticipates a ex:Participates ;
                             ex:participates_movie ?movie ;
                             ex:participates_person ?director ;
                             ex:participates_role ?dirRole .
            ?dirRole ex:profession_name "director" .
            ?director ex:name ?directorName .
            
            ?writerParticipates a ex:Participates ;
                               ex:participates_movie ?movie ;
                               ex:participates_person ?writer ;
                               ex:participates_role ?writerRole .
            ?writerRole ex:profession_name "writer" .
            ?writer ex:name ?writerName .
            
            FILTER (?director != ?writer)
        }
        GROUP BY ?directorName ?writerName
        ORDER BY DESC(?collabCount)
        LIMIT 20
        """, 
        lambda df: create_collaboration_heatmap(df)),
        
        ("Popularity-Rating Correlation", 
        """
        PREFIX ex: <http://example.org/moviekg/>
        SELECT ?person ?popularity (AVG(?rating) as ?avgRating)
        WHERE {
            ?person a ex:Person ;
                    ex:popularity ?popularity ;
                    ex:isKnownfor ?knownFor .
            ?knownFor ex:known_for_movie ?movie .
            ?rating a ex:Rating ;
                    ex:rating_movie ?movie ;
                    ex:rating_score ?ratingScore .
            BIND(?ratingScore as ?rating)
        }
        GROUP BY ?person ?popularity
        """, 
        lambda df: create_popularity_rating_plot(df)),
        
        ("Career Longevity", 
        """
        PREFIX ex: <http://example.org/moviekg/>
        SELECT ?name (MIN(?year) as ?debut) (MAX(?year) as ?lastActive)
        WHERE {
            ?person a ex:Person ;
                    ex:name ?name ;
                    ex:isKnownfor ?knownFor .
            ?knownFor ex:known_for_movie ?movie .
            ?movie ex:startYear ?year .
        }
        GROUP BY ?person ?name
        HAVING (COUNT(?year) > 5)
        ORDER BY DESC(?lastActive - ?debut)
        LIMIT 20
        """, 
        lambda df: create_career_longevity_plot(df)),
        
        ("Genre-Rating Relationship", 
        """
        PREFIX ex: <http://example.org/moviekg/>
        SELECT ?genreName (AVG(?ratingScore) as ?avgRating)
        WHERE {
            ?movie a ex:Movie ;
                   ex:has_genre ?genre .
            ?genre ex:genre_name ?genreName .
            ?rating a ex:Rating ;
                    ex:rating_movie ?movie ;
                    ex:rating_score ?ratingScore .
        }
        GROUP BY ?genreName
        """, 
        lambda df: create_genre_rating_plot(df)),
        
        ("Top Rated Movies by Genre", 
        """
        PREFIX ex: <http://example.org/moviekg/>
        SELECT ?title ?genreName (AVG(?ratingScore) as ?avgRating) (COUNT(?rating) as ?ratingCount)
        WHERE {
            ?movie a ex:Movie ;
                   ex:title ?title ;
                   ex:has_genre ?genre .
            ?genre ex:genre_name ?genreName .
            ?rating a ex:Rating ;
                    ex:rating_movie ?movie ;
                    ex:rating_score ?ratingScore .
        }
        GROUP BY ?title ?genreName
        HAVING (COUNT(?rating) >= 10)
        ORDER BY DESC(?avgRating)
        LIMIT 20
        """, 
        lambda df: create_top_movies_plot(df)),
        
        ("Person Popularity Distribution", 
        """
            PREFIX ex: <http://example.org/moviekg/>
            SELECT ?professionName ?popularity
            WHERE {
                ?person a ex:Person ;
                        ex:popularity ?popularity ;
                        ex:primary_profession ?profession .
                ?profession ex:profession_name ?professionName .
                FILTER (?popularity > 0)
            }
        """, 
        lambda df: create_popularity_distribution_plot(df))
    ]
    
    for name, query, plot_func in analyses:
        logger.info(f"Starting analysis: {name}")
        df = run_sparql_query(query)
        if not df.empty:
            plot_func(df)
        else:
            logger.warning(f"No data for {name}")

# Plot creation functions
def create_profession_gender_plot(df):
    df['count'] = df['count'].astype(int)
    pivot = df.pivot(index='professionName', columns='gender', values='count').fillna(0)
    
    fig, ax = plt.subplots(figsize=(14, 10))
    pivot.plot(kind='bar', stacked=True, ax=ax, color=['#FF6B6B', '#4ECDC4', '#45B7D1'])
    ax.set_title('Gender Distribution by Profession', fontsize=16, fontweight='bold')
    ax.set_ylabel('Count')
    ax.set_xlabel('Profession')
    plt.xticks(rotation=45, ha='right')
    plt.legend(title='Gender', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    save_and_upload_plot(fig, "profession_gender_distribution.png")

def create_genre_evolution_plots(df):
    df['decade'] = df['decade'].astype(int)
    df['count'] = df['count'].astype(int)
    
    # Line Chart (Top-5 genres)
    top_genres = df.groupby('genreName')['count'].sum().nlargest(5).index
    df_top = df[df['genreName'].isin(top_genres)]
    pivot_line = df_top.pivot(index='decade', columns='genreName', values='count').fillna(0)
    
    fig1, ax1 = plt.subplots(figsize=(14, 8))
    colors = plt.cm.Set2(np.linspace(0, 1, len(top_genres)))
    for i, genre in enumerate(top_genres):
        ax1.plot(pivot_line.index, pivot_line[genre], label=genre, marker='o', linewidth=2.5, color=colors[i])
    ax1.set_title('Top 5 Genre Popularity Over Decades', fontsize=16, fontweight='bold')
    ax1.set_xlabel('Decade')
    ax1.set_ylabel('Movie Count')
    ax1.grid(True, linestyle='--', alpha=0.7)
    ax1.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    save_and_upload_plot(fig1, "genre_trends_line.png")
    
    # Market share streamgraph
    pivot_all = df.pivot(index='decade', columns='genreName', values='count').fillna(0)
    pivot_perc = pivot_all.div(pivot_all.sum(axis=1), axis=0) * 100
    
    fig2, ax2 = plt.subplots(figsize=(16, 10))
    ax2.stackplot(pivot_perc.index, pivot_perc.T, labels=pivot_perc.columns, alpha=0.8)
    ax2.set_title('Genre Market Share Evolution', fontsize=16, fontweight='bold')
    ax2.set_xlabel('Decade')
    ax2.set_ylabel('Percentage (%)')
    ax2.legend(loc='upper left', bbox_to_anchor=(1, 1), fontsize=9)
    plt.tight_layout()
    save_and_upload_plot(fig2, "genre_market_share_streamgraph.png")

def create_collaboration_heatmap(df):
    df['collabCount'] = df['collabCount'].astype(int)
    df_sorted = df.sort_values('collabCount', ascending=True).tail(15)
    
    fig, ax = plt.subplots(figsize=(14, 10))
    y_pos = range(len(df_sorted))
    bars = ax.barh(y_pos, df_sorted['collabCount'], color='steelblue', alpha=0.7)
    
    labels = [f"{row['directorName']} & {row['writerName']}" for _, row in df_sorted.iterrows()]
    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels, fontsize=10)
    
    for i, bar in enumerate(bars):
        width = bar.get_width()
        ax.text(width + 0.1, bar.get_y() + bar.get_height()/2, 
                f'{int(width)}', va='center', ha='left', fontsize=9)
    
    ax.set_title('Top Director-Writer Collaborations', fontsize=16, fontweight='bold')
    ax.set_xlabel('Number of Collaborations')
    ax.grid(axis='x', linestyle='--', alpha=0.7)
    plt.tight_layout()
    save_and_upload_plot(fig, "director_writer_collaboration_chart.png")

def create_popularity_rating_plot(df):
    df['popularity'] = df['popularity'].astype(float)
    df['avgRating'] = df['avgRating'].astype(float)
    
    fig, ax = plt.subplots(figsize=(12, 8))
    scatter = ax.scatter(df['popularity'], df['avgRating'], alpha=0.6, 
                        edgecolor='w', s=80, c=df['popularity'], cmap='viridis')
    
    if len(df) > 1:
        slope, intercept, r_value, _, _ = linregress(df['popularity'], df['avgRating'])
        ax.plot(df['popularity'], intercept + slope * df['popularity'], 
                'r--', linewidth=2, label=f'R² = {r_value**2:.3f}')
    
    ax.set_title('Popularity vs Average Rating', fontsize=16, fontweight='bold')
    ax.set_xlabel('Popularity Score')
    ax.set_ylabel('Average Rating')
    ax.grid(True, linestyle='--', alpha=0.3)
    ax.legend()
    plt.colorbar(scatter, label='Popularity Score')
    plt.tight_layout()
    save_and_upload_plot(fig, "popularity_rating_correlation.png")

def create_career_longevity_plot(df):
    df['debut'] = df['debut'].astype(int)
    df['lastActive'] = df['lastActive'].astype(int)
    df['career_span'] = df['lastActive'] - df['debut']
    df = df.sort_values('career_span')
    
    fig, ax = plt.subplots(figsize=(14, 10))
    bars = ax.barh(df['name'], df['career_span'], color='steelblue', alpha=0.7)
    
    for bar in bars:
        width = bar.get_width()
        ax.text(width + 0.5, bar.get_y() + bar.get_height()/2, 
                f'{int(width)} years', va='center', ha='left', fontsize=9)
    
    ax.set_title('Top 20 Longest Careers in Film Industry', fontsize=16, fontweight='bold')
    ax.set_xlabel('Career Span (Years)')
    ax.set_ylabel('Person')
    ax.grid(axis='x', linestyle='--', alpha=0.7)
    plt.tight_layout()
    save_and_upload_plot(fig, "career_longevity_barchart.png")

def create_genre_rating_plot(df):
    df['avgRating'] = df['avgRating'].astype(float)
    df = df.sort_values('avgRating', ascending=False)
    
    fig, ax = plt.subplots(figsize=(14, 8))
    bars = ax.bar(df['genreName'], df['avgRating'], color='darkorange', alpha=0.7)
    
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2, height + 0.02, 
                f'{height:.2f}', ha='center', va='bottom', fontsize=9)
    
    ax.set_title('Average Rating by Genre', fontsize=16, fontweight='bold')
    ax.set_ylabel('Average Rating')
    ax.set_xlabel('Genre')
    plt.xticks(rotation=45, ha='right')
    ax.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    save_and_upload_plot(fig, "genre_rating_relationship.png")

def create_top_movies_plot(df):
    df['avgRating'] = df['avgRating'].astype(float)
    df['ratingCount'] = df['ratingCount'].astype(int)
    df_top = df.head(15).sort_values('avgRating')
    
    fig, ax = plt.subplots(figsize=(14, 10))
    colors = plt.cm.Set3(np.linspace(0, 1, len(df_top['genreName'].unique())))
    genre_colors = dict(zip(df_top['genreName'].unique(), colors))
    
    bars = ax.barh(range(len(df_top)), df_top['avgRating'], 
                   color=[genre_colors[genre] for genre in df_top['genreName']])
    
    labels = [f"{title[:30]}..." if len(title) > 30 else title for title in df_top['title']]
    ax.set_yticks(range(len(df_top)))
    ax.set_yticklabels(labels, fontsize=9)
    
    for i, (bar, rating_count) in enumerate(zip(bars, df_top['ratingCount'])):
        width = bar.get_width()
        ax.text(width + 0.02, bar.get_y() + bar.get_height()/2, 
                f'{width:.2f} ({rating_count})', va='center', ha='left', fontsize=8)
    
    ax.set_title('Top Rated Movies (with 10+ ratings)', fontsize=16, fontweight='bold')
    ax.set_xlabel('Average Rating (Rating Count)')
    ax.grid(axis='x', linestyle='--', alpha=0.7)
    plt.tight_layout()
    save_and_upload_plot(fig, "top_rated_movies.png")

def create_popularity_distribution_plot(df):
    df['popularity'] = df['popularity'].astype(float)
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
    
    # Box plot by profession
    professions = df['professionName'].unique()
    data_by_profession = [df[df['professionName'] == prof]['popularity'].values for prof in professions]
    
    box_plot = ax1.boxplot(data_by_profession, labels=professions, patch_artist=True)
    colors = plt.cm.Set2(np.linspace(0, 1, len(professions)))
    for patch, color in zip(box_plot['boxes'], colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.7)
    
    ax1.set_title('Popularity Distribution by Profession', fontsize=14, fontweight='bold')
    ax1.set_ylabel('Popularity Score')
    ax1.set_xlabel('Profession')
    ax1.tick_params(axis='x', rotation=45)
    ax1.grid(axis='y', linestyle='--', alpha=0.7)
    
    # Overall histogram
    ax2.hist(df['popularity'], bins=30, alpha=0.7, color='skyblue', edgecolor='black')
    ax2.set_title('Overall Popularity Distribution', fontsize=14, fontweight='bold')
    ax2.set_xlabel('Popularity Score')
    ax2.set_ylabel('Frequency')
    ax2.grid(axis='y', linestyle='--', alpha=0.7)
    
    plt.tight_layout()
    save_and_upload_plot(fig, "popularity_distribution.png")

if __name__ == "__main__":
    logger.info("Starting movie knowledge graph analysis pipeline")
    execute_analysis()
    logger.info("All analyses completed successfully")