from pykeen.pipeline import pipeline

# 1. Train your model (example with TransH)
result = pipeline(
    model='TransH',
    dataset='PathDataset',
    dataset_kwargs=dict(
        path='triplets.tsv',  # your triplets file: head \t relation \t tail
        delimiter='\t',
    ),
    training_kwargs=dict(num_epochs=100, batch_size=256),
    model_kwargs=dict(embedding_dim=100),
)

user_id = 'User_123'
relation = 'rates_movie'

# Load triplets to find all movies and user interactions
triplets = pd.read_csv('triplets.tsv', sep='\t', header=None, names=['head', 'relation', 'tail'])

# Get all movies (entities that start with 'Movie_')
all_movies = triplets[triplets['tail'].str.startswith('Movie_')]['tail'].unique().tolist()

# Movies the user has already rated
user_seen_movies = triplets.loc[
    (triplets['head'] == user_id) & (triplets['relation'] == relation), 'tail'
].tolist()

# Movies the user has NOT seen
candidate_movies = [m for m in all_movies if m not in user_seen_movies]

# Build triples to score: (user, rates_movie, candidate_movie)
triples_to_score = [(user_id, relation, movie) for movie in candidate_movies]

# Get scores from the model
import torch
scores = result.model.score_hrt(triples_to_score)
scores = scores.detach().cpu().numpy() if isinstance(scores, torch.Tensor) else scores

import pandas as pd
# Get top-N recommendations
top_n = 5
top_indices = scores.argsort()[-top_n:][::-1]
recommended_movies = [candidate_movies[i] for i in top_indices]

print(f"Recommended movies for {user_id}: {recommended_movies}")
