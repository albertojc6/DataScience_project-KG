from sklearn.cluster import KMeans
import torch
import pandas as pd

model_path = 'C:/Users/marta/trained_model.pkl'
entity_to_id_path = 'C:\Users/marta/entity_to_id.tsv.zip'

model = torch.load(model_path, map_location=torch.device('cpu'))
print("Model loaded")

# Load entity_to_id mapping
entity_to_id_df = pd.read_csv(entity_to_id_path, sep='\t', header=None, names=['id', 'entity'])
# Filter only rows where 'id' is a number
entity_to_id_df = entity_to_id_df[entity_to_id_df['id'].apply(lambda x: str(x).isdigit())]
entity_to_id = {row['entity']: int(row['id']) for _, row in entity_to_id_df.iterrows()}
id_to_entity = {int(row['id']): row['entity'] for _, row in entity_to_id_df.iterrows()}


# Filter only movie entities
movie_ids = [i for i, name in id_to_entity.items() if '/Movie/' in str(name)]
movie_names = [id_to_entity[i] for i in movie_ids]

# Get embeddings
embedding_module = model.entity_representations[0]
all_embeddings = embedding_module(indices=None).detach().cpu().numpy()
movie_embeddings = all_embeddings[movie_ids]

# Clustering
kmeans = KMeans(n_clusters=5, random_state=42)
labels = kmeans.fit_predict(movie_embeddings)

# Save movie_entity and cluster in a single CSV
result_df = pd.DataFrame({
    'movie_entity': movie_names,
    'cluster': labels
})
result_df.to_csv('C:/Users/marta/movie_clusters.csv', index=False)
print('File movie_clusters.csv saved in C:/Users/marta/')

# Print only the first 20 results to console
for name, label in list(zip(movie_names, labels))[:20]:
    print(f"{name}: Cluster {label}")