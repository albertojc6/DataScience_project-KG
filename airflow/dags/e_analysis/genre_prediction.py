import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.neural_network import MLPClassifier
from sklearn.preprocessing import MultiLabelBinarizer

# 1. Prepare genres as multilabel
# If genres are stored as a string separated by '|', convert them to a list
movies['genres_list'] = movies['genres'].apply(lambda x: x.split('|') if isinstance(x, str) else [])

# 2. Binarize the genres
mlb = MultiLabelBinarizer()
Y = mlb.fit_transform(movies['genres_list'])

# 3. Build X with the embeddings
X = np.vstack(movies['embedding'].values)

# 4. Split and train
X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.2, random_state=42)
clf = MLPClassifier(hidden_layer_sizes=(64, 32), max_iter=100, random_state=42)
clf.fit(X_train, Y_train)

# 5. Multilabel prediction for a new movie
def predict_movie_genres(movie_id):
    movie_emb = movies.loc[movies['movieId'] == movie_id, 'embedding'].values[0].reshape(1, -1)
    pred = clf.predict(movie_emb)
    genres_pred = mlb.inverse_transform(pred)
    return genres_pred[0] if genres_pred else []

# Example usage:
print("Predicted genres:", predict_movie_genres('m456'))