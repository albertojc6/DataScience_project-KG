import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.neural_network import MLPClassifier
from sklearn.ensemble import RandomForestClassifier
from xgboost import XGBClassifier

# 1. Merge embeddings with ratings
def get_embedding(row, emb_df, key):
    return emb_df.loc[emb_df[key] == row[key], 'embedding'].values[0]

# Assume embeddings are in the 'embedding' column as np.array
ratings['user_emb'] = ratings.apply(lambda row: get_embedding(row, user_embeddings, 'userId'), axis=1)
ratings['movie_emb'] = ratings.apply(lambda row: get_embedding(row, movie_embeddings, 'movieId'), axis=1)

# 2. Build X and y
X = np.vstack([np.concatenate([u, m]) for u, m in zip(ratings['user_emb'], ratings['movie_emb'])])
# For example, binarize: rating >= 7 is "high"
y = (ratings['rating'] >= 7).astype(int)

# 3. Split data
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# 4. Train and evaluate different classifiers
models = {
    "MLPClassifier": MLPClassifier(hidden_layer_sizes=(64, 32), max_iter=100, random_state=42),
    "RandomForest": RandomForestClassifier(n_estimators=100, random_state=42),
    "XGBoost": XGBClassifier(use_label_encoder=False, eval_metric='logloss', random_state=42)
}

for name, clf in models.items():
    clf.fit(X_train, y_train)
    score = clf.score(X_test, y_test)
    print(f"{name} accuracy: {score:.4f}")

# 5. Prediction function using the last trained model (change as needed)
def predict_user_movie(user_id, movie_id, clf=models["MLPClassifier"]):
    user_emb = user_embeddings.loc[user_embeddings['userId'] == user_id, 'embedding'].values[0]
    movie_emb = movie_embeddings.loc[movie_embeddings['movieId'] == movie_id, 'embedding'].values[0]
    x = np.concatenate([user_emb, movie_emb]).reshape(1, -1)
    proba = clf.predict_proba(x)[0, 1]
    return proba

# Example usage:
print("Probability of high rating (MLP):", predict_user_movie('u123', 'm456', clf=models["MLPClassifier"]))
print("Probability of high rating (RandomForest):", predict_user_movie('u123', 'm456', clf=models["RandomForest"]))
print("Probability of high rating (XGBoost):", predict_user_movie('u123', 'm456', clf=models["XGBoost"]))