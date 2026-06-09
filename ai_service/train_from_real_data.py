"""
Train the Isolation Forest on real e-commerce events from the REES46 dataset.

Reads the Kaggle CSV (2019-Nov.csv) in chunks, samples ~500K events, fits the
model, and saves it to ai_service/model/. The ai-service loads this at startup.

Run once from the host (not inside Docker):
    python ai_service/train_from_real_data.py
"""
import os
import sys
import numpy as np
import pandas as pd
import joblib
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import LabelEncoder

DATA_PATH = "data/2019-Nov.csv"
MODEL_OUT = "ai_service/model/isolation_forest_real.joblib"
SAMPLE_SIZE = 500_000

if not os.path.exists(DATA_PATH):
    sys.exit(f"missing: {DATA_PATH}\nDownload from Kaggle (REES46 dataset) first.")

os.makedirs("ai_service/model", exist_ok=True)
print(f"loading up to {SAMPLE_SIZE:,} events from {DATA_PATH}...")

chunks, total = [], 0
for chunk in pd.read_csv(DATA_PATH, chunksize=100_000,
                         usecols=["event_type","price","user_session",
                                  "event_time","brand","category_code"]):
    chunks.append(chunk); total += len(chunk)
    if total >= SAMPLE_SIZE: break

df = pd.concat(chunks).head(SAMPLE_SIZE)
df = df.dropna(subset=["event_type","price","user_session"])
df = df[df["price"] > 0]
df["brand"] = df["brand"].fillna("unknown")
df["category_code"] = df["category_code"].fillna("unknown")
df["hour"] = pd.to_datetime(df["event_time"]).dt.hour
df = df.sort_values(["user_session","event_time"])
df["session_pos"] = df.groupby("user_session").cumcount() + 1
lens = df.groupby("user_session").size()
valid = lens[(lens >= 2) & (lens <= 100)].index
df = df[df["user_session"].isin(valid)]
print(f"after cleaning: {len(df):,} rows from {df['user_session'].nunique():,} sessions")

TOP_BRANDS = df["brand"].value_counts().head(50).index.tolist()
TOP_CATS = df["category_code"].value_counts().head(50).index.tolist()
df["brand"] = df["brand"].where(df["brand"].isin(TOP_BRANDS), "other")
df["category_code"] = df["category_code"].where(df["category_code"].isin(TOP_CATS), "other")

encoders = {
    "event_type": LabelEncoder().fit(df["event_type"]),
    "brand": LabelEncoder().fit(df["brand"]),
    "category": LabelEncoder().fit(df["category_code"]),
}
X = np.column_stack([
    encoders["event_type"].transform(df["event_type"]),
    encoders["brand"].transform(df["brand"]),
    encoders["category"].transform(df["category_code"]),
    df["price"].clip(0, 5000).values,
    df["hour"].values,
    df["session_pos"].clip(1, 100).values,
]).astype(float)

print(f"training Isolation Forest on {X.shape[0]:,} events...")
model = IsolationForest(n_estimators=200, contamination=0.02,
                        max_samples=256, random_state=42, n_jobs=-1).fit(X)
joblib.dump({"model": model, "encoders": encoders, "top_brands": TOP_BRANDS,
             "top_categories": TOP_CATS,
             "feature_names": ["event_type","brand","category","price","hour","session_pos"],
             "training_size": len(X), "source": "REES46 e-commerce dataset (2019-Nov)"}, MODEL_OUT)
print(f"saved -> {MODEL_OUT}")
