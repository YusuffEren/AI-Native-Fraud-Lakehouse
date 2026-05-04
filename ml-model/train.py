"""
Fraud detection modeli egitimi ve MLflow'a kaydi.

Sentetik finansal islem verisi olusturur, Isolation Forest ile
anomali tespiti modeli egitir ve MLflow'a kaydeder.
"""

import numpy as np
import pandas as pd
import mlflow
import mlflow.sklearn
from sklearn.ensemble import IsolationForest, RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, f1_score
from sklearn.preprocessing import StandardScaler
import os
import time
import pickle

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MODEL_NAME = "fraud-detector"


def generate_training_data(n_normal=10000, n_fraud=500):
    """Sentetik egitim verisi uret."""
    np.random.seed(42)

    # Normal islemler
    normal = pd.DataFrame({
        "amount": np.random.lognormal(mean=3.5, sigma=1.0, size=n_normal),
        "hour": np.random.randint(6, 23, size=n_normal),
        "day_of_week": np.random.randint(0, 7, size=n_normal),
        "tx_per_user_last_1h": np.random.poisson(lam=2, size=n_normal),
        "avg_amount_last_24h": np.random.lognormal(mean=3.5, sigma=0.8, size=n_normal),
        "is_fraud": 0
    })

    # Fraud islemler: yuksek tutar, gece saatleri, yuksek frekans
    fraud = pd.DataFrame({
        "amount": np.random.lognormal(mean=6.5, sigma=1.5, size=n_fraud),
        "hour": np.random.choice([0, 1, 2, 3, 4, 5, 23], size=n_fraud),
        "day_of_week": np.random.randint(0, 7, size=n_fraud),
        "tx_per_user_last_1h": np.random.poisson(lam=8, size=n_fraud),
        "avg_amount_last_24h": np.random.lognormal(mean=3.0, sigma=0.5, size=n_fraud),
        "is_fraud": 1
    })

    data = pd.concat([normal, fraud], ignore_index=True)
    data = data.sample(frac=1, random_state=42).reset_index(drop=True)

    # Turetilmis ozellikler
    data["amount_to_avg_ratio"] = data["amount"] / (data["avg_amount_last_24h"] + 1)
    data["is_night"] = (data["hour"].isin([0, 1, 2, 3, 4, 5, 23])).astype(int)
    data["high_frequency"] = (data["tx_per_user_last_1h"] > 5).astype(int)

    return data


def train_and_register():
    """Modeli egit, MLflow'a kaydet."""
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

    print(f"MLflow tracking URI: {MLFLOW_TRACKING_URI}")
    print("Egitim verisi olusturuluyor...")

    data = generate_training_data()

    feature_cols = [
        "amount", "hour", "day_of_week", "tx_per_user_last_1h",
        "avg_amount_last_24h", "amount_to_avg_ratio", "is_night", "high_frequency"
    ]
    X = data[feature_cols]
    y = data["is_fraud"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    # --- Model 1: Isolation Forest (unsupervised anomaly detection) ---
    print("Isolation Forest egitiliyor...")
    iso_forest = IsolationForest(
        n_estimators=200,
        contamination=0.05,
        max_samples="auto",
        random_state=42
    )
    iso_forest.fit(X_train_scaled)

    iso_scores = iso_forest.score_samples(X_test_scaled)
    # Skoru 0-1 arasina cevir (dusuk skor = daha anomali)
    iso_scores_normalized = 1 - (iso_scores - iso_scores.min()) / (iso_scores.max() - iso_scores.min())

    # --- Model 2: Random Forest (supervised classification) ---
    print("Random Forest egitiliyor...")
    rf_model = RandomForestClassifier(
        n_estimators=200,
        max_depth=10,
        class_weight="balanced",
        random_state=42
    )
    rf_model.fit(X_train_scaled, y_train)

    rf_preds = rf_model.predict(X_test_scaled)
    rf_proba = rf_model.predict_proba(X_test_scaled)[:, 1]
    f1 = f1_score(y_test, rf_preds)

    print("\n--- Random Forest Sonuclari ---")
    print(classification_report(y_test, rf_preds))

    # MLflow'a kaydet
    mlflow.set_experiment("fraud-detection")

    with mlflow.start_run(run_name="fraud-detector-v1") as run:
        # Parametreler
        mlflow.log_param("rf_n_estimators", 200)
        mlflow.log_param("rf_max_depth", 10)
        mlflow.log_param("iso_n_estimators", 200)
        mlflow.log_param("iso_contamination", 0.05)
        mlflow.log_param("n_features", len(feature_cols))
        mlflow.log_param("features", ",".join(feature_cols))

        # Metrikler
        mlflow.log_metric("f1_score", f1)
        mlflow.log_metric("train_size", len(X_train))
        mlflow.log_metric("test_size", len(X_test))
        mlflow.log_metric("fraud_ratio", y.mean())

        # Modelleri kaydet
        mlflow.sklearn.log_model(rf_model, "random_forest")
        mlflow.sklearn.log_model(iso_forest, "isolation_forest")

        # Scaler'i da kaydet
        scaler_path = "/tmp/scaler.pkl"
        with open(scaler_path, "wb") as f:
            pickle.dump(scaler, f)
        mlflow.log_artifact(scaler_path, "scaler")

        # Feature listesini kaydet
        feature_path = "/tmp/features.txt"
        with open(feature_path, "w") as f:
            f.write("\n".join(feature_cols))
        mlflow.log_artifact(feature_path, "metadata")

        run_id = run.info.run_id
        print(f"\nMLflow Run ID: {run_id}")
        print(f"F1 Score: {f1:.4f}")

    # Modeli MLflow Model Registry'ye kaydet
    model_uri = f"runs:/{run_id}/random_forest"
    result = mlflow.register_model(model_uri, MODEL_NAME)
    print(f"\nModel registered: {MODEL_NAME} version {result.version}")

    return run_id


if __name__ == "__main__":
    # MLflow'un ayaga kalkmasi icin bekle
    print("MLflow'un hazir olmasini bekleniyor...")
    for attempt in range(30):
        try:
            mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
            mlflow.search_experiments()
            print("MLflow hazir!")
            break
        except Exception:
            print(f"  Bekleniyor... ({attempt + 1}/30)")
            time.sleep(5)
    else:
        print("MLflow'a baglanilamadi, yine de deneniyor...")

    train_and_register()
