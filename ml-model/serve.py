"""
MLflow'dan modeli yukleyip REST API olarak sunan servis.

Spark streaming uygulamasi bu endpoint'e istek atarak
gercek zamanli fraud tahmini alir.
"""

import os
import pickle
import time
import logging

import mlflow
import mlflow.sklearn
import numpy as np
from flask import Flask, request, jsonify

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MODEL_NAME = os.getenv("MODEL_NAME", "fraud-detector")

# Global model ve scaler
rf_model = None
iso_model = None
scaler = None


def load_models():
    """MLflow'dan en son modeli yukle."""
    global rf_model, iso_model, scaler

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

    # En son registered modeli al
    try:
        client = mlflow.tracking.MlflowClient()
        versions = client.search_model_versions(f"name='{MODEL_NAME}'")

        if not versions:
            logger.warning("Kayitli model bulunamadi, egitim bekleniyor...")
            return False

        latest = sorted(versions, key=lambda v: int(v.version), reverse=True)[0]
        run_id = latest.run_id
        logger.info(f"Model yukleniyor: {MODEL_NAME} v{latest.version} (run: {run_id})")

        # Random Forest
        rf_model = mlflow.sklearn.load_model(f"runs:/{run_id}/random_forest")
        logger.info("Random Forest yuklendi")

        # Isolation Forest
        iso_model = mlflow.sklearn.load_model(f"runs:/{run_id}/isolation_forest")
        logger.info("Isolation Forest yuklendi")

        # Scaler
        scaler_path = mlflow.artifacts.download_artifacts(
            run_id=run_id, artifact_path="scaler/scaler.pkl"
        )
        with open(scaler_path, "rb") as f:
            scaler = pickle.load(f)
        logger.info("Scaler yuklendi")

        return True

    except Exception as e:
        logger.error(f"Model yukleme hatasi: {e}")
        return False


@app.route("/health", methods=["GET"])
def health():
    """Servis sagligi kontrolu."""
    return jsonify({
        "status": "healthy" if rf_model is not None else "waiting",
        "model_loaded": rf_model is not None
    })


@app.route("/predict", methods=["POST"])
def predict():
    """
    Tek bir islem icin fraud tahmini yap.

    Beklenen JSON:
    {
        "amount": 1500.0,
        "hour": 3,
        "day_of_week": 5,
        "tx_per_user_last_1h": 7,
        "avg_amount_last_24h": 200.0
    }
    """
    if rf_model is None:
        return jsonify({"error": "Model henuz yuklenmedi"}), 503

    data = request.get_json()
    if not data:
        return jsonify({"error": "JSON body gerekli"}), 400

    try:
        amount = float(data.get("amount", 0))
        hour = int(data.get("hour", 12))
        day_of_week = int(data.get("day_of_week", 0))
        tx_per_user_last_1h = int(data.get("tx_per_user_last_1h", 1))
        avg_amount_last_24h = float(data.get("avg_amount_last_24h", amount))

        # Turetilmis ozellikler
        amount_to_avg_ratio = amount / (avg_amount_last_24h + 1)
        is_night = 1 if hour in [0, 1, 2, 3, 4, 5, 23] else 0
        high_frequency = 1 if tx_per_user_last_1h > 5 else 0

        features = np.array([[
            amount, hour, day_of_week, tx_per_user_last_1h,
            avg_amount_last_24h, amount_to_avg_ratio, is_night, high_frequency
        ]])

        features_scaled = scaler.transform(features)

        # Random Forest: fraud olasiligi
        rf_proba = rf_model.predict_proba(features_scaled)[0][1]

        # Isolation Forest: anomali skoru
        iso_raw = iso_model.score_samples(features_scaled)[0]
        # Normalize: dusuk skor = daha anomali, 0-1 arasina cevir
        iso_score = max(0.0, min(1.0, -iso_raw))

        # Birlesik risk skoru: RF ve IF'in agirlkli ortalamasi
        risk_score = round(0.7 * rf_proba + 0.3 * iso_score, 4)
        is_anomaly = risk_score >= 0.5

        return jsonify({
            "risk_score": risk_score,
            "is_anomaly": is_anomaly,
            "rf_fraud_probability": round(rf_proba, 4),
            "isolation_anomaly_score": round(iso_score, 4)
        })

    except Exception as e:
        logger.error(f"Tahmin hatasi: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route("/predict/batch", methods=["POST"])
def predict_batch():
    """
    Toplu tahmin endpoint'i.

    Beklenen JSON:
    {
        "instances": [
            {"amount": 100, "hour": 14, ...},
            {"amount": 9000, "hour": 2, ...}
        ]
    }
    """
    if rf_model is None:
        return jsonify({"error": "Model henuz yuklenmedi"}), 503

    data = request.get_json()
    instances = data.get("instances", [])

    if not instances:
        return jsonify({"error": "instances listesi gerekli"}), 400

    results = []
    for inst in instances:
        try:
            amount = float(inst.get("amount", 0))
            hour = int(inst.get("hour", 12))
            day_of_week = int(inst.get("day_of_week", 0))
            tx_per_user_last_1h = int(inst.get("tx_per_user_last_1h", 1))
            avg_amount_last_24h = float(inst.get("avg_amount_last_24h", amount))

            amount_to_avg_ratio = amount / (avg_amount_last_24h + 1)
            is_night = 1 if hour in [0, 1, 2, 3, 4, 5, 23] else 0
            high_frequency = 1 if tx_per_user_last_1h > 5 else 0

            features = np.array([[
                amount, hour, day_of_week, tx_per_user_last_1h,
                avg_amount_last_24h, amount_to_avg_ratio, is_night, high_frequency
            ]])

            features_scaled = scaler.transform(features)
            rf_proba = rf_model.predict_proba(features_scaled)[0][1]
            iso_raw = iso_model.score_samples(features_scaled)[0]
            iso_score = max(0.0, min(1.0, -iso_raw))
            risk_score = round(0.7 * rf_proba + 0.3 * iso_score, 4)

            results.append({
                "risk_score": risk_score,
                "is_anomaly": risk_score >= 0.5
            })
        except Exception as e:
            results.append({"risk_score": 0.0, "is_anomaly": False, "error": str(e)})

    return jsonify({"predictions": results})


if __name__ == "__main__":
    # Model yuklenmesini bekle
    logger.info("Model yukleniyor...")
    for attempt in range(60):
        if load_models():
            break
        logger.info(f"Model bekleniyor... ({attempt + 1}/60)")
        time.sleep(10)

    if rf_model is None:
        logger.warning("Model yuklenemedi, servis model olmadan baslatiliyor")

    app.run(host="0.0.0.0", port=5001, debug=False)
