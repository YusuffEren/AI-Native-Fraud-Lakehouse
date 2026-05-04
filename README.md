# AI-Native Fraud Lakehouse

Gerçek zamanlı finansal işlemleri Kafka üzerinden üretip, Spark ile işleyerek Iceberg formatında MinIO'ya yazan bir veri pipeline'ı. Veriler Trino ile SQL üzerinden sorgulanabilir.

Spark streaming aşamasında MLflow'a kayıtlı bir ML modeli (Random Forest + Isolation Forest) üzerinden gerçek zamanlı fraud tespiti yapılıyor. Model, her gelen işlem için risk skoru hesaplayıp anomali olup olmadığını belirliyor.

Kubernetes ortamı için yazıldı, lokal test için Docker Compose versiyonu da mevcut.

## Mimari

```
Kafka Producer → Kafka → Spark Streaming → Iceberg → MinIO
                                ↕
                         Model Serving (Flask)
                                ↕
                         MLflow (Model Registry)
```

## Kullanılan Teknolojiler

- **Apache Kafka** – mesaj kuyruğu, transaction verilerini taşır
- **Apache Spark (Structured Streaming)** – Kafka'dan okur, işler, yazar
- **Apache Iceberg** – tablo formatı (ACID, schema evolution, time-travel)
- **Project Nessie** – Iceberg katalog (commit/branch desteği var)
- **MinIO** – S3 uyumlu nesne depolama
- **Trino** – MinIO üzerindeki Iceberg tablolarını SQL ile sorgular
- **MLflow** – model eğitimi takibi ve model registry
- **scikit-learn** – Isolation Forest + Random Forest modelleri
- **Flask** – model serving REST API
- **Java 17 + Maven** – producer ve streaming uygulamaları
- **Docker Compose** – lokal ortam

## ML Pipeline

1. `ml-model/train.py` sentetik fraud verisi üretir, iki model eğitir:
   - **Random Forest** – supervised fraud classification
   - **Isolation Forest** – unsupervised anomaly detection
2. Eğitilen modeller MLflow'a kaydedilir (metrikler, parametreler, artifactlar)
3. `ml-model/serve.py` MLflow'dan en son modeli çekip `/predict` endpoint'i üzerinden sunar
4. Spark streaming her işlem için bu endpoint'e istek atıp birleşik risk skoru alır (RF %70 + IF %30 ağırlıklı)
5. Risk skoru ≥ 0.5 ise anomali olarak işaretlenir ve Iceberg tablosuna yazılır

Model servisi erişilemezse kural tabanlı fallback devreye girer.

## Çalıştırmak

```bash
docker-compose up -d --build
```

Sıra: MinIO → MLflow → model eğitimi → model serving → Kafka → producer → streaming

Durdurmak:

```bash
docker-compose down
```

## Servisler

| Servis | Adres |
|---|---|
| Kafka UI | http://localhost:8081 |
| MinIO | http://localhost:9001 (minioadmin / minioadmin) |
| Nessie | http://localhost:19120 |
| MLflow | http://localhost:5000 |
| Model Serving | http://localhost:5001 |
| Trino | http://localhost:8080 |

## Örnek Sorgular

DBeaver veya başka bir SQL aracıyla Trino'ya bağlan (host: `localhost`, port: `8080`, kullanıcı: `admin`):

```sql
-- En riskli işlemler
SELECT transaction_id, amount, ai_risk_score, ml_is_anomaly
FROM iceberg.nessie.db.fraud_transactions
WHERE ml_is_anomaly = true
ORDER BY ai_risk_score DESC
LIMIT 10;

-- Fraud tespit oranı
SELECT
    COUNT(*) as total,
    SUM(CASE WHEN ml_is_anomaly THEN 1 ELSE 0 END) as detected_anomalies,
    ROUND(AVG(ai_risk_score), 4) as avg_risk_score
FROM iceberg.nessie.db.fraud_transactions;
```

## Model Serving API

Doğrudan model serving'e istek atabilirsin:

```bash
curl -X POST http://localhost:5001/predict \
  -H "Content-Type: application/json" \
  -d '{"amount": 8500, "hour": 3, "day_of_week": 6, "tx_per_user_last_1h": 9, "avg_amount_last_24h": 150}'
```

## Kubernetes

Production ortamı için `deploy.sh` ve YAML dosyalarına (kafka-strimzi.yaml, infra.yaml, mlflow.yaml) bak.

## Proje Yapısı

```
├── docker-compose.yml          # Tüm servislerin orkestrasyonu
├── Dockerfile                  # Java uygulamaları için multi-stage build
├── pom.xml                     # Maven bağımlılıkları
├── src/main/java/com/lakehouse/
│   ├── FraudDataProducer.java  # Kafka'ya sentetik transaction gönderir
│   └── LakehouseStreaming.java # Spark streaming + ML inference
├── ml-model/
│   ├── Dockerfile              # Python ML image
│   ├── requirements.txt
│   ├── train.py                # Model eğitimi + MLflow kaydı
│   └── serve.py                # REST API ile model serving
├── trino/catalog/              # Trino Iceberg konfigürasyonu
└── *.yaml                      # Kubernetes manifest dosyaları
```
