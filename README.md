# AI-Native Fraud Lakehouse

Kafka'dan akan finansal işlemleri Spark ile okuyup, üstüne bir ML modeli çalıştırıp, sonuçları Iceberg formatında MinIO'ya yazan bir pipeline. Sonra da Trino ile SQL atıp bakıyorsun.

## Ne var burada?

Proje kabaca şu akışta çalışıyor:

```
Producer → Kafka → Spark Streaming → Iceberg/MinIO
                        ↕
                   Model Serving
                        ↕
                      MLflow
```

- **Producer** sentetik finansal işlem verisi üretip Kafka'ya gönderiyor
- **Spark Streaming** bu veriyi okuyor, her işlem için ML modeline soruyor "bu fraud mı?"
- Model **risk skoru** ve **anomali** bilgisini ekliyor, sonuç Iceberg tablosuna yazılıyor
- **Trino** ile bu tablolara SQL sorgusu atabiliyorsun

## ML tarafı

`ml-model/train.py` sentetik veri üretip iki model eğitiyor:
- **Random Forest** — supervised, fraud/normal sınıflandırması
- **Isolation Forest** — unsupervised, anomali tespiti

Modeller MLflow'a kaydediliyor. `ml-model/serve.py` de MLflow'dan en son modeli çekip bir Flask API olarak ayağa kaldırıyor. Spark her işlem için bu API'ye istek atıp iki modelin birleşik skorunu alıyor.

Model servisi ayakta değilse basit bir kural tabanlı fallback devreye giriyor, pipeline durmaya devam etmiyor.

## Teknolojiler

| Ne | Ne için |
|---|---|
| Kafka | Mesaj kuyruğu |
| Spark Streaming | Veri işleme |
| Iceberg | Tablo formatı |
| Nessie | Iceberg kataloğu |
| MinIO | S3 uyumlu depolama |
| Trino | SQL sorgu motoru |
| MLflow | Model registry |
| scikit-learn | Model eğitimi |
| Flask | Model serving API |
| Java 17 + Maven | Producer ve streaming |
| Docker Compose | Lokal orkestrasyon |

## Nasıl çalıştırılır

```bash
docker-compose up -d --build
```

Her şey sırayla ayağa kalkıyor: MinIO → MLflow → model eğitimi → model serving → Kafka → producer → streaming.

Durdurmak için:
```bash
docker-compose down
```

## Arayüzler

| Servis | Adres |
|---|---|
| Kafka UI | http://localhost:8081 |
| MinIO | http://localhost:9001 (minioadmin / minioadmin) |
| Nessie | http://localhost:19120 |
| MLflow | http://localhost:5000 |
| Model Serving | http://localhost:5001 |
| Trino | http://localhost:8080 |

## Örnek sorgu

Trino'ya bağlan (host: `localhost`, port: `8080`, kullanıcı: `admin`):

```sql
SELECT transaction_id, amount, ai_risk_score, ml_is_anomaly
FROM iceberg.nessie.db.fraud_transactions
WHERE ml_is_anomaly = true
ORDER BY ai_risk_score DESC
LIMIT 10;
```

## Model API'si

Doğrudan model serving'e de istek atabilirsin:

```bash
curl -X POST http://localhost:5001/predict \
  -H "Content-Type: application/json" \
  -d '{"amount": 8500, "hour": 3, "day_of_week": 6, "tx_per_user_last_1h": 9, "avg_amount_last_24h": 150}'
```

## Proje yapısı

```
├── docker-compose.yml
├── Dockerfile
├── pom.xml
├── src/main/java/com/lakehouse/
│   ├── FraudDataProducer.java     # Kafka'ya transaction gönderir
│   └── LakehouseStreaming.java    # Spark streaming + ML inference
├── ml-model/
│   ├── train.py                   # Model eğitimi + MLflow kaydı
│   └── serve.py                   # REST API ile model serving
├── trino/catalog/
└── *.yaml                         # Kubernetes manifestları
```

## Kubernetes

Lokal değil de Kubernetes üzerinde çalıştırmak istersen `deploy.sh` ve YAML dosyalarına bak.
