# AI-Native Fraud Lakehouse

Gerçek zamanlı finansal işlemleri Kafka üzerinden üretip, Spark ile işleyerek Iceberg formatında MinIO'ya yazan bir veri pipeline'ı. Veriler Trino ile SQL üzerinden sorgulanabilir hale geliyor.

Kubernetes ortamı için yazıldı, lokal test için Docker Compose versiyonu da mevcut.

## Kullanılan Teknolojiler

- **Apache Kafka** – mesaj kuyruğu, transaction verilerini taşır
- **Apache Spark (Structured Streaming)** – Kafka'dan okur, işler, yazar
- **Apache Iceberg** – tablo formatı (ACID, schema evolution, time-travel)
- **Project Nessie** – Iceberg katalog (commit/branch desteği var)
- **MinIO** – S3 uyumlu nesne depolama
- **Trino** – MinIO üzerindeki Iceberg tablolarını SQL ile sorgular
- **MLflow** – model registry (entegrasyon yapılandırıldı ama henüz aktif değil)
- **Java 17 + Maven** – producer ve streaming uygulamaları
- **Docker Compose** – lokal ortam için

## Nasıl Çalışır

`FraudDataProducer.java` rastgele finansal işlem verisi üretip Kafka'ya gönderir.

`LakehouseStreaming.java` bu veriyi Kafka'dan okur, her işlem için bir risk skoru hesaplar (Spark UDF), anomali tespiti yapar ve sonuçları Iceberg tablosuna yazar.

Risk skoru ve anomali tespiti şu an basit bir simülasyon. MLflow entegrasyonu için yapı hazır.

## Çalıştırmak

```bash
docker-compose up -d --build
```

Durdurmak için:

```bash
docker-compose down
```

## Servisler

| Servis | Adres |
|---|---|
| Kafka UI | http://localhost:8081 |
| MinIO | http://localhost:9001 (minioadmin / minioadmin) |
| Nessie | http://localhost:19120 |
| Trino | http://localhost:8080 |

## Örnek Sorgu

DBeaver veya başka bir SQL aracıyla Trino'ya bağlanıp (host: `localhost`, port: `8080`, kullanıcı: `admin`) çalıştırabilirsin:

```sql
SELECT transaction_id, amount, ai_risk_score, ml_is_anomaly
FROM iceberg.nessie.transactions
WHERE ml_is_anomaly = true
ORDER BY ai_risk_score DESC
LIMIT 10;
```

## Kubernetes

Lokal değil production benzeri bir ortam için `deploy.sh` ve YAML dosyalarına (kafka-strimzi.yaml, infra.yaml vb.) bakabilirsin.
