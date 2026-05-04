# AI-Native Lakehouse & Gerçek Zamanlı Sahtekarlık Tespit (Fraud Detection) Mimarisi

Bu proje, modern veri mühendisliği araçlarını (Modern Data Stack) kullanarak tamamen yerel (local) bir ortamda gerçek zamanlı bir veri akışı (streaming) ve Lakehouse (Veri Gölü/Deposu) mimarisi kurmayı amaçlar.

Özellikle **Finansal Sahtekarlık (Fraud Detection)** verilerinin Kafka üzerinden canlı olarak akıtılması, Apache Spark ile işlenmesi ve Iceberg/Nessie kullanılarak S3 (MinIO) üzerine kaydedilmesi senaryosunu gerçekler. Ardından kaydedilen bu veriler Trino dağıtık SQL motoru ile anında sorgulanabilir hale gelir.

## 🏗 Mimari ve Kullanılan Teknolojiler

Bu projede sektör standartlarında, açık kaynaklı modern veri yığını araçları kullanılmıştır:

- **Apache Kafka:** Saniyede binlerce finansal işlemin (transaction) taşındığı mesaj kuyruğu sistemi.
- **Apache Spark (Streaming):** Kafka'dan gelen JSON formatındaki canlı verileri okuyup yapılandıran ve Lakehouse'a yazan işleme motoru.
- **Apache Iceberg:** Veri gölü üzerinde ACID işlemler yapabilmeyi, şema evrimini ve zaman yolculuğunu (time-travel) sağlayan açık tablo formatı.
- **Project Nessie:** Veri gölü için bir katalog sistemi. Veritabanı tablolarınız için "Git" benzeri (commit, branch, rollback) özellikler sunar (Data-as-Code).
- **MinIO:** Verilerin fiziksel olarak tutulduğu, AWS S3 uyumlu obje depolama (Object Storage) çözümü.
- **Trino:** S3 üzerinde duran Iceberg tablolarını saniyeler içinde devasa hızda sorgulamanızı sağlayan dağıtık SQL analiz motoru.
- **Java 17 & Maven:** Veri üreten (Producer) ve veriyi işleyen (Streaming) uygulamaların dili.
- **Docker & Docker Compose:** Tüm sistemin tek tıkla çalışmasını sağlayan konteyner mimarisi.

## 🧠 Neden "AI-Native"? (MLOps & Gerçek Zamanlı Çıkarım)

Bu proje sadece veriyi taşıyıp depolamakla kalmaz; aynı zamanda Apache Spark ortamında **gerçek zamanlı yapay zeka çıkarımını (Real-time Model Inference)** simüle eder.

* `LakehouseStreaming.java` içerisinde, gelen finansal akışa anında bir Makine Öğrenmesi (ML) modeli uygulanır.
* Spark'ın **User Defined Functions (UDF)** mekanizması kullanılarak akan her işlem için bir `ai_risk_score` hesaplanır ve anomali tespiti (`ml_is_anomaly`) yapılarak Iceberg/Lakehouse'a kaydedilir.
* *Not:* Mimari, normalde **MLflow** ile entegre çalışacak şekilde tasarlanmıştır (Geliştirme aşamasında MLflow Model Registry'den model yüklemek üzere yapılandırılmıştır).

## 🚀 Kurulum ve Orkestrasyon (Kubernetes vs Docker Compose)

**ÖNEMLİ:** Modern veri yığınlarının (Modern Data Stack) temeli **bağımsız ölçeklenebilirliktir (Independent Scalability).** Bu projenin orijinal ve production'a (canlıya) en yakın kurulum yöntemi **Kubernetes (Minikube)** üzerinden yapılmalıdır. 
Proje içerisindeki `deploy.sh` ve `.yaml` (örn: `kafka-strimzi.yaml`, `infra.yaml`) dosyaları bu amaçla yazılmıştır.

Ancak lokal ortamda (özellikle Windows'ta) **hızlı test ve gösterim** yapabilmek için tüm Kubernetes mimarisi bir **`docker-compose.yml`** dosyasına indirgenmiştir.

### Projeyi Lokal (Test) Ortamında Başlatmak
Terminal (veya PowerShell) üzerinden proje dizinine gidin ve aşağıdaki komutu çalıştırın:

```bash
docker-compose up -d --build
```

Bu komut sistemi ayağa kaldırır ve Java uygulamalarınızı derler. Sistemi durdurmak için: `docker-compose down`

## 📊 Görsel Arayüzler (Dashboards)

Proje makalelerde veya sunumlarda göstermek için muazzam görsel arayüzler sunar:

| Arayüz / Uygulama | Adres | Açıklama |
| :--- | :--- | :--- |
| **Kafka UI** | [http://localhost:8081](http://localhost:8081) | Canlı sahtekarlık (fraud) verilerini, Topic'leri ve mesajları izleyin. |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | S3 arayüzü. Iceberg'ün yazdığı `.parquet` dosyalarını inceleyin. (Kullanıcı: `minioadmin`, Şifre: `minioadmin`) |
| **Nessie UI/API** | [http://localhost:19120](http://localhost:19120) | Veri kataloğunuzun dallarını (branches) ve commit geçmişini (Git mantığı) inceleyin. |
| **Trino SQL Engine** | [http://localhost:8080](http://localhost:8080) | Gelen verileri anında sorgulamak için çalışan SQL analiz motorunun arayüzü. |

## 💻 Trino ile Yapay Zeka Sonuçlarını Sorgulama (DBeaver)

Veri MinIO üzerine akmaya başladıktan sonra herhangi bir SQL IDE'si (örneğin [DBeaver](https://dbeaver.io/)) üzerinden Trino'ya bağlanabilirsiniz.
*(Host: `localhost`, Port: `8080`, Kullanıcı: `admin`)*

**Yapay Zeka Risk Skorlarını Sorgulama Örneği:**
```sql
SELECT 
    transaction_id, 
    amount, 
    ai_risk_score, 
    ml_is_anomaly 
FROM iceberg.nessie.transactions 
WHERE ml_is_anomaly = true 
ORDER BY ai_risk_score DESC 
LIMIT 10;
```
Bu sorgu ile makinenin yakaladığı en riskli anomalileri anında görebilirsiniz!

## 📝 Uygulama Bileşenleri (Java)
* **FraudDataProducer.java:** Sahte finansal verileri JSON formatında Kafka'ya fırlatır.
* **LakehouseStreaming.java:** Kafka'dan veriyi çeker, anında **AI Model Çıkarımını (Inference)** uygular ve sonuçları Iceberg formatında S3'e yazar.

---
*Bu proje, Modern Data Stack, MLOps ve Lakehouse mimarilerini yerel Kubernetes/Docker ortamında uçtan uca kanıtlamak için hazırlanmıştır.*
