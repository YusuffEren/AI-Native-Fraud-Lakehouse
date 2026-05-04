package com.lakehouse;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.OutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.LocalDateTime;

public class LakehouseStreaming {
    private static final String MODEL_SERVING_URL = System.getenv()
            .getOrDefault("MODEL_SERVING_URL", "http://model-serving:5001/predict");

    public static void main(String[] args) throws Exception {
        String kafkaBootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "my-cluster-kafka-bootstrap:9092");
        String nessieUri = System.getenv().getOrDefault("NESSIE_URI", "http://nessie:19120/api/v2");
        String minioEndpoint = System.getenv().getOrDefault("MINIO_ENDPOINT", "http://minio:9000");

        SparkSession spark = SparkSession.builder()
                .appName("FraudLakehouseStreaming")
                .master("local[*]")
                // Iceberg and Nessie configuration
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.nessie.type", "nessie")
                .config("spark.sql.catalog.nessie.uri", nessieUri)
                .config("spark.sql.catalog.nessie.ref", "main")
                .config("spark.sql.catalog.nessie.warehouse", "s3a://lakehouse/")
                .config("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                // AWS S3 / MinIO Configuration for Iceberg S3FileIO
                .config("spark.sql.catalog.nessie.s3.endpoint", minioEndpoint)
                .config("spark.sql.catalog.nessie.s3.path-style-access", "true")
                .config("spark.sql.catalog.nessie.s3.access-key-id", "minioadmin")
                .config("spark.sql.catalog.nessie.s3.secret-access-key", "minioadmin")
                // Hadoop AWS Configuration for Spark Structured Streaming Checkpoints
                .config("spark.hadoop.fs.s3a.endpoint", minioEndpoint)
                .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
                .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .getOrCreate();

        // Streaming tablo olusturulmadan once veritabani ve tablonun hazir olmasini garantile
        spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.db");
        spark.sql("CREATE TABLE IF NOT EXISTS nessie.db.fraud_transactions (" +
                "transaction_id STRING, " +
                "user_id STRING, " +
                "amount DOUBLE, " +
                "timestamp LONG, " +
                "is_fraud BOOLEAN, " +
                "ai_risk_score DOUBLE, " +
                "ml_is_anomaly BOOLEAN) " +
                "USING iceberg");

        StructType schema = new StructType()
                .add("transaction_id", DataTypes.StringType)
                .add("user_id", DataTypes.StringType)
                .add("amount", DataTypes.DoubleType)
                .add("timestamp", DataTypes.LongType)
                .add("is_fraud", DataTypes.BooleanType);

        Dataset<Row> df = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaBootstrap)
                .option("subscribe", "fraud-transactions")
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", "false")
                .load()
                .selectExpr("CAST(value AS STRING) as json")
                .select(org.apache.spark.sql.functions.from_json(
                        org.apache.spark.sql.functions.col("json"), schema).as("data"))
                .select("data.*");

        // MLflow Model Serving uzerinden gercek ML inference
        // Model-serving endpoint'ine HTTP istegi atar, Random Forest + Isolation Forest
        // birlesik risk skoru alir.
        spark.udf().register("ai_model_predict", (Double amount) -> {
            try {
                // Saat ve gun bilgisini al
                LocalDateTime now = LocalDateTime.now();
                int hour = now.getHour();
                int dayOfWeek = now.getDayOfWeek().getValue() % 7;

                // Model serving'e istek at
                String json = String.format(
                        "{\"amount\":%.2f,\"hour\":%d,\"day_of_week\":%d,\"tx_per_user_last_1h\":1,\"avg_amount_last_24h\":%.2f}",
                        amount, hour, dayOfWeek, amount * 0.8
                );

                URL url = new URL(MODEL_SERVING_URL);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("POST");
                conn.setRequestProperty("Content-Type", "application/json");
                conn.setConnectTimeout(2000);
                conn.setReadTimeout(2000);
                conn.setDoOutput(true);

                try (OutputStream os = conn.getOutputStream()) {
                    os.write(json.getBytes());
                    os.flush();
                }

                int status = conn.getResponseCode();
                if (status == 200) {
                    BufferedReader reader = new BufferedReader(
                            new InputStreamReader(conn.getInputStream()));
                    StringBuilder response = new StringBuilder();
                    String line;
                    while ((line = reader.readLine()) != null) {
                        response.append(line);
                    }
                    reader.close();

                    // Basit JSON parse: "risk_score": 0.85 degerini cek
                    String resp = response.toString();
                    int idx = resp.indexOf("\"risk_score\":");
                    if (idx >= 0) {
                        String scoreStr = resp.substring(idx + 13);
                        int end = scoreStr.indexOf(",");
                        if (end < 0) end = scoreStr.indexOf("}");
                        double score = Double.parseDouble(scoreStr.substring(0, end).trim());
                        return Math.round(score * 100.0) / 100.0;
                    }
                }

                conn.disconnect();
            } catch (Exception e) {
                // Model servisi henuz hazir degilse veya hata olursa
                // fallback: basit kural tabanli skor
                System.err.println("Model serving hatasi, fallback kullaniliyor: " + e.getMessage());
            }

            // Fallback: model servisi erisilemezse kural tabanli skor
            if (amount > 5000) return 0.75;
            if (amount > 2000) return 0.45;
            return 0.15;
        }, DataTypes.DoubleType);

        Dataset<Row> enrichedDf = df.withColumn("ai_risk_score",
                org.apache.spark.sql.functions.expr("ai_model_predict(amount)"))
            .withColumn("ml_is_anomaly",
                org.apache.spark.sql.functions.expr("ai_risk_score >= 0.50"));

        StreamingQuery query = enrichedDf.writeStream()
                .format("iceberg")
                .outputMode("append")
                .option("checkpointLocation", "s3a://lakehouse/checkpoints/fraud_transactions")
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .start("nessie.db.fraud_transactions");

        query.awaitTermination();
    }
}
