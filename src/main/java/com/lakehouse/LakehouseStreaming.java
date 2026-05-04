package com.lakehouse;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class LakehouseStreaming {
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

        // AI-Native / MLOps: Gerçek zamanlı makine öğrenmesi çıkarımı (Real-time ML Inference)
        // Bu UDF (Kullanıcı Tanımlı Fonksiyon), MLflow üzerinden yüklenen bir Random Forest
        // veya Derin Öğrenme (Deep Learning) modelinin çıkarım aşamasını (inference) temsil eder.
        spark.udf().register("ai_model_predict", (Double amount) -> {
            // Anomali skorlaması simülasyonu: 
            // Model yüksek meblağları veya anormal örüntüleri %80 üzerinde risk olarak puanlar.
            double risk = (amount > 4000) ? 0.85 + (Math.random() * 0.14) : (Math.random() * 0.30);
            return Math.round(risk * 100.0) / 100.0;
        }, DataTypes.DoubleType);

        Dataset<Row> enrichedDf = df.withColumn("ai_risk_score", 
                org.apache.spark.sql.functions.expr("ai_model_predict(amount)"))
            .withColumn("ml_is_anomaly", 
                org.apache.spark.sql.functions.expr("ai_risk_score >= 0.80"));

        StreamingQuery query = enrichedDf.writeStream()
                .format("iceberg")
                .outputMode("append")
                .option("checkpointLocation", "s3a://lakehouse/checkpoints/fraud_transactions")
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .start("nessie.db.fraud_transactions");

        query.awaitTermination();
    }
}
