package com.datavizyon;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class SparkStreamingJob {

    public static void main(String[] args) throws Exception {
        String kafkaBootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "kafka:9092");
        String topic = System.getenv().getOrDefault("KAFKA_TOPIC", "transactions");
        String nessieUri = System.getenv().getOrDefault("NESSIE_URI", "http://nessie.default.svc.cluster.local:19120/api/v2");
        String warehouse = System.getenv().getOrDefault("WAREHOUSE", "s3://lakehouse/");
        String minioEndpoint = System.getenv().getOrDefault("MINIO_ENDPOINT", "http://minio.default.svc.cluster.local:9000");
        String minioAccessKey = System.getenv().getOrDefault("MINIO_ACCESS_KEY", "minioadmin");
        String minioSecretKey = System.getenv().getOrDefault("MINIO_SECRET_KEY", "minioadmin");

        SparkSession spark = SparkSession.builder()
            .appName("DataVizyonSparkStreaming")
            .master("local[*]")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.nessie.type", "nessie")
            .config("spark.sql.catalog.nessie.uri", nessieUri)
            .config("spark.sql.catalog.nessie.warehouse", warehouse)
            .config("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config("spark.sql.catalog.nessie.s3.endpoint", minioEndpoint)
            .config("spark.sql.catalog.nessie.s3.path-style-access", "true")
            .config("spark.sql.catalog.nessie.s3.access-key-id", minioAccessKey)
            .config("spark.sql.catalog.nessie.s3.secret-access-key", minioSecretKey)
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.sql.adaptive.enabled", "false")
            .getOrCreate();

        StructType schema = new StructType()
            .add("id", DataTypes.StringType)
            .add("seq", DataTypes.LongType)
            .add("timestamp", DataTypes.LongType)
            .add("amount", DataTypes.DoubleType)
            .add("category", DataTypes.StringType)
            .add("user_id", DataTypes.StringType)
            .add("currency", DataTypes.StringType)
            .add("status", DataTypes.StringType);

        Dataset<Row> df = spark.readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", kafkaBootstrap)
            .option("subscribe", topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
            .selectExpr("CAST(value AS STRING) as json")
            .select(org.apache.spark.sql.functions.from_json(org.apache.spark.sql.functions.col("json"), schema).as("data"))
            .select("data.*");

        StreamingQuery query = df.writeStream()
            .format("iceberg")
            .outputMode("append")
            .option("checkpointLocation", "s3a://lakehouse/checkpoints")
            .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("10 seconds"))
            .start("nessie.default.transactions");

        query.awaitTermination();
    }
}
