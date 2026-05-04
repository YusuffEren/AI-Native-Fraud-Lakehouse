#!/bin/bash
set -e

/opt/spark/bin/spark-submit \
  --class com.datavizyon.SparkStreamingJob \
  --master local[*] \
  --conf spark.driver.memory=512m \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.nessie.type=nessie \
  --conf "spark.sql.catalog.nessie.uri=${NESSIE_URI}" \
  --conf "spark.sql.catalog.nessie.warehouse=${WAREHOUSE}" \
  --conf spark.sql.catalog.nessie.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf "spark.sql.catalog.nessie.s3.endpoint=${MINIO_ENDPOINT}" \
  --conf spark.sql.catalog.nessie.s3.path-style-access=true \
  --conf "spark.sql.catalog.nessie.s3.access-key-id=${MINIO_ACCESS_KEY}" \
  --conf "spark.sql.catalog.nessie.s3.secret-access-key=${MINIO_SECRET_KEY}" \
  /opt/spark/app.jar
