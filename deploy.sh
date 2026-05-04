#!/bin/bash
set -e

echo "1. Minikube Docker Ortami Yukleniyor..."
minikube status || minikube start
eval $(minikube docker-env)

echo "2. Java Uygulamasi Docker Image Olarak Derleniyor..."
docker build -t lakehouse/app:latest .

echo "3. Altyapi Servisleri Kuruluyor..."
kubectl apply -f minio.yaml
kubectl apply -f nessie.yaml
kubectl apply -f mlflow.yaml

echo "4. Kafka Kuruluyor (Strimzi)..."
kubectl apply -f kafka-strimzi.yaml
echo "Kafka'nin hazir olmasi bekleniyor (Bu islem birkac dakika surebilir)..."
kubectl wait kafka/my-cluster --for=condition=Ready --timeout=300s || true

echo "5. Uygulamalar Deploy Ediliyor..."
kubectl apply -f producer-deploy.yaml
kubectl apply -f streaming-deploy.yaml

echo "Kurulum Tamamlandi!"
kubectl get pods
