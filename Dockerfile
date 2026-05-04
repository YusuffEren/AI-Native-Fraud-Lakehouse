# 1. Derleme Asamasi
FROM maven:3.9.6-eclipse-temurin-17 AS builder
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package -DskipTests

# 2. Calistirma Asamasi
FROM eclipse-temurin:17-jre-alpine
RUN apk add --no-cache bash
WORKDIR /app
COPY --from=builder /app/target/ai-native-lakehouse-1.0-SNAPSHOT-jar-with-dependencies.jar /app/app.jar
