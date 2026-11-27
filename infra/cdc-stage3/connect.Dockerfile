# Debeziumは公式の版タグを使い、取得時のダイジェストはCIで記録する。
FROM eclipse-temurin:21.0.13.1_2-jre-ubi9-minimal@sha256:d143795277f77ebba5315b1a2d896fca4527bb9971ddd80db0e85ddf162b6999 AS jre
FROM quay.io/debezium/connect:3.7.1.Final
COPY --from=jre /opt/java/openjdk /opt/stage3-java
ENV JAVA_HOME=/opt/stage3-java JAVA_VERSION=jdk-21.0.13.1+2 PATH=/opt/stage3-java/bin:${PATH}
RUN java -version
