# These AMD64 source manifests must match images.lock.json before the build.
FROM eclipse-temurin:21.0.13.1_2-jre-alpine@sha256:c788d7d9ddc1cf0cf38a1735e14792eb2ff4dc6886e9658771095b7e36a69523 AS jre
FROM apache/kafka:4.3.2@sha256:8f325fc00143fed78eabe00b033a86747078bb06edc21345d06651f77ae44f30
USER root
RUN rm -rf /opt/java/openjdk
COPY --from=jre /opt/java/openjdk /opt/java/openjdk
# The original JVM built the shipped CDS archives.
ENV JAVA_VERSION=jdk-21.0.13.1+2 JAVA_TOOL_OPTIONS=-Xshare:off
USER appuser
RUN java -version
