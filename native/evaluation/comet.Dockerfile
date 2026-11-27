# JVMを要する比較専用ジョブ。公式のJARは固定したSHA-256で確認する。
FROM docker.io/apache/spark:4.3.0@sha256:3dedc5c8665a6f472e2f0a3a828207a79a30785090ef325d3af219cb90a73d63
ADD --chmod=0444 --checksum=sha256:5055c43dca9e8cce83c33b1ff25646d1436c94698ca48beb3475bf60843b8d37 https://repo.maven.apache.org/maven2/org/apache/datafusion/comet-spark-spark4.1_2.13/1.2.0/comet-spark-spark4.1_2.13-1.2.0.jar /opt/spark/jars/foundation-comet.jar
COPY --chmod=0444 native/evaluation/comet_smoke.py /opt/spark/foundation-comet.py
