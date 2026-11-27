# JVMを要する比較専用ジョブ。公式のJARは固定したSHA-256で確認する。
FROM docker.io/apache/spark:4.2.1-scala2.13-java17-python3-ubuntu@sha256:c0e6770a7c500d3f1090c9b9f4e26cb5914aec987a94cd44cae1eb1f65b0fa51
ADD --chmod=0444 --checksum=sha256:ea5d1caba4db6b16e86c31aa0694ea1330a8faaec58d44856df425adf5f7fe86 https://repo.maven.apache.org/maven2/org/apache/datafusion/comet-spark-spark4.2_2.13/1.2.0/comet-spark-spark4.2_2.13-1.2.0.jar /opt/spark/jars/foundation-comet.jar
COPY --chmod=0444 native/evaluation/comet_smoke.py /opt/spark/foundation-comet.py
