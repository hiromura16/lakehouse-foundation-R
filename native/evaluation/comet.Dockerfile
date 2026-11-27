# JVMを要する比較専用ジョブ。公式のJARは固定したSHA-256で確認する。
FROM docker.io/apache/spark:4.2.0-scala2.13-java17-python3-ubuntu@sha256:84a4eedb1abcf36a90808d5a1310e3e910b78c85d85aaa1599e31af5f862ed59
ADD --chmod=0444 --checksum=sha256:ea5d1caba4db6b16e86c31aa0694ea1330a8faaec58d44856df425adf5f7fe86 https://repo.maven.apache.org/maven2/org/apache/datafusion/comet-spark-spark4.2_2.13/1.2.0/comet-spark-spark4.2_2.13-1.2.0.jar /opt/spark/jars/foundation-comet.jar
COPY --chmod=0444 native/evaluation/comet_smoke.py /opt/spark/foundation-comet.py
