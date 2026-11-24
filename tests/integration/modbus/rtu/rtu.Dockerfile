# 収集器と試験ランタイムを、それぞれ不変イメージから取得する。
FROM docker.io/library/telegraf:1.40.3@sha256:695045186c69152ff71827ec0c4f40216fb10491597af71c00195d98be8c39f2 AS telegraf
FROM docker.io/library/python:3.14-slim@sha256:ac25e5afbfdb4a94ff99cc24b0345719210133144481480bcba6085efa36a341
COPY --from=telegraf /usr/bin/telegraf /usr/local/bin/telegraf
COPY --chmod=644 rtu_fixture.py telegraf-reference.toml /opt/fixture/
RUN chmod 0755 /opt/fixture
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 GOMAXPROCS=1
USER 10001:10001
ENTRYPOINT ["python", "/opt/fixture/rtu_fixture.py"]
