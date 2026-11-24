# 固定した公式イメージを使い、空のデータvolumeの所有者を設定する。
FROM docker.io/library/influxdb:3-core@sha256:9eb55d1873ea629e497179cc465d1d345a78f4bce5c5bc318d3d3ac0b8647022
ARG TOOLS_UID=1000
ARG TOOLS_GID=1000
USER root
RUN test "$TOOLS_UID" -gt 0 \
    && mkdir -p /var/lib/influxdb3/data \
    && chown "$TOOLS_UID:$TOOLS_GID" /var/lib/influxdb3/data \
    && chmod 0700 /var/lib/influxdb3/data
# ホストの0600のトークンを、実行ユーザーでも読み取れるようにする。
USER ${TOOLS_UID}:${TOOLS_GID}
