# 検査用BusyBoxと公式バイナリを固定し、ビルド中のパッケージ取得を避ける。
ARG OPENOBSERVE_IMAGE_PATH=docker.io/openobserve/openobserve
FROM docker.io/library/busybox:1.38.0-uclibc@sha256:b2e35951f72501f6a99f1bf45bcfe6d4795be08c518c2f97d0ed2fe776958a3b AS probe
FROM ${OPENOBSERVE_IMAGE_PATH}:v1.2.0@sha256:896361d533cac56ef346d940b1ddd6664e04d5f9606bd80b4a2e28069a33020e
ARG WORKER_UID=10001
ARG WORKER_GID=10001
# BusyBoxはamd64・arm64の静的リンク版。OpenObserveと共有ライブラリは公式イメージに含まれる。
COPY --from=probe /bin/busybox /bin/busybox
# 空のvolumeを初期化するための権限。マウント済みのデータには適用しない。
RUN ["/bin/busybox", "sh", "-ec", "test \"$WORKER_UID\" -gt 0; /bin/busybox mkdir -p /data; /bin/busybox chown \"$WORKER_UID:$WORKER_GID\" /data; /bin/busybox chmod 0700 /data"]
RUN ["/bin/busybox", "mkdir", "-m", "0755", "/etc/lakehouse-openobserve"]
COPY --chmod=0444 infra/openobserve/entrypoint.sh /etc/lakehouse-openobserve/entrypoint.sh
COPY --chmod=0444 infra/openobserve/health.sh /etc/lakehouse-openobserve/health.sh
USER ${WORKER_UID}:${WORKER_GID}
ENTRYPOINT ["/bin/busybox", "sh", "/etc/lakehouse-openobserve/entrypoint.sh"]
