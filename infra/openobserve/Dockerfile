# 検査用BusyBoxと公式バイナリを固定し、ビルド中のパッケージ取得を避ける。
FROM docker.io/library/busybox:1.37.0-uclibc@sha256:7aa3d3c53cc1ed823c6a5967ebaac87700a5b0766af45ff599dbca3506c8b072 AS probe
FROM public.ecr.aws/zinclabs/openobserve:v1.1.2@sha256:4a154849432f75c4fb024de32fe34163c02406c905dab22bf8d213d2e3386392
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
