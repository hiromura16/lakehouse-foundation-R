# Rust製Sailのwheelを利用し、検証サーバーを一般ユーザーで起動する。
FROM docker.io/library/python:3.14-slim@sha256:080b4f1282edb604b21e61dfca35d1d215ed6cd1c17de27ccfc34e6fadc4ee49
COPY --from=ghcr.io/astral-sh/uv:0.13.7@sha256:96f40d3904aad8d0bd010539001d36c8e2188b981e1334471a90c4a1d93bb490 /uv /usr/local/bin/uv
ENV UV_SYSTEM_CERTS=true UV_PYTHON_DOWNLOADS=never HOME=/tmp
COPY --chmod=644 infra/proxy/uv_sync.py /tmp/uv_sync.py
COPY infra/proxy/install_ca.py /tmp/install_ca.py
# 公開CAのハッシュをビルドキャッシュの入力とする。認証値は含めない。
ARG CORPORATE_CA_SHA256
RUN --mount=type=secret,id=corporate-ca,required=false \
    python3 /tmp/install_ca.py --ca-file /run/secrets/corporate-ca \
        --expected-sha256 "${CORPORATE_CA_SHA256}"
COPY native/evaluation/sail.lock /tmp/requirements.lock
RUN --mount=type=secret,id=python-index-config,target=/run/secrets/uv.toml,required=false \
    --mount=type=secret,id=python-index-auth,target=/run/secrets/pypi.netrc,required=false \
    python /tmp/uv_sync.py /tmp/requirements.lock /opt/evaluation
USER 10001:10001
CMD ["/opt/evaluation/bin/sail", "spark", "server", "--ip", "0.0.0.0", "--port", "50051"]
