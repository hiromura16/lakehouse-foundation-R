# Rust製Sailのwheelを利用し、検証サーバーを一般ユーザーで起動する。
FROM docker.io/library/python:3.14-slim@sha256:ac25e5afbfdb4a94ff99cc24b0345719210133144481480bcba6085efa36a341
COPY --from=ghcr.io/astral-sh/uv:0.13.7@sha256:96f40d3904aad8d0bd010539001d36c8e2188b981e1334471a90c4a1d93bb490 /uv /usr/local/bin/uv
ENV UV_SYSTEM_CERTS=true UV_PYTHON_DOWNLOADS=never HOME=/tmp
COPY infra/proxy/install_ca.py /tmp/install_ca.py
RUN --mount=type=secret,id=corporate-ca,required=false \
    python3 /tmp/install_ca.py --ca-file /run/secrets/corporate-ca
COPY native/evaluation/sail.lock /tmp/requirements.lock
RUN uv --no-cache venv /opt/evaluation \
    && uv --no-cache pip sync --python /opt/evaluation/bin/python \
        --require-hashes --only-binary :all: /tmp/requirements.lock
USER 10001:10001
CMD ["/opt/evaluation/bin/sail", "spark", "server", "--ip", "0.0.0.0", "--port", "50051"]
