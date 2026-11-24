FROM docker.io/library/python:3.14-slim@sha256:ac25e5afbfdb4a94ff99cc24b0345719210133144481480bcba6085efa36a341
COPY --from=ghcr.io/astral-sh/uv:0.13.7@sha256:96f40d3904aad8d0bd010539001d36c8e2188b981e1334471a90c4a1d93bb490 /uv /usr/local/bin/uv
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 UV_SYSTEM_CERTS=true
COPY --chmod=644 infra/proxy/uv_sync.py /tmp/uv_sync.py
COPY infra/proxy/install_ca.py /tmp/install_ca.py
COPY native/telemetry/requirements-opcua.lock /tmp/requirements.lock
# 公開CAのハッシュをビルドキャッシュの入力とする。認証値は含めない。
ARG CORPORATE_CA_SHA256
RUN --mount=type=secret,id=corporate-ca,required=false \
    --mount=type=secret,id=python-index-config,target=/run/secrets/uv.toml,required=false \
    --mount=type=secret,id=python-index-auth,target=/run/secrets/pypi.netrc,required=false \
    python /tmp/install_ca.py --ca-file /run/secrets/corporate-ca \
        --expected-sha256 "${CORPORATE_CA_SHA256}" \
    && python /tmp/uv_sync.py /tmp/requirements.lock /opt/simulator
COPY --chmod=644 native/telemetry/opcua_simulator.py /opt/opcua_simulator.py
ENTRYPOINT ["/opt/simulator/bin/python", "/opt/opcua_simulator.py"]
