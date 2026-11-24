FROM docker.io/library/python:3.14-slim@sha256:ac25e5afbfdb4a94ff99cc24b0345719210133144481480bcba6085efa36a341 AS base
COPY --from=ghcr.io/astral-sh/uv:0.13.7@sha256:96f40d3904aad8d0bd010539001d36c8e2188b981e1334471a90c4a1d93bb490 /uv /usr/local/bin/uv

# 元の信頼ストアを保ったまま、任意で会社CAの公開証明書を追加する。
# BuildKitの標準プロキシ引数は実行時の環境変数に残さない。
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt \
    REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt \
    AWS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt \
    UV_SYSTEM_CERTS=true \
    UV_PYTHON_DOWNLOADS=never \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
COPY --chmod=644 infra/proxy/uv_sync.py /tmp/uv_sync.py
COPY infra/proxy/install_ca.py /tmp/install_ca.py
# 公開CAのハッシュをビルドキャッシュの入力とする。認証値は含めない。
ARG CORPORATE_CA_SHA256
RUN --mount=type=secret,id=corporate-ca,required=false \
    python /tmp/install_ca.py --ca-file /run/secrets/corporate-ca \
        --expected-sha256 "${CORPORATE_CA_SHA256}"

WORKDIR /opt/lakehouse
COPY native/requirements.lock ./requirements.lock
RUN --mount=type=secret,id=python-index-config,target=/run/secrets/uv.toml,required=false \
    --mount=type=secret,id=python-index-auth,target=/run/secrets/pypi.netrc,required=false \
    python /tmp/uv_sync.py requirements.lock /opt/venv

# 実行ステージへは信頼ストア、仮想環境、実行に必要なコードだけを移す。
FROM docker.io/library/python:3.14-slim@sha256:ac25e5afbfdb4a94ff99cc24b0345719210133144481480bcba6085efa36a341
COPY --from=base /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=base /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH" \
    SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt \
    REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt \
    AWS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR /opt/lakehouse
COPY --chmod=644 scripts/raw_file_input.py scripts/landing_contract.py scripts/generate-demo.py scripts/source_scenarios.py scripts/sql_batch_worker.py ./scripts/
COPY --chmod=644 contracts/native/data-products.json ./contracts/native/data-products.json
COPY --chmod=755 medallion ./medallion
COPY --chmod=644 native/*.py native/openapi.json ./native/
COPY --chmod=644 infra/cdc/config.py infra/cdc/consumer.py infra/cdc/consumer_runtime.py infra/cdc/event_limits.py infra/cdc/fixed_source.py infra/cdc/raw_writer.py infra/cdc/runtime_control.py infra/cdc/state.py infra/cdc/raw_history.py infra/cdc/cdc_replay.py ./cdc/
RUN chmod 755 /opt/lakehouse/native /opt/lakehouse/scripts /opt/lakehouse/cdc \
    /opt/lakehouse/contracts /opt/lakehouse/contracts/native
USER 10001:10001
CMD ["python", "-m", "native.api", "serve"]
