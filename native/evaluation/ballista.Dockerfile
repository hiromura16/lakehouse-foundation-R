# 公式Pythonクライアントで独立したSchedulerとExecutorへ問い合わせる。
FROM docker.io/library/python:3.14-slim@sha256:22eafb370920ad8b429e0aec3fff433e0669968519f5d60469d83729c138b28b
COPY --from=ghcr.io/astral-sh/uv:0.13.7@sha256:96f40d3904aad8d0bd010539001d36c8e2188b981e1334471a90c4a1d93bb490 /uv /usr/local/bin/uv
ENV UV_SYSTEM_CERTS=true UV_PYTHON_DOWNLOADS=never HOME=/tmp
COPY --chmod=644 infra/proxy/uv_sync.py /tmp/uv_sync.py
COPY infra/proxy/install_ca.py /tmp/install_ca.py
# 公開CAのハッシュをビルドキャッシュの入力とする。認証値は含めない。
ARG CORPORATE_CA_SHA256
RUN --mount=type=secret,id=corporate-ca,required=false \
    python3 /tmp/install_ca.py --ca-file /run/secrets/corporate-ca \
        --expected-sha256 "${CORPORATE_CA_SHA256}"
COPY native/evaluation/ballista.lock /tmp/requirements.lock
RUN --mount=type=secret,id=python-index-config,target=/run/secrets/uv.toml,required=false \
    --mount=type=secret,id=python-index-auth,target=/run/secrets/pypi.netrc,required=false \
    python /tmp/uv_sync.py /tmp/requirements.lock /opt/evaluation
COPY --chmod=0444 native/evaluation/ballista_check.py /opt/evaluation/check.py
USER 10001:10001
CMD ["/opt/evaluation/bin/python", "/opt/evaluation/check.py"]
