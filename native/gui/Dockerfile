FROM docker.io/library/python:3.14-slim@sha256:ac25e5afbfdb4a94ff99cc24b0345719210133144481480bcba6085efa36a341 AS build
COPY --from=ghcr.io/astral-sh/uv:0.13.7@sha256:96f40d3904aad8d0bd010539001d36c8e2188b981e1334471a90c4a1d93bb490 /uv /usr/local/bin/uv

# 変換ワーカーと同じ公開CAの受け渡しを使い、依存をハッシュで検証する。
ENV UV_SYSTEM_CERTS=true UV_PYTHON_DOWNLOADS=never
COPY infra/proxy/install_ca.py /tmp/install_ca.py
COPY native/gui/requirements.lock /tmp/requirements.lock
RUN --mount=type=secret,id=corporate-ca,required=false \
    python /tmp/install_ca.py --ca-file /run/secrets/corporate-ca \
    && uv --no-cache venv /opt/venv \
    && uv --no-cache pip sync --python /opt/venv/bin/python \
       --require-hashes --only-binary :all: /tmp/requirements.lock

FROM docker.io/library/python:3.14-slim@sha256:ac25e5afbfdb4a94ff99cc24b0345719210133144481480bcba6085efa36a341
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH" \
    HOME=/tmp \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    STREAMLIT_SERVER_HEADLESS=true \
    STREAMLIT_SERVER_ADDRESS=0.0.0.0 \
    STREAMLIT_SERVER_FILE_WATCHER_TYPE=none \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false
WORKDIR /opt/lakehouse-view
COPY --chmod=644 native/gui/app.py ./app.py
COPY --chmod=644 native/gui/theme.toml ./.streamlit/config.toml
RUN chmod 755 .streamlit
USER 10001:10001
CMD ["sh", "-c", "exec streamlit run app.py --browser.serverAddress \"$LAKEHOUSE_BROWSER_HOST\""]
