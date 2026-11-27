FROM docker.io/library/python:3.14-slim@sha256:22eafb370920ad8b429e0aec3fff433e0669968519f5d60469d83729c138b28b
COPY --from=ghcr.io/astral-sh/uv:0.13.7@sha256:96f40d3904aad8d0bd010539001d36c8e2188b981e1334471a90c4a1d93bb490 /uv /usr/local/bin/uv
COPY --chmod=644 requirements.lock /opt/fixture/requirements.lock
RUN uv --no-cache pip install --system --only-binary :all: --require-hashes -r /opt/fixture/requirements.lock
COPY --chmod=644 simulator.py probe.py /opt/fixture/
RUN chmod 0755 /opt/fixture
USER 10001:10001
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
CMD ["python", "/opt/fixture/simulator.py"]
