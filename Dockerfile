FROM python:3.11-slim-bookworm AS dependencies
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app/src
RUN pip install --no-cache-dir uv==0.12.19
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --no-dev --no-install-project
ENV PATH=/app/.venv/bin:$PATH

FROM dependencies AS spark
USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --no-dev --extra spark --no-install-project
# Resolve connector jars at build time, not during a recovery.
COPY infra/resolve_spark.py /tmp/resolve_spark.py
RUN python /tmp/resolve_spark.py
COPY src ./src
CMD ["python", "-m", "observatory.streaming"]

FROM dependencies AS runtime
COPY frontend/dist ./frontend/dist
COPY docs ./docs
COPY src ./src
CMD ["uvicorn", "observatory.api:app", "--host", "0.0.0.0", "--port", "8080"]
