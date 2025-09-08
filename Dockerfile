FROM python:3.12-slim

RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -m -u 1000 appuser

WORKDIR /app

RUN pip install --no-cache-dir uv

COPY --chown=appuser:appuser pyproject.toml ./

RUN uv pip install --system --no-cache .

COPY --chown=appuser:appuser main.py ./

USER appuser

CMD ["python", "main.py"]