FROM python:3.11-slim

WORKDIR /app

# System deps needed by some Python packages (lxml, cryptography, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libffi-dev && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies (layer-cached unless requirements.txt changes)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir gunicorn>=21.2.0

# Copy application code
COPY . .

# The KB database lives in a named volume mounted at runtime.
RUN mkdir -p /data

# Default environment — overridden via .env / docker-compose env_file
ENV TRADING_KB_DB=/data/trading_knowledge.db \
    FLASK_ENV=production \
    WORKERS=2 \
    THREADS=4 \
    TIMEOUT=120

EXPOSE 5050

# Liveness probe — used by Docker healthcheck and Caddy upstream check
HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:5050/health')"

# Production: gunicorn with 2 sync workers + 4 threads each.
# 2 workers is correct for SQLite (single-writer) — more workers increase
# WAL read concurrency without contention on writes.
CMD ["sh", "-c", "gunicorn api:app \
    --bind 0.0.0.0:5050 \
    --workers ${WORKERS} \
    --threads ${THREADS} \
    --timeout ${TIMEOUT} \
    --worker-class sync \
    --access-logfile - \
    --error-logfile - \
    --log-level info"]
