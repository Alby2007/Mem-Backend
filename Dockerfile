FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (layer-cached unless requirements.txt changes)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the codebase
COPY . .

# The KB database lives in a named volume mounted at runtime.
# Create the mount-point directory so Docker can bind it.
RUN mkdir -p /data

# Use the volume path as the default DB location.
# Can be overridden via TRADING_KB_DB env var.
ENV TRADING_KB_DB=/data/trading_knowledge.db

EXPOSE 5050

CMD ["python", "api.py"]
