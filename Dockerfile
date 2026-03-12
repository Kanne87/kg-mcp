FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY server.py .
COPY sleep.py .
COPY wbs.html .
COPY dashboard.html .

# Data volume for SQLite persistence
VOLUME /data

ENV KG_DB_PATH=/data/kg.db
ENV KG_TRANSPORT=streamable-http
ENV KG_HOST=0.0.0.0
ENV KG_PORT=8099

# Sleep workflow config (set ANTHROPIC_API_KEY + SMTP_* in Coolify)
ENV SLEEP_ENABLED=true
ENV SLEEP_CRON_HOUR=1
ENV SLEEP_CRON_MINUTE=0

EXPOSE 8099

CMD ["python", "server.py"]
