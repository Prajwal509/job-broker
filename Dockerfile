# syntax=docker/dockerfile:1
FROM python:3.11-slim AS build
WORKDIR /app
COPY requirements.txt .
RUN python -m pip install --prefix=/install -r requirements.txt

FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY --from=build /install /usr/local
COPY api.py .
COPY broker ./broker
RUN groupadd --system app && useradd --system --gid app --create-home app
RUN mkdir -p /data && chown app:app /data
USER app
ENV DB_PATH=/data/job-broker.db
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/healthz')" || exit 1
CMD ["python", "-m", "uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]
