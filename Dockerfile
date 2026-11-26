FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && useradd --create-home stratum && mkdir /data && chown stratum /data
COPY stratum stratum
COPY scripts scripts
COPY examples examples
USER stratum
ENV STRATUM_DATA=/data PYTHONDONTWRITEBYTECODE=1
EXPOSE 8000
CMD ["uvicorn","stratum.api:app","--host","0.0.0.0","--port","8000"]
