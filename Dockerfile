FROM apache/airflow:slim-2.9.3-python3.11

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
ENV PIP_DEFAULT_TIMEOUT=120
ENV AIRFLOW__CORE__DAGS_FOLDER=/app/dags
ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=false
ENV AIRFLOW__CORE__LOAD_EXAMPLES=false

USER root
WORKDIR /app
RUN chown -R airflow:0 /app

USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --retries 5 --timeout 120 -r /tmp/requirements.txt

COPY --chown=airflow:0 . /app

CMD ["airflow", "standalone"]
