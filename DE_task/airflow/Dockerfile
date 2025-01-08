# Используем официальный образ Airflow
FROM apache/airflow:2.8.0

COPY setup_airflow.sh /opt/setup_scripts/setup_airflow.sh
# Устанавливаем PostgreSQL клиент
USER root
RUN apt-get update && apt-get install -y postgresql-client
RUN chown "$AIRFLOW_UID:0" /opt/setup_scripts/setup_airflow.sh
# Устанавливаем необходимые Python-библиотеки
USER airflow
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Копируем Spark-приложения (если не используем volume)
COPY ./spark_jobs /opt/airflow/spark_jobs