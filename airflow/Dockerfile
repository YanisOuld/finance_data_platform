FROM apache/airflow:2.9.3-python3.12

USER airflow
COPY airflow/requirements.txt /requirements.txt
RUN python -m pip install --upgrade pip \
 && pip install --no-cache-dir -r /requirements.txt