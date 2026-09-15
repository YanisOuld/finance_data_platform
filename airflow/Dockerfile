FROM apache/airflow:2.9.3-python3.12

USER airflow
COPY airflow/requirements.txt /requirements.txt
RUN python -m pip install --upgrade pip \
 && pip install --no-cache-dir -r /requirements.txt

# --- Isolated venv for the project's ETL code -------------------------------
# The project's SQLAlchemy models use 2.0-only APIs (mapped_column / Mapped),
# but Airflow 2.9 pins SQLAlchemy < 2.0 (flask-appbuilder needs < 1.5) -- the
# two can't share one interpreter. This venv inherits Airflow's already-built
# heavy libs (polars, boto3, pandas, pyarrow, pydantic...) via
# --system-site-packages and only shadows SQLAlchemy with 2.0, so it stays
# small and fast to build. DAG tasks run the pipelines here through
# ExternalPythonOperator; Airflow core keeps using its own 1.4 interpreter.
USER root
RUN python -m venv --system-site-packages /opt/pipeline-venv \
 && /opt/pipeline-venv/bin/pip install --no-cache-dir "sqlalchemy==2.0.29" \
 && chmod -R a+rx /opt/pipeline-venv
USER airflow