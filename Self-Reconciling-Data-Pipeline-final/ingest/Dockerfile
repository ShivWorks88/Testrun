# Airflow 3 for the POC, run locally against the GCP test project.
# The same DAGs move to Managed Airflow (Cloud Composer) later without code changes.
FROM apache/airflow:3.3.2-python3.12

ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.12

# Only what the official image does not already ship; constrained to Airflow's tested versions.
RUN pip install --no-cache-dir \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
      "apache-airflow-providers-google" "psycopg[binary]" "pyarrow" \
      "google-cloud-bigquery" "google-cloud-storage" "google-cloud-pubsub" \
 && pip install --no-cache-dir "Faker>=30"

# Phase 2: dbt in its own virtualenv, so its dependencies never clash with Airflow's constraints.
# The dbt_build DAG calls /home/airflow/dbt-venv/bin/dbt.
RUN python -m venv /home/airflow/dbt-venv \
 && /home/airflow/dbt-venv/bin/pip install --no-cache-dir dbt-core==1.12.6 dbt-bigquery==1.12.1

# Our packages are mounted from the repo (see docker-compose.yml) and put on PYTHONPATH,
# so code edits apply without rebuilding.
ENV PYTHONPATH=/opt/reconcile/ingest:/opt/reconcile/data-gen