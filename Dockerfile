# syntax=docker/dockerfile:1

ARG PYTHON_BASE
FROM python:${PYTHON_BASE}-slim AS airflow-base

ARG PYTHON_BASE
ARG AIRFLOW_VERSION

ENV PYTHON_BASE=${PYTHON_BASE}
ENV AIRFLOW_VERSION=${AIRFLOW_VERSION}
ENV AIRFLOW_HOME=/opt/airflow
WORKDIR ${AIRFLOW_HOME}

ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_BASE}.txt"

RUN python -m pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

ENV AIRFLOW_CONSTRAINT_URL=${CONSTRAINT_URL}

FROM airflow-base

ARG AIRFLOW_VERSION=${AIRFLOW_VERSION}
ARG CONSTRAINT_URL=${AIRFLOW_CONSTRAINT_URL}

RUN python -m pip install "apache-airflow-providers-fab" --constraint "${CONSTRAINT_URL}"
RUN python -m pip install "apache-airflow-providers-postgres" --constraint "${CONSTRAINT_URL}"
RUN python -m pip install psycopg2-binary

WORKDIR /app

COPY requirements.txt ./
RUN pip install -r requirements.txt

RUN pip install pyarrow

COPY . .

RUN pip install --no-deps -e ./flight-etl

RUN chmod +x ./airflow.sh

ENTRYPOINT ["./airflow.sh"]