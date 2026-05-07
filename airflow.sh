#!/bin/bash
airflow db migrate
airflow users create \
--username ${USERNAME} \
--firstname ${FIRSTNAME:-Admin} \
--lastname ${LASTNAME:-user} \
--role Admin \
--email ${EMAIL} \
--password ${PASSWORD}||true
airflow dag-processor &
airflow triggerer &
airflow scheduler &
airflow api-server --port 8080
