FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir dbt-snowflake==1.11.4

COPY . /app

RUN dbt deps

CMD ["dbt", "run"]