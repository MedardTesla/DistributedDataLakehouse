FROM python:3.11-slim
RUN pip install --no-cache-dir dbt-core dbt-trino==1.10.1
WORKDIR /usr/app/dbt
ENTRYPOINT ["tail", "-f", "/dev/null"]