FROM ghcr.io/dbt-labs/dbt-postgres:1.8.2

ENV DBT_PROFILES_DIR=/usr/app/dbt
WORKDIR /usr/app/dbt
COPY dbt/ /usr/app/dbt/

ENTRYPOINT ["dbt"]
CMD ["build"]
