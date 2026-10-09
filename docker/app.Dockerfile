# One image for every Python workload: ingestion, Kafka consumers, Spark job and API.
FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install ".[pipeline,api,spark]"

# S3A (MinIO) + PostgreSQL JDBC drivers, versions matched to PySpark 3.5.3 / Hadoop 3.3.4
ARG MAVEN=https://repo1.maven.org/maven2
RUN SPARK_JARS="$(python -c 'import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')" \
    && curl -fsSL -o "$SPARK_JARS/hadoop-aws-3.3.4.jar" "$MAVEN/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" \
    && curl -fsSL -o "$SPARK_JARS/aws-java-sdk-bundle-1.12.262.jar" "$MAVEN/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" \
    && curl -fsSL -o "$SPARK_JARS/postgresql-42.7.4.jar" "$MAVEN/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar"

RUN useradd --create-home --uid 1000 app
USER app

CMD ["python", "-m", "jobmarket.ingest", "--help"]
