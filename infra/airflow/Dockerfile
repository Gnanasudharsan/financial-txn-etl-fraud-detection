# Airflow image able to run the fintxn Spark jobs (Java 17 + the project package).
FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless make \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64

USER airflow
COPY --chown=airflow:root pyproject.toml uv.lock /opt/fintxn/
COPY --chown=airflow:root src /opt/fintxn/src
COPY --chown=airflow:root configs /opt/fintxn/configs
RUN pip install --no-cache-dir /opt/fintxn
ENV FINTXN_CONFIG_DIR=/opt/fintxn/configs
