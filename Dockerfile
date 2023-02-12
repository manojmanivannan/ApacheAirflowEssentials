FROM apache/airflow:2.5.1
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-11-jre-headless procps wget\
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
RUN wget -O /usr/local/share/postgresql-42.5.2.jar https://jdbc.postgresql.org/download/postgresql-42.5.2.jar
USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt