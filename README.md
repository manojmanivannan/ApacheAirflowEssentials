# Apache Airflow Essentials  <!-- omit in toc -->

Learning Airflow and building data pipelines

## Table of Contents <!-- omit in toc -->
- [Setup Environment](#setup-environment)
  - [Code completion](#code-completion)
  - [Airflow web server](#airflow-web-server)
  - [Cleaning-up the environment](#cleaning-up-the-environment)



## Setup Environment

This is completely done with docker containers. Refer official documentation [here](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html). From here you can get the `docker-compose.yaml` file which is also in this repository with a slight modification of the image. I use a custom image from `Dockerfile`.

Once downloaded, start the suite of containers with `docker-compose up --build`. Then access the airflow server at `localhost:8080`.

### Code completion
Setup a virtual python env to help your IDE with code completion
- `pip install -r py_requirements.txt`
### Airflow web server
Login to web server with credentials airflow/airflow, before you run any dag, set your spark connection
- Then go to Admin > connections.
  - Add a new connection
  - name: `spark_default`
  - connection Type: `Spark`
  - host: `local[*]`
  - extras: `{"queue":"root.default","spark_binary":"spark-submit","namespace":"default"}`

### Cleaning-up the environment
Since we are in docker containers, simply run `docker compose down --volumes --remove-orphans`