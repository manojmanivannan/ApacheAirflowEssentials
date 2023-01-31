# Apache Airflow Essentials  <!-- omit in toc -->

Learning Airflow and building data pipelines

## Table of Contents <!-- omit in toc -->
- [Setup Environment](#setup-environment)
  - [Cleaning-up the environment](#cleaning-up-the-environment)



## Setup Environment

This is completely done with docker containers. Refer official documentation [here](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html). From here you can get the `docker-compose.yaml` file which is also in this repository with a slight modification of the image. I use a custom image from `Dockerfile`.

Once downloaded, start the suite of containers with `docker-compose up`. Then access the airflow server at `localhost:8080`.

### Cleaning-up the environment
Since we are in docker containers, simply run `docker compose down --volumes --remove-orphans`