# Apache Airflow Essentials  <!-- omit in toc -->

Learning Airflow and building data pipelines

## Table of Contents <!-- omit in toc -->
- [Setup Environment](#setup-environment)
  - [Code completion](#code-completion)
  - [Airflow web server](#airflow-web-server)
  - [Access Postgres DB](#access-postgres-db)
  - [Cleaning-up the environment](#cleaning-up-the-environment)
- [To-Do](#to-do)



## Setup Environment

This is completely done with docker containers. Refer official documentation [here](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html). From here you can get the `docker-compose.yaml` file which is also in this repository with a slight modification of the image. I use a custom image from `Dockerfile`. In this image, I'm install the python dependencies and also postgres driver (JAR file) so the dags can communicate and write the data into the DB.

Once downloaded, start the suite of containers with `docker-compose up --build`. Then access the airflow server at `localhost:8080`.

### Code completion
Setup a virtual python env to help your IDE with code completion
- `pip install -r py_requirements.txt`
### Airflow web server
Login to web server (localhost:8080) with credentials airflow/airflow, before you run any dag, set your spark connection in case it is different from the default spark://local[*]
- Go to Admin > connections.
  - Add a new connection
  - name: `spark_default`
  - connection Type: `Spark`
  - host: `local[*]`


### Access Postgres DB
Execute a shell into the postgres container
- `docker exec -it apacheairflowessentials-postgres-1 bash`
- Then open postgres console `psql -U mdm --user airflow` where the password is `airflow`
- There should be database under the schema `mdm`
```sql
airflow=# \dt mdm.*
           List of relations
 Schema |    Name     | Type  |  Owner  
--------+-------------+-------+---------
 mdm    | emp_records | table | airflow
(1 row)

airflow=# select first_name,last_name,hire_date,job_id from mdm.emp_records limit 5;
 first_name | last_name | hire_date |  job_id
------------+-----------+-----------+----------
 Donald     | OConnell  | 21-JUN-07 | SH_CLERK
 Douglas    | Grant     | 13-JAN-08 | SH_CLERK
 Jennifer   | Whalen    | 17-SEP-03 | AD_ASST
 Michael    | Hartstein | 17-FEB-04 | MK_MAN
 Pat        | Fay       | 17-AUG-05 | MK_REP
(5 rows)

```

### Cleaning-up the environment
Since we are in docker containers, simply run `docker-compose down --volumes --remove-orphans`

## To-Do
- [x] Get a csv from a given url
  - [ ] Use different csv in each run (try using continuous data)
- [x] Load the csv into spark dataframe
- [x] Load the dataframe data into postgres  
- [ ] Create a schedule to run the dag at specific time
