# Foundation Workspace for Airflow, Spark, Hive, and Azure Data Lake Gen2 via Docker
Welcome to the Foundation Workspace repository! This project aims to provide a comprehensive workspace environment for data engineering tasks involving Airflow, Spark, Hive, and Azure Data Lake Gen2. By leveraging Docker, users can easily set up a consistent environment with all necessary dependencies for their ETL (Extract, Transform, Load) workflows.

### The WorkSpace contains the following Dependencies


| Tool | Version | Description |
| -----| ------- | -------- |
| Docker | `24.0.7` | See Mac installation [instructions](https://docs.docker.com/desktop/install/windows-install/).
| Java 17 SDK | `openjdk-17-jre-headless` | In DockerFile "RUN apt-get install -y openjdk-17-jre-headless".
| Airflow | `apache/airflow:2.8.4-python3.10` | Base Image. See release history [here](https://hub.docker.com/layers/apache/hive/4.0.0-alpha-2/images/sha256-69e482fdcebb9e07610943b610baea996c941bb36814cf233769b8a4db41f9c1?context=explore)
| Spark | `version 3.5.1` | `bitnami/spark:latest` See release history [here](https://hub.docker.com/r/bitnami/spark).
| Hive | `apache/hive:4.0.0-alpha-2` | See release history [here](https://hub.docker.com/layers/apache/hive/4.0.0-alpha-2/images/sha256-69e482fdcebb9e07610943b610baea996c941bb36814cf233769b8a4db41f9c1?context=explore).
| Azure Data Lake Gen2 | `hadoop-azure-3.3.1.jar` | The JAR must be configured during Spark Submit [here](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-azure/3.3.1).
| Python | `3.10` | Installed using `apache/airflow:2.8.4-python3.10` Image .
| PySpark | `version 3.5.1` | This should match the Spark version.


## Features
- **Dockerized Environment:** The project offers Docker containers configured with Airflow, Spark, Hive, and Azure Data Lake Gen2 dependencies, ensuring seamless setup across different platforms.
- **Complete ETL Examples:** Explore two comprehensive ETL examples included in the repository:

  - **Azure Data Lake Gen2:** Connect and perform ETL operations using PySpark, demonstrating integration with Azure Data Lake Gen2.
  - **Local Metastore:** Work with a local metastore and perform ETL tasks using PySpark and Hive, showcasing flexibility in different data storage setups.

- **Diverse DAGs:** Various types of Directed Acyclic Graphs (DAGs) are provided, incorporating Python operators and Bash operators to demonstrate different workflow configurations and task executions.
- **Configuration Files:** Essential configuration files such as Dockerfile, Java layout, and Docker Compose files are included, simplifying setup and customization of the workspace environment.

## Getting Started

- ### Clone Repo
  
Clone the repository to your local machine
``` shell
git clone https://github.com/aaliashraf/airflow-spark-hive-azure-docker-workflow.git
```

Navigate to the Repo directory
``` shell
cd airflow-spark-hive-azure-docker-workflow
```

- ### Build Docker Image

``` shell
docker compose build
```
Run the following command to generate the .env file containing the required Airflow UID 

``` shell
echo AIRFLOW_UID=1000 > .env
```

- ### Bringing Up Container Services

``` shell
docker compose up
```

## Accessing Services

After starting the containers, you can access the services through the following URLs:

### Airflow
**Username:** airflow  
**Password:** airflow
- Go To [http://localhost:8080](http://localhost:8080)
![image](https://github.com/aaliashraf/airflow-spark-hive-azure-docker-workflow/assets/56219554/a79ca824-72c7-4dfa-aca6-41186e0e3553)


- Example Airflow DAG Web UI displaying a running workflow.
![image](https://github.com/aaliashraf/airflow-spark-hive-azure-docker-workflow/assets/56219554/3fc69ff7-8c24-452f-b920-79d6c963755f)







### Spark

- Go To [http://localhost:8181](http://localhost:8181)
![image](https://github.com/aaliashraf/airflow-spark-hive-azure-docker-workflow/assets/56219554/b3790ed2-d478-469f-b70c-b8628bd93c01)




### Hive

- Go To [http://localhost:10002](http://localhost:10002)
![image](https://github.com/aaliashraf/airflow-spark-hive-azure-docker-workflow/assets/56219554/09401f86-10bd-4b12-b438-dfd43d3c9701)


## Contents

- **/dags**: Contains Airflow DAGs and workflows for ETL tasks.
- **/logs**: Airflow logs.
- **/plugins**: Airflow plugins.
- **/src**: Utility scripts, PySpark code, and JARs.
- **/metastore**: Contains Hive database and tables locally.
- **Dockerfile**: Dockerfiles for building custom Docker images.
- **docker-compose.yaml**: Docker Compose file for orchestrating containers.

have fun! 🚀🚀🚀



