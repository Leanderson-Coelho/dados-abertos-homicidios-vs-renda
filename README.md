# dados-abertos-homicidios-vs-renda

Projeto da disciplina de Integração de Dados do Mestrado Profissional do IFPB

# Configuração do ambiente

##### Configurar primeira inicialização MinIO

On **Linux**:

> mkdir ./minio-data

On Windows:

- Criar pasta minio-data

#### Configurando primeira incialização do airflow

On **Linux**, the quick-start needs to know your host user id and needs to have group id set to 0. Otherwise the files created in dags, logs and plugins will be created with root user ownership. You have to make sure to configure them for the docker-compose:

<!-- > mkdir -p ./logs ./plugins ./config -->

> mkdir -p ./dags ./logs ./plugins ./config
> echo -e "AIRFLOW_UID=$(id -u)" > .env

On Windows:

- Criar pastas ./dags ./logs ./plugins ./config

Depois:

> docker compose up airflow-init --build

After initialization is complete, you should see a message like this:

```
airflow-init_1       | Upgrades done
airflow-init_1       | Admin user airflow created
airflow-init_1       | 2.10.2
start_airflow-init_1 exited with code 0
```

Após isso rodar o comando

> docker compose up

The webserver is available at: http://localhost:8080

- [Caso aconteça algum erro e precisar reiniciar o processo use este link para apagar os containers](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#cleaning-up-the-environment)

## Integrantes

Leanderson, Nathália, Allan

## Data Lake

### Arquitetura do Data Lake escolhida: Zone-Based Architecture

#### Zones iniciais

- transient landing
- raw data
- trusted data

## Ingestion

## Processing

### Airflow

Links úteis
Airflow com docker compose: https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html
