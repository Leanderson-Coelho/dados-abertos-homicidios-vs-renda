import datetime
from requests import request
import json
from minio import Minio
import os
from airflow.hooks.base import BaseHook
from airflow import DAG
from airflow.decorators import task

URI = "https://www.ipea.gov.br/atlasviolencia/api/v1/valores-series/328/3"
BUCKET = "ppgti"

URI_IBGE = "https://servicodados.ibge.gov.br/api/v1/pesquisas/-/indicadores/80663/resultados/N3"

URI_IBGE_Localidade ="https://servicodados.ibge.gov.br/api/v1/localidades/estados"

minio_connection = BaseHook.get_connection('minio')
host = minio_connection.host + ':' + str(minio_connection.port)

client = Minio(host, secure=False, access_key=minio_connection.login, secret_key=minio_connection.password)

with DAG(
    dag_id="ipea_download",
    schedule=None,
    start_date=datetime.datetime(2020, 1, 1),
    catchup=False,
    tags=['ipea'],
) as dag:

    @task(task_id="verificar-conexao-minio")
    def verificar_conexao_minio():
        print("verificar_conexao_minio")
        client.list_buckets()

    verificar_conexao_minio_step = verificar_conexao_minio()

    @task(task_id="verificar-bucket")
    def verificar_bucket():
        print("verificar_bucket")
        buckets = client.list_buckets()
        buckets = [i.name for i in buckets]
        if BUCKET not in buckets:
            client.make_bucket(BUCKET)

    verificar_bucket_step = verificar_bucket()

    @task(task_id="ipea-download")
    def ipea_donwload():
        print("ipea_donwload")
        response = request("GET", URI)

        json_object = json.dumps(response.json(), indent=2)

        # Writing to sample.json
        with open("series-328-3.json", "w") as outfile:
            outfile.write(json_object)

    ipea_donwload_step = ipea_donwload()

    @task(task_id="IBGE-download")
    def IBGE_donwload():
        print("IBGE_donwload")
        response = request("GET", URI_IBGE)

        json_object = json.dumps(response.json(), indent=2)

        # Writing to sample.json
        with open("indicadores_10070_8.1.2.1.1.json", "w") as outfile:
            outfile.write(json_object)

        response = request("GET", URI_IBGE_Localidade)

        json_object = json.dumps(response.json(), indent=2)

        # Writing to sample.json
        with open("Localidades.json", "w") as outfile:
            outfile.write(json_object)

    IBGE_donwload_step = IBGE_donwload()

    @task(task_id="enviar-para-minio")
    def enviar_para_minio():
        print("enviar_para_minio")
        client.fput_object(BUCKET, "transient/series-328-3.json", "series-328-3.json")
        client.fput_object(BUCKET, "transient/indicadores_10070_8.1.2.1.1.json", "indicadores_10070_8.1.2.1.1.json")
        client.fput_object(BUCKET, "transient/Localidades.json", "Localidades.json")

    enviar_para_minio_step = enviar_para_minio()

    @task(task_id="limpar-dados")
    def limpar_dados():
        print("limpar_dados")
        if os.path.exists("series-328-3.json"):
            os.remove("series-328-3.json")
            print(f"File series-328-3.json deleted successfully.")
        else:
            print(f"File series-328-3.json not found.")

        if os.path.exists("indicadores_10070_8.1.2.1.1.json"):
            os.remove("indicadores_10070_8.1.2.1.1.json")
            print(f"File indicadores_10070_8.1.2.1.1.json deleted successfully.")
        else:
            print(f"File indicadores_10070_8.1.2.1.1.json not found.")
        
        if os.path.exists("Localidades.json"):
            os.remove("Localidades.json")
            print(f"File Localidades.json deleted successfully.")
        else:
            print(f"File Localidades.json not found.")

    limpar_dados_step = limpar_dados()

verificar_conexao_minio_step >> verificar_bucket_step
verificar_bucket_step >> ipea_donwload_step
ipea_donwload_step >> enviar_para_minio_step
verificar_bucket_step >> IBGE_donwload_step
IBGE_donwload_step >> enviar_para_minio_step
enviar_para_minio_step >> limpar_dados_step