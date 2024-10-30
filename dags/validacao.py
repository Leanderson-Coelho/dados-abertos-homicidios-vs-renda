from minio import Minio
from minio.commonconfig import CopySource
import datetime
from airflow import DAG
from util import verificar_conexao_minio, verificar_bucket, BUCKET, RAW_LAYER, TRANSIENT_LAYER, obter_conexao_minio, \
    TRUSTED_LAYER
from airflow.decorators import task
from datetime import datetime
import re
import json
import os

REGEX_DATA_IPEA = r'^\d{4}-\d{2}-\d{2}$'
client = obter_conexao_minio()
SIGLAS_VALIDAS = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA",
    "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN",
    "RS", "RO", "RR", "SC", "SP", "SE", "TO"
]

with DAG(
    dag_id="validacao",
    schedule=None,
    start_date=datetime(2020, 1, 1),
    catchup=False,
    tags=['trusted'],
) as dag:


    @task(task_id="verificar-conexao-minio")
    def override_verificar_conexao_minio():
        verificar_conexao_minio(client)

    verificar_conexao_minio_step = override_verificar_conexao_minio()

    @task(task_id="verificar-bucket")
    def override_verificar_bucket():
        verificar_bucket(client)

    verificar_bucket_step = override_verificar_bucket()

    @task(task_id="filtrar-dados-invalidos-ipea")
    def filtrar_dados_invalidos_ipea():
        # {'cod': '26', 'sigla': 'PE', 'valor': '3409', 'periodo': '2022-01-15'}
        print("filtrar_dados_invalidos_ipea")
        lista_valida = []
        try:
            response = client.get_object(BUCKET, f"{RAW_LAYER}/series-328-3.json")
            print(response.json())
            for item in response.json():
                if int(item['valor']) < 0:
                    continue
                if not re.match(REGEX_DATA_IPEA, item['periodo']):
                    continue
                try:
                    data = datetime.strptime(item['periodo'], '%Y-%m-%d')
                    # Verifica se o ano é maior que 2000
                    if data.year < 2000:
                        continue
                except ValueError:
                    continue
                if item['sigla'] not in SIGLAS_VALIDAS:
                    continue

                print(item)
                lista_valida.append(item)

            with open("series-328-3.json", 'w') as arquivo_json:
                json.dump(lista_valida, arquivo_json, indent=2)

            client.fput_object(BUCKET, f"{TRUSTED_LAYER}/series-328-3.json", "series-328-3.json")

            if os.path.exists("series-328-3.json"):
                os.remove("series-328-3.json")
                print(f"File series-328-3.json deleted successfully.")


        finally:
            response.close()
            response.release_conn()


    filtrar_dados_invalidos_ipea_step = filtrar_dados_invalidos_ipea()

verificar_conexao_minio_step >> verificar_bucket_step
verificar_bucket_step >> filtrar_dados_invalidos_ipea_step





if __name__ == "__main__":
    dag.test()
