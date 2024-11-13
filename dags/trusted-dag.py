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
from fastavro import writer, parse_schema, reader

REGEX_DATA_IPEA = r'^\d{4}-\d{2}-\d{2}$'
client = obter_conexao_minio()
SIGLAS_VALIDAS = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA",
    "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN",
    "RS", "RO", "RR", "SC", "SP", "SE", "TO"
]

with DAG(
    dag_id="trusted-dag",
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

    @task(task_id="filtrar-dados-invalidos-IBGE")
    def filtrar_dados_invalidos_IBGE():
        print("filtrar_dados_invalidos_IBGE")
        lista_valida = []
        try:
            response = client.get_object(BUCKET, f"{RAW_LAYER}/indicadores_10070_8.1.2.1.1.json")
            print(response.json())
            response_Localidades = client.get_object(BUCKET, f"{RAW_LAYER}/Localidades.json")
            print(response_Localidades.json())

            Localidades = {}
            for item in response_Localidades.json():
                Localidades[str(item["id"])]=item
            
            for item in response.json()[0]["res"]:
                item_valido = {}
                item_valido ["localidade"] = item ["localidade"]
                item_valido ["res"] = {}
                Localidade=Localidades[item["localidade"]]
                item_valido["sigla"]=Localidade["sigla"]
                item_valido["nome"]=Localidade["nome"]
                for ano in item ["res"]:
                    try:
                        valor = int (item["res"][ano])
                        if valor < 0:
                            continue
                        item_valido ["res"][ano] = valor
                    except ValueError:
                        continue
                if len(item_valido["res"]) == 0:
                    continue
                lista_valida.append(item_valido)

            # specifying the avro schema
            schema = {
                'name': 'Renda',
                'namespace': 'IBGE',
                'type': 'record',
                'fields': [
                    {'name': 'localidade', 'type': 'string'},
                    {'name': 'res', 'type': {"type":'map', "values":"int"}},
                    {'name': 'sigla', 'type': 'string'},
                    {'name': 'nome', 'type': 'string'}
                ]
            }
            parsed_schema = parse_schema(schema)
            with open("indicadores_10070_8.1.2.1.1.avro", 'wb') as arquivo_avro:
                writer(arquivo_avro, parsed_schema, lista_valida)

            client.fput_object(BUCKET, f"{TRUSTED_LAYER}/indicadores_10070_8.1.2.1.1.avro", "indicadores_10070_8.1.2.1.1.avro")

            if os.path.exists("indicadores_10070_8.1.2.1.1.avro"):
                os.remove("indicadores_10070_8.1.2.1.1.avro")
                print(f"File indicadores_10070_8.1.2.1.1.avro deleted successfully.")


        finally:
            response.close()
            response.release_conn()
            response_Localidades.close()
            response_Localidades.release_conn()

    filtrar_dados_invalidos_IBGE_step=filtrar_dados_invalidos_IBGE()

verificar_conexao_minio_step >> verificar_bucket_step
verificar_bucket_step >> filtrar_dados_invalidos_ipea_step
verificar_bucket_step >> filtrar_dados_invalidos_IBGE_step





if __name__ == "__main__":
    dag.test()
