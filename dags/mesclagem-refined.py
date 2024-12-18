import os
from shutil import rmtree
import json

import datetime
from airflow import DAG
from util import verificar_conexao_minio, verificar_bucket, BUCKET, obter_conexao_minio, TRUSTED_LAYER, \
    ler_arquivo_local, REFINED_LAYER, obter_conexao_minio_debug
from airflow.decorators import task
from fastavro import writer, parse_schema


client = obter_conexao_minio()
# client = obter_conexao_minio_debug()

DIRETORIO_TMP = "./tmp/"

# {
#   "sigla": "CE",
#   "periodo": [
#     {
#       "homicidios": 1,
#       "renda": 1,
#       "ano": 2012
#     }
#   ]
# }

with DAG(
    dag_id="mesclar_dados",
    schedule=None,
    start_date=datetime.datetime(2020, 1, 1),
    catchup=False,
    tags=['refined'],
) as dag:

    @task(task_id="verificar-conexao-minio")
    def override_verificar_conexao_minio():
        verificar_conexao_minio(client)

    verificar_conexao_minio_step = override_verificar_conexao_minio()


    @task(task_id="verificar-bucket")
    def override_verificar_bucket():
        verificar_bucket(client)

    verificar_bucket_step = override_verificar_bucket()


    @task(task_id="recuperar_arquivo_avro_ibge")
    def recuperar_arquivo_avro_ibge():
        client.fget_object(BUCKET, f"{TRUSTED_LAYER}/indicadores_10070_8.1.2.1.1.avro", f"{DIRETORIO_TMP}indicadores_10070_8.1.2.1.1.avro")

    recuperar_arquivo_avro_ibge_step = recuperar_arquivo_avro_ibge()


    @task(task_id="recuperar_arquivo_avro_ipea")
    def recuperar_arquivo_avro_ipea():
        client.fget_object(BUCKET, f"{TRUSTED_LAYER}/series-328-3.avro", f"{DIRETORIO_TMP}series-328-3.avro")

    recuperar_arquivo_avro_ipea_step = recuperar_arquivo_avro_ipea()


    @task(task_id="mesclar_dados")
    def mesclar_dados():
        dados_ipea = ler_arquivo_local(f"{DIRETORIO_TMP}series-328-3.avro")
        dados_ibge = ler_arquivo_local(f"{DIRETORIO_TMP}indicadores_10070_8.1.2.1.1.avro")

        print("Tamanho: ", dados_ipea.__len__())
        resultado = []

        # INICIO MESCLAGEM
        for item in dados_ipea:
            print(item)
            sigla = item["sigla"]
            periodo = item["periodo"]
            homicidios = int(item["valor"])
            ano = periodo.split("-")[0]

            # if sigla not in resultado:
            #     resultado[sigla] = {"sigla": sigla, "periodo": []}

            resultado.append({
                "sigla": sigla,
                "ano": ano,
                "renda": None,
                "homicidios": homicidios
            })



        for item in dados_ibge:
            sigla = item['sigla']
            ibge_res = item['res']

            print(sigla)
            print(str(ibge_res) + "\n")

            for ano, renda in ibge_res.items():
                for item_resultado in resultado:
                    if item_resultado['ano'] == ano and item_resultado['sigla'] == sigla:
                        item_resultado['renda'] = renda

        print(json.dumps(resultado))

        # FIM MESCLAGEM


        # SALVAR LOCAL EM AVRO
        schema = {
            'name': 'Homicidios_VS_Renda',
            'namespace': 'IPEA_IBGE',
            'type': 'record',
            'fields': [
                {'name': 'sigla', 'type': 'string'},
                {'name': 'ano', 'type': 'string'},
                {'name': 'renda', 'type': ['null', 'int']},
                {'name': 'homicidios', 'type': ['null', 'int']},
            ]
        }
        parsed_schema = parse_schema(schema)
        with open(f"{DIRETORIO_TMP}renda_vs_homicidios.avro", 'wb') as arquivo_avro:
            writer(arquivo_avro, parsed_schema, resultado)

    mesclar_dados_step = mesclar_dados()


    @task(task_id="enviar_para_refined")
    def enviar_para_refined():
        client.fput_object(BUCKET, f"{REFINED_LAYER}/renda_vs_homicidios.avro", f"{DIRETORIO_TMP}renda_vs_homicidios.avro")

    enviar_para_refined_step = enviar_para_refined()


    @task(task_id="remover_tmp")
    def remover_tmp():
        if os.path.exists(DIRETORIO_TMP):
            rmtree(DIRETORIO_TMP)

    remover_tmp_step = remover_tmp()



verificar_conexao_minio_step >> verificar_bucket_step
verificar_bucket_step >> recuperar_arquivo_avro_ibge_step
recuperar_arquivo_avro_ibge_step >> recuperar_arquivo_avro_ipea_step
recuperar_arquivo_avro_ipea_step >> mesclar_dados_step
mesclar_dados_step >> enviar_para_refined_step
enviar_para_refined_step >> remover_tmp_step



if __name__ == "__main__":
    verificar_conexao_minio(client)
    verificar_bucket(client)
    recuperar_arquivo_avro_ibge()
    recuperar_arquivo_avro_ipea()
    mesclar_dados()
    enviar_para_refined()
    remover_tmp()
