import io
import os
from shutil import rmtree
import json

from minio import Minio
from minio.commonconfig import CopySource
import datetime
from airflow import DAG
import fastavro
from fastavro import reader
from dags.util import TRUSTED_LAYER, ler_arquivo_local, REFINED_LAYER
from util import verificar_conexao_minio, verificar_bucket, BUCKET, RAW_LAYER, TRANSIENT_LAYER, obter_conexao_minio, \
    obter_conexao_minio_debug
from airflow.decorators import task
from fastavro import writer, parse_schema, reader


# client = obter_conexao_minio() TODO
client = obter_conexao_minio_debug()

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


def recuperar_arquivo_avro_ibge():
    client.fget_object(BUCKET, f"{TRUSTED_LAYER}/indicadores_10070_8.1.2.1.1.avro", f"{DIRETORIO_TMP}indicadores_10070_8.1.2.1.1.avro")


def recuperar_arquivo_avro_ipea():
    client.fget_object(BUCKET, f"{TRUSTED_LAYER}/series-328-3.avro", f"{DIRETORIO_TMP}series-328-3.avro")


def mesclar_dados():
    dados_ipea = ler_arquivo_local(f"{DIRETORIO_TMP}series-328-3.avro")
    dados_ibge = ler_arquivo_local(f"{DIRETORIO_TMP}indicadores_10070_8.1.2.1.1.avro")

    print("Tamanho: ", dados_ipea.__len__())
    resultado = {}

    # INICIO MESCLAGEM
    for item in dados_ipea:
        print(item)
        sigla = item["sigla"]
        periodo = item["periodo"]
        homicidios = int(item["valor"])
        ano = periodo.split("-")[0]
        if (ano == None):
            print('opala')

        if sigla not in resultado:
            resultado[sigla] = {"sigla": sigla, "periodo": []}

        resultado[sigla]["periodo"].append({
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
            for index, periodo in enumerate(resultado.get(sigla)['periodo']):
                if ano == periodo['ano']:
                    print(ano, periodo['ano'])
                    resultado.get(sigla)['periodo'][index]['renda'] = renda
                    print(index, resultado.get(sigla)['periodo'][index])

    resultado_array = []
    for key, item in resultado.items():
        print("key " + key + " - " + str(item))
        resultado_array.append(item)
    print(json.dumps(resultado_array))
    # FIM MESCLAGEM

    # SALVAR LOCAL EM JSON
    with open(f"{DIRETORIO_TMP}renda_vs_homicidios.json", "w") as outfile:
        outfile.write(json.dumps(resultado_array))

    # SALVAR LOCAL EM AVRO
    schema = {
        'name': 'Homicidios_VS_Renda',
        'namespace': 'IPEA_IBGE',
        'type': 'record',
        'fields': [
            {'name': 'sigla', 'type': 'string'},
            {'name': 'periodo', 'type': {
                'type': 'array',
                'items': {
                    'name': 'Periodo',
                    'type': 'record',
                    'fields': [
                        {'name': 'homicidios', 'type': ['null', 'int']},
                        {'name': 'renda', 'type': ['null', 'int']},
                        {'name': 'ano', 'type': 'string'}
                    ]
                }
            }}
        ]
    }
    parsed_schema = parse_schema(schema)
    with open(f"{DIRETORIO_TMP}renda_vs_homicidios.avro", 'wb') as arquivo_avro:
        writer(arquivo_avro, parsed_schema, resultado_array)


def enviar_para_refined():
    client.fput_object(BUCKET, f"{REFINED_LAYER}/renda_vs_homicidios.json", f"{DIRETORIO_TMP}renda_vs_homicidios.json")
    client.fput_object(BUCKET, f"{REFINED_LAYER}/renda_vs_homicidios.avro", f"{DIRETORIO_TMP}renda_vs_homicidios.avro")


def remover_tmp():
    if os.path.exists(DIRETORIO_TMP):
        rmtree(DIRETORIO_TMP)


if __name__ == "__main__":
    verificar_conexao_minio(client)
    verificar_bucket(client)
    recuperar_arquivo_avro_ibge()
    recuperar_arquivo_avro_ipea()
    mesclar_dados()
    enviar_para_refined()
    remover_tmp()
