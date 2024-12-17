from minio import Minio
from airflow.hooks.base import BaseHook
from fastavro import reader

BUCKET = "ppgti"
RAW_LAYER = "raw"
TRANSIENT_LAYER = "transient"
TRUSTED_LAYER = "trusted"
REFINED_LAYER = "refined"

def verificar_conexao_minio(client: Minio):
    print("verificar_conexao_minio")
    client.list_buckets()

def verificar_bucket(client: Minio):
    print("verificar_bucket")
    buckets = client.list_buckets()
    buckets = [i.name for i in buckets]
    if BUCKET not in buckets:
        client.make_bucket(BUCKET)

def obter_conexao_minio():
    minio_connection = BaseHook.get_connection('minio')
    host = minio_connection.host + ':' + str(minio_connection.port)

    client = Minio(host, secure=False, access_key=minio_connection.login, secret_key=minio_connection.password)

    return client

def obter_conexao_minio_debug():
    client = Minio("localhost:9000", secure=False, access_key="test", secret_key="test12334567")
    return client


def ler_arquivo_local(file):
    with open(file, "rb") as arquivo:
        leitor = reader(arquivo)
        registros = []

        # print("Esquema:", leitor.writer_schema)

        for registro in leitor:
            registros.append(registro)

    return registros