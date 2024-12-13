from minio import Minio
from minio.commonconfig import CopySource
import datetime
from airflow import DAG
from util import verificar_conexao_minio, verificar_bucket, BUCKET, RAW_LAYER, TRANSIENT_LAYER, obter_conexao_minio
from airflow.decorators import task

client = obter_conexao_minio()

with DAG(
    dag_id="mover_dados_brutos_raw",
    schedule=None,
    start_date=datetime.datetime(2020, 1, 1),
    catchup=False,
    tags=['raw'],
) as dag:

    @task(task_id="verificar-conexao-minio")
    def override_verificar_conexao_minio():
        verificar_conexao_minio(client)

    verificar_conexao_minio_step = override_verificar_conexao_minio()

    @task(task_id="verificar-bucket")
    def override_verificar_bucket():
        verificar_bucket(client)

    verificar_bucket_step = override_verificar_bucket()

    @task(task_id="mover-dados-ipea")
    def mover_dados_ipea_raw():
        print("mover_dados_ipea_raw")
        client.copy_object(BUCKET, f"{RAW_LAYER}/series-328-3.json",
                        CopySource(BUCKET, f"{TRANSIENT_LAYER}/series-328-3.json"))


    mover_dados_ipea_raw_step = mover_dados_ipea_raw()
    @task(task_id="mover-dados-ibge")
    def mover_dados_ibge_raw():
        print("mover_dados_ibge_raw")
        client.copy_object(BUCKET, f"{RAW_LAYER}/indicadores_10070_8.1.2.1.1.json", CopySource(BUCKET, f"{TRANSIENT_LAYER}/indicadores_10070_8.1.2.1.1.json"))
        client.copy_object(BUCKET, f"{RAW_LAYER}/Localidades.json", CopySource(BUCKET, f"{TRANSIENT_LAYER}/Localidades.json"))
        client.copy_object(BUCKET, f"{RAW_LAYER}/Renda_01_15.json", CopySource(BUCKET, f"{TRANSIENT_LAYER}/Renda_01_15.json"))
        client.copy_object(BUCKET, f"{RAW_LAYER}/Renda_96_06_grande_regiao.json", CopySource(BUCKET, f"{TRANSIENT_LAYER}/Renda_96_06_grande_regiao.json"))

    mover_dados_ibge_raw_step = mover_dados_ibge_raw()



verificar_conexao_minio_step >> verificar_bucket_step
verificar_bucket_step >> mover_dados_ipea_raw_step
verificar_bucket_step >> mover_dados_ibge_raw_step


if __name__ == "__main__":
    dag.test()

