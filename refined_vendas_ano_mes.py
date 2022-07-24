from fileinput import filename
from google.cloud import bigquery
from google.cloud import storage
import pandas
from google.oauth2 import service_account

import logging
from sys import stdout
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as f


credentials_dict = {
  "type": "service_account",
  "project_id": "teste-gcp-2022",
  "private_key_id": "ce2dffe409dae8a1978c4c9182ddd4f2849de7fb",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDJo/d5+KMQ5R3f\neWTVYjPYFzI6mTFnoHLpqNlDw61hQxQOJqFUJDhTn1QKob79WMtUPSk6Ez5nA/V5\nf9hVFb5T12m0tBc2KxJXwiyM5NQ+0pT34brAij+mQHAaKccRA9V7kUrZ1MZiqW+p\nwvWa3VwT4ylk67PAKPFpusJp5BR5AHcnvLslvnUU9rmBolAb+z8NfCPD5fZp/r3u\nupVcV7Icld30cyINOw37l5H++j39SApaET9kDItzf1kv8rhivws8Wq7Rjpqj0Dwj\n/NabNVZkU6caoePGjix02NUU+LekgiZo1htRoVcM2VhKYR+kW/r5NaR3QLtiFMl/\niHjVT9fxAgMBAAECggEAD+X28v28463g8VgJsH5d7pWDnxWpjM1cihpHSR3CS5cl\nm2kF0tPJQiSflHgEzkZkP7fTypGuW6J3BhWjqa+9cjBblAUYCGwUdeloKFK5fluN\npc69MgWkd5gAjruJB8ko2aWOxIaPfsNzu6uUwFlgM2w3fQ7NfSLtR+QTBJTYXWLd\nVNrUUhT8fuFrsYn8dGP1293ephDBGGO9ybV/heFjRJOuEUrZ8DTEcersJlDqlxmp\nkBjfYRP8k2db1gr2C/ZQfvxcmvGkW+xvSwOm42JyBo5z+aJ/ldDkAhM3Tm4yzA2L\nYOVn7YogTSI4e5ymu1s6lA9Npkh2HKSmz4H0McNIjQKBgQD0yO0PcEDolwO6l//t\nDJ43yQ81hkNvZQTj0hGri8SQazFei4eVRgoqV0o+ErHLC/YfkqY4mtO9Cs2lPp+v\nkc2h21YZQzthf0K8LMBnxlnpwA4FGevxFuKIbXriWsTM5hW1oxrI1PCz6heFvYin\nS2Wq95c++A7Q4JclSnVz8+qeRQKBgQDS4QBv2m8j8ELONsz7T6Y9g0izooVs6meE\nwI7Xo3hu3rbkgNdGhggx2Su42sdbhT4EgOv//Ry7kDhcV/ABn7d8XR+a9A+0xgkJ\nVov3YcozSI6VQOg5GIEUbkGaaPoms/QBYNxja8fkoX+21tsahoTJ16/Gjy3u3EPQ\nnVLxrABzvQKBgGt74qQZzVaUIP4JCkajeMHUFkqRZwD94ZTLxBiIacpkVyxFpkZE\nl+gZpi71dH5NBUi90yEd0wW6PaxmgCXOpvWAYyD6pZNdFwebuuyWaxq5yy4wQKr/\nOn9fW0sTQkEacsPsF7HB1uOZUbTXEa8r6zuUNiRfNdpAE/464LIGk6nlAoGARq4f\nhV4DTlpjKdb4UWta3tc38O2SJvVSCQ65UdF6tj5zspb0kLCv4nVV0DmUBDrelIfZ\nkg12Ke+m1FytFv4/c7GKvFb9RKvzr2uQsjPWn+W71I7SxwBNk1l5J5Xz8jDyoduj\nQreHYjw3IENXYMahjz15ruWikaLGIA/7EwGBjUkCgYBarq5vmKfKAcj5cc+Hu7n5\n0+aJmEiycY/Y2squmC33adTixe6p2T0z/i/NE5NmBLuGoKNCNTVavAvEScjrWeaB\nkPWa0qEcUaCpsoD46OTyzg8Su6+76KS0ZzOqsUVJD8IgvPlIU3PoCbXkex1aSh4X\nFXYZfzyAwTbDODBMAU3ZUQ==\n-----END PRIVATE KEY-----\n",
  "client_email": "account-service@teste-gcp-2022.iam.gserviceaccount.com",
  "client_id": "113146422137808746869",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/account-service%40teste-gcp-2022.iam.gserviceaccount.com"
}

BUCKET_NAME = 'gcp_gb'
credentials = service_account.Credentials.from_service_account_info(credentials_dict)
client = storage.Client(project = 'teste-gcp-2022',credentials=credentials)

trusted_table_name = 'trusted.tb_vendas'

table_ano_mes = 'refined.tb_vendas_ano_mes'
bucket_temp = 'gs://gcp_gb/temp'

def config_log(flag_stdout=True, flag_logfile=False):
    handler_list = list()
    LOGGER = logging.getLogger(__name__)

    [LOGGER.removeHandler(h) for h in LOGGER.handlers]

    if flag_logfile:
        path_log = './logs/{}_{:%Y%m%d}.log'.format('log', datetime.now())
        if not os.path.isdir('./logs'):
            os.makedirs('./logs')
        handler_list.append(logging.FileHandler(path_log))

    if flag_stdout:
        handler_list.append(logging.StreamHandler(stdout))
        
    logging.basicConfig(
        level=logging.INFO\
        ,format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'\
        ,handlers=handler_list)    
    return LOGGER


def save(df, bucket_temp, table_name):
     df.write\
       .format("bigquery")\
       .option("writeMethod", "direct")\
	   .option("temporaryGcsBucket",bucket_temp)\
	   .mode("overwrite")\
       .save(table_name)
	   
	   
def read_table(table_name):
    df =  spark.read.format('bigquery')\
               .option('table', table_name)\
               .option("parentProject", "teste-gcp-2022")\
               .load()
    return df


def agg_ano_mes(df):
    df = df.select(f.col('ano_venda'), f.col('mes_venda'), f.col('qtd_venda'))
    df_ano_mes = df.groupBy(f.col('ano_venda'),f.col('mes_venda'))\
	               .agg(f.sum(f.col('qtd_venda'))\
				   .alias('qtd_venda'))
    return df_ano_mes

    
def orquestrador(df, log,table): 
    df_agg = agg_ano_mes(df)
	del df
    log.info(f'saving {table}')
    try:
        save(df_agg, bucket_temp,table )
    except Exception as e:   
        raise (f'Erro ao gravar dados no BigQuery: {e}') 
		
		
def main():
    log = config_log()
    log.info('reading table trusted.tb_vendas')
    df_trusted = read_table(trusted_table_name)
    orquestrador(df_trusted,log,table_ano_mes)
	log.info(f'finalizando ingestão {table_ano_mes}')

	
if __name__ == '__main__':
    spark = SparkSession.Builder()\
                       .appName("Refined_vendas_ano_mes")\
					   .getOrCreate() 
    main()
