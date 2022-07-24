
from datetime import datetime 
from airflow import DAG
from os import getenv

from airflow.providers.google.cloud.operators.gcs import *
from airflow.providers.google.cloud.operators.dataproc import *
from airflow.providers.google.cloud.operators.bigquery import *
from airflow.providers.google.cloud.sensors.dataproc import *

GCP_PROJECT_ID = getenv("GCP_PROJECT_ID", "teste-gcp-2022")
REGION = getenv("REGION","us-central1")
PYSPARK_ANO_MES = 'gs://gcp_gb/refined_vendas_ano_mes.py'
PYSPARK_MARCA_LINHA = 'gs://gcp_gb/refined_vendas_marca_linha.py'
PYSPARK_MARCA_ANO_MES = 'gs://gcp_gb/refined_vendas_marca_ano_mes.py'
PYSPARK_LINHA_ANO_MES = 'gs://gcp_gb/refined_vendas_linha_ano_mes.py'
PYSPARK_TRUSTED = 'gs://gcp_gb/trusted_vendas.py'
PYSPARK_RAW = 'gs://gcp_gb/raw_vendas.py'
BG_JAR = 'gs://gcp_gb/jars/spark-bigquery-with-dependencies_2.12-0.26.0.jar'
CLUSTER_NAME = getenv("CLUSTER_NAME","cluster-gp-gb")

from airflow import models
from airflow.operators import bash_operator
from airflow.operators import python_operator

default_dag_args = {
    'owner': 'as.segatelli',
	'retries':1}
	
    #'start_date': datetime.datetime(2022, 7, 24),
#}


with models.DAG(
        dag_id ='Teste-GCP',
        default_args = default_dag_args,
        start_date=datetime(year=2022, month=7, day=24),
        schedule_interval='@daily',
        catchup=False
) as dag:
		
		
    config_job_ano_mes={
	    "reference":{"project_id":GCP_PROJECT_ID},
	    "placement":{"cluster": CLUSTER_NAME},
	    "pyspark_job":{"main_python_file_uri":PYSPARK_ANO_MES}
    }
	
    ano_mes_submit = DataprocSubmitJobOperator(
		task_id="job_ano_mes",
		project_id=GCP_PROJECT_ID,
		location=REGION,
		job=config_job_ano_mes,
		asynchronous=True,
		gcp_conn_id="gcp"
    )	


    config_job_marca_linha={
	"reference":{"project_id":GCP_PROJECT_ID},
	"placement":{"cluster": CLUSTER_NAME},
	"pyspark_job":{"main_python_file_uri":PYSPARK_MARCA_LINHA}
     }

    marca_linha_submit = DataprocSubmitJobOperator(
		task_id="job_marca_linha",
		project_id=GCP_PROJECT_ID,
		location=REGION,
		job=config_job_marca_linha,
		asynchronous=True,
		gcp_conn_id="gcp"
    )	


    config_job_marca_ano_mes={
	"reference":{"project_id":GCP_PROJECT_ID},
	"placement":{"cluster": CLUSTER_NAME},
	"pyspark_job":{"main_python_file_uri":PYSPARK_MARCA_ANO_MES}
    }


    marca_ano_mes_submit = DataprocSubmitJobOperator(
		task_id="job_marca_ano_mes",
		project_id=GCP_PROJECT_ID,
		location=REGION,
		job=config_job_marca_ano_mes,
		asynchronous=True,
		gcp_conn_id="gcp"
    )	


    config_job_linha_ano_mes={
	"reference":{"project_id":GCP_PROJECT_ID},
	"placement":{"cluster": CLUSTER_NAME},
	"pyspark_job":{"main_python_file_uri":PYSPARK_LINHA_ANO_MES}
    }

    linha_ano_mes_submit = DataprocSubmitJobOperator(
		task_id="job_linha_ano_mes",
		project_id=GCP_PROJECT_ID,
		location=REGION,
		job=config_job_linha_ano_mes,
		asynchronous=True,
		gcp_conn_id="gcp"
    )	


    config_job_trusted={
	"reference":{"project_id":GCP_PROJECT_ID},
	"placement":{"cluster": CLUSTER_NAME},
	"pyspark_job":{"main_python_file_uri":PYSPARK_TRUSTED}
    }

    trusted_submit = DataprocSubmitJobOperator(
		task_id="job_trusted",
		project_id=GCP_PROJECT_ID,
		location=REGION,
		job=config_job_trusted,
		asynchronous=True,
		gcp_conn_id="gcp"
    )	

    config_job_raw={
	"reference":{"project_id":GCP_PROJECT_ID},
	"placement":{"cluster": CLUSTER_NAME},
	"pyspark_job":{"main_python_file_uri":PYSPARK_RAW}
    }
		
    raw_submit = DataprocSubmitJobOperator(
		task_id="job_raw",
		project_id=GCP_PROJECT_ID,
		location=REGION,
		job=config_job_raw,
		asynchronous=True,
		gcp_conn_id="gcp"
    )	
				
#gcs_sync_repo=GCSSyncronizeBucketOperator(
#    task_id = 'gcs_sync_repo',
#	source_bucket='',
	#source_objects='',
	#destination_bucket='gs://gcp_gb',
	##destination_object='',
	#allow_overwrite=True,
	#gcp_conn_id='gcp'
#)



    raw_submit>> trusted_submit >>[ ano_mes_submit, linha_ano_mes_submit, marca_ano_mes_submit, marca_linha_submit ]
		