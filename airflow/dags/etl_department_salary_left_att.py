from datetime import datetime,date, timedelta
import pandas as pd
from io import BytesIO
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from minio import Minio
from sqlalchemy.engine import create_engine


DEFAULT_ARGS = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 5, 13),
}

dag = DAG('etl_department_salary_left_att', 
          default_args=DEFAULT_ARGS,
          schedule_interval="@once"
        )

data_lake_server = Variable.get("data_lake_server")
data_lake_login = Variable.get("data_lake_login")
data_lake_password = Variable.get("data_lake_password")

database_server = Variable.get("database_server")
database_login = Variable.get("database_login")
database_password = Variable.get("database_password")
database_name = Variable.get("database_name")


url_connection = "mysql+pymysql://{}:{}@{}/{}".format(
                 str(database_login)
                ,str(database_password)
                ,str(database_server)
                ,str(database_name)
                )

engine = create_engine(url_connection)

client = Minio(
        data_lake_server,
        access_key=data_lake_login,
        secret_key=data_lake_password,
        secure=False
    )

def extract():

    #query to consul the data.
    query = """SELECT emp.department as department,sal.salary as salary, emp.left
            FROM employees emp
            INNER JOIN salaries sal
            ON emp.emp_no = sal.emp_id;"""

    df_ = pd.read_sql_query(query,engine)
    
    #persist the files in the Staging area.
    df_.to_csv( "/tmp/department_salary_left.csv"
                ,index=False
            )

def load():

    #load the data from Staging area.
    df_ = pd.read_csv("/tmp/department_salary_left.csv")

    #convert data to parquet format.
    df_.to_parquet(
        "/tmp/department_salary_left.parquet"
        ,index=False
    )

    #load the data to the Data Lake.
    client.fput_object(
        "processing",
        "department_salary_left.parquet",
        "/tmp/department_salary_left.parquet"
    )


extract_task = PythonOperator(
    task_id='extract_data_from_database',
    provide_context=True,
    python_callable=extract,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_file_to_data_lake',
    provide_context=True,
    python_callable=load,
    dag=dag
)

clean_task = BashOperator(
    task_id="clean_files_on_staging",
    bash_command="rm -f /tmp/*.csv;rm -f /tmp/*.json;rm -f /tmp/*.parquet;",
    dag=dag
)

extract_task >> load_task >> clean_task