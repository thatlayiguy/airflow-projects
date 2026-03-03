"""
REVERSE ETL DAG: SFTP -> CSV -> MariaDB

STRUCTURE (mirrors the forward ETL DAG):
1. download_task:
   - Connects to SFTP using Airflow connection + key-based auth
   - Downloads a CSV file to local disk

2. read_task:
   - Reads the downloaded CSV file
   - Pushes parsed rows and column names via XCom

3. load_task:
   - Inserts CSV data into a MariaDB table using PyMySQL
"""

import csv
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path

import paramiko
import pymysql
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator


def download_from_sftp(**context):
    sftp_conn_id = Variable.get("etl_sftp_conn_id", default_var="sftp_default")
    remote_path = Variable.get(
        "etl_sftp_remote_path", default_var="/sftpuser/upload/etl_export.csv"
    )
    local_path = Variable.get(
        "etl_input_path", default_var="/tmp/etl_import.csv"
    )

    sftp_conn = BaseHook.get_connection(sftp_conn_id)
    extra = sftp_conn.extra_dejson if sftp_conn.extra else {}

    Path(local_path).parent.mkdir(parents=True, exist_ok=True)

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    connect_kwargs = {
        "hostname": sftp_conn.host,
        "port": sftp_conn.port or 22,
        "username": sftp_conn.login,
        "password": sftp_conn.password or None,
    }

    if "private_key" in extra:
        key_str = extra["private_key"]
        for key_class in (
            paramiko.RSAKey,
            paramiko.Ed25519Key,
            paramiko.ECDSAKey,
        ):
            try:
                pkey = key_class.from_private_key(StringIO(key_str))
                break
            except paramiko.ssh_exception.SSHException:
                continue
        else:
            raise ValueError("Unable to parse private key")

        connect_kwargs["pkey"] = pkey
        connect_kwargs["password"] = None

    client.connect(**connect_kwargs)
    sftp = client.open_sftp()
    sftp.get(remote_path, local_path)
    sftp.close()
    client.close()

    context["ti"].xcom_push(key="csv_path", value=local_path)


def read_csv(**context):
    csv_path = context["ti"].xcom_pull(
        task_ids="download_task", key="csv_path"
    )

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        columns = next(reader)
        rows = list(reader)

    context["ti"].xcom_push(
        key="csv_data",
        value={"columns": columns, "rows": rows},
    )


def load_into_db(**context):
    db_conn_id = Variable.get("etl_db_conn_id", default_var="mariadb_default")
    target_table = Variable.get(
        "etl_target_table", default_var="customers"
    )

    data = context["ti"].xcom_pull(
        task_ids="read_task", key="csv_data"
    )

    columns = data["columns"]
    rows = data["rows"]

    placeholders = ", ".join(["%s"] * len(columns))
    col_names = ", ".join(columns)

    insert_sql = f"""
        INSERT INTO {target_table} ({col_names})
        VALUES ({placeholders})
    """

    db_conn = BaseHook.get_connection(db_conn_id)

    connection = pymysql.connect(
        host=db_conn.host,
        port=db_conn.port or 3306,
        user=db_conn.login,
        password=db_conn.password,
        database=db_conn.schema or "",
        charset="utf8mb4",
    )

    with connection:
        with connection.cursor() as cursor:
            cursor.executemany(insert_sql, rows)
        connection.commit()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="etl_sftp_to_mariadb",
    description="Reverse ETL: SFTP to MariaDB",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "sftp", "mariadb"],
) as dag:

    download_task = PythonOperator(
        task_id="download_task",
        python_callable=download_from_sftp,
    )

    read_task = PythonOperator(
        task_id="read_task",
        python_callable=read_csv,
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load_into_db,
    )

    download_task >> read_task >> load_task
