"""
CHANGES FROM PREVIOUS VERSION (SINGLE-TASK DAG):

- Refactored the single `extract_and_upload` task into three distinct tasks:
  1. extract_task: extracts data from MariaDB
  2. write_task: writes extracted data to a CSV file
  3. upload_task: uploads the generated CSV file to an SFTP server

- Introduced XComs to explicitly pass data and file paths between tasks,
  instead of relying on implicit local state.
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


def extract_from_db(**context):
    db_conn_id = Variable.get("etl_db_conn_id", default_var="mariadb_default")
    query = Variable.get(
        "etl_extract_query",
        default_var="SELECT * FROM customers",
    )
    output_path = Variable.get(
        "etl_output_path", default_var="/tmp/etl_export.csv"
    )

    db_conn = BaseHook.get_connection(db_conn_id)

    connection = pymysql.connect(
        host=db_conn.host,
        port=db_conn.port or 3306,
        user=db_conn.login,
        password=db_conn.password,
        database=db_conn.schema or "",
        charset="utf8mb4",
    )

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

    context["ti"].xcom_push(
        key="extracted_data",
        value={
            "columns": columns,
            "rows": rows,
            "output_path": output_path,
        },
    )


def write_to_csv(**context):
    data = context["ti"].xcom_pull(
        task_ids="extract_task", key="extracted_data"
    )

    output_path = data["output_path"]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(data["columns"])
        writer.writerows(data["rows"])

    context["ti"].xcom_push(key="csv_path", value=output_path)


def upload_to_sftp(**context):
    sftp_conn_id = Variable.get("etl_sftp_conn_id", default_var="sftp_default")
    remote_path = Variable.get(
        "etl_sftp_remote_path", default_var="/sftpuser/upload/etl_export.csv"
    )

    output_path = context["ti"].xcom_pull(
        task_ids="write_task", key="csv_path"
    )

    sftp_conn = BaseHook.get_connection(sftp_conn_id)
    extra = sftp_conn.extra_dejson if sftp_conn.extra else {}

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
    sftp.put(output_path, remote_path)
    sftp.close()
    client.close()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="etl_mariadb_to_sftp_three_tasks",
    description="Three-task ETL: extract, write, upload",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "mariadb", "sftp"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract_from_db,
    )

    write_task = PythonOperator(
        task_id="write_task",
        python_callable=write_to_csv,
    )

    upload_task = PythonOperator(
        task_id="upload_task",
        python_callable=upload_to_sftp,
    )

    extract_task >> write_task >> upload_task
