"""
Single-task ETL DAG: Extract from MariaDB -> Write CSV -> Upload to SFTP

Why single task:
- MWAA workers are ephemeral
- Local files are NOT shared between tasks
- Keeping extract + upload in one task guarantees file availability

Airflow Connections required:
- mariadb_default (or via Variable etl_db_conn_id)
- sftp_default (SSH connection with key-based auth)

Variables:
- etl_extract_query: SQL query (default: SELECT * FROM your_table)
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


def extract_and_upload(**context):
    # -----------------------------
    # Config
    # -----------------------------
    db_conn_id = Variable.get("etl_db_conn_id", default_var="mariadb_default")
    sftp_conn_id = Variable.get("etl_sftp_conn_id", default_var="sftp_default")

    output_path = Variable.get(
        "etl_output_path", default_var="/tmp/etl_export.csv"
    )
    remote_path = Variable.get(
        "etl_sftp_remote_path", default_var="/sftpuser/upload/etl_export.csv"
    )
    query = Variable.get(
        "etl_extract_query",
        default_var="SELECT * FROM customers",
    )

    # -----------------------------
    # Extract from MariaDB
    # -----------------------------
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

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)

    # -----------------------------
    # Upload to SFTP
    # -----------------------------
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

    # Inline private key (recommended for MWAA)
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

    # Key file path (only if baked into image)
    elif "key_file" in extra:
        for key_class in (
            paramiko.RSAKey,
            paramiko.Ed25519Key,
            paramiko.ECDSAKey,
        ):
            try:
                pkey = key_class.from_private_key_file(extra["key_file"])
                break
            except paramiko.ssh_exception.SSHException:
                continue
        else:
            raise ValueError("Unable to read key file")

        connect_kwargs["pkey"] = pkey
        connect_kwargs["password"] = None

    client.connect(**connect_kwargs)
    sftp = client.open_sftp()
    sftp.put(output_path, remote_path)
    sftp.close()
    client.close()

    return f"Extracted data and uploaded {output_path} to {remote_path}"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="etl_mariadb_to_sftp_single_task",
    description="Single-task ETL: MariaDB to SFTP (MWAA-safe)",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "mariadb", "sftp"],
) as dag:

    etl_task = PythonOperator(
        task_id="extract_and_upload",
        python_callable=extract_and_upload,
    )
