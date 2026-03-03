"""
ETL DAG: Extract from MariaDB -> Write to file -> Upload to SFTP (key-based auth)

Uses PyMySQL and paramiko directly (no airflow.providers.mysql required).

Airflow Connections required:
  - mariadb_default (or etl_db_conn_id): Generic connection
    * Host, Schema (database), Login, Password, Port
  - sftp_default (or etl_sftp_conn_id): SSH connection with key-based auth
    * Host, Username, Port
    * Extra: {"key_file": "/path/to/private_key"} or {"private_key": "<key content>"}

Variables (optional, have defaults):
  - etl_db_conn_id: DB connection ID
  - etl_output_path: Local file path (default: /tmp/etl_export.csv)
  - etl_extract_query: SQL query to run
  - etl_sftp_conn_id: SFTP connection ID
  - etl_sftp_remote_path: Remote file path
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
    """Extract data from MariaDB using PyMySQL and write to CSV file."""
    conn_id = Variable.get("etl_db_conn_id", default_var="mariadb_default")
    output_path = Variable.get("etl_output_path", default_var="/tmp/etl_export.csv")
    query = Variable.get(
        "etl_extract_query",
        default_var="SELECT 1 AS id, 'test' AS name LIMIT 1",
    )

    conn = BaseHook.get_connection(conn_id)
    connection = pymysql.connect(
        host=conn.host,
        port=conn.port or 3306,
        user=conn.login,
        password=conn.password,
        database=conn.schema or "",
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

    context["ti"].xcom_push(key="output_path", value=output_path)
    return output_path


def upload_to_sftp(**context):
    """Upload file to SFTP server using paramiko (key-based auth)."""
    ti = context["ti"]
    local_path = ti.xcom_pull(task_ids="extract_and_write", key="output_path")
    if not local_path:
        local_path = ti.xcom_pull(task_ids="extract_and_write")
    remote_path = Variable.get("etl_sftp_remote_path", default_var="/upload/etl_export.csv")
    conn_id = Variable.get("etl_sftp_conn_id", default_var="sftp_default")

    conn = BaseHook.get_connection(conn_id)
    extra = conn.extra_dejson if conn.extra else {}

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    connect_kwargs = {
        "hostname": conn.host,
        "port": conn.port or 22,
        "username": conn.login,
        "password": conn.password or None,
    }

    if "private_key" in extra:
        key_str = extra["private_key"]
        for key_class in (paramiko.RSAKey, paramiko.Ed25519Key, paramiko.ECDSAKey):
            try:
                pkey = key_class.from_private_key(StringIO(key_str))
                break
            except paramiko.ssh_exception.SSHException:
                continue
        else:
            raise ValueError("Could not parse private key")
        connect_kwargs["pkey"] = pkey
        connect_kwargs["password"] = None
    elif "key_file" in extra:
        for key_class in (paramiko.RSAKey, paramiko.Ed25519Key, paramiko.ECDSAKey):
            try:
                pkey = key_class.from_private_key_file(extra["key_file"])
                break
            except paramiko.ssh_exception.SSHException:
                continue
        else:
            raise ValueError("Could not parse key file")
        connect_kwargs["pkey"] = pkey
        connect_kwargs["password"] = None

    client.connect(**connect_kwargs)
    sftp = client.open_sftp()
    sftp.put(local_path, remote_path)
    sftp.close()
    client.close()

    return f"Uploaded {local_path} to {remote_path}"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="etl_mariadb_to_sftp",
    default_args=default_args,
    description="Extract from MariaDB, write to file, upload to SFTP (key-based auth)",
    schedule=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "mariadb", "sftp"],
) as dag:

    extract_and_write = PythonOperator(
        task_id="extract_and_write",
        python_callable=extract_from_db,
    )

    upload_file = PythonOperator(
        task_id="upload_to_sftp",
        python_callable=upload_to_sftp,
    )

    extract_and_write >> upload_file
