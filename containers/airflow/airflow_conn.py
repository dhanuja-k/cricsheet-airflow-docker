import subprocess
import json

conn_id = "aws_default"
conn_type = "aws"
extra = {
            "aws_access_key_id": "minio",
            "aws_secret_access_key": "minio123",
            "region_name": "us-east-1",
            "host": "http://minio:9000",
}

extra_json = json.dumps(extra)

command = [
    "airflow",
    "connections",
    "add",
    conn_id,
    "--conn-type",
    conn_type,
    "--conn-extra",
    extra_json,
]
# Execute the command
subprocess.run(command)

