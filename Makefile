####################################################################################################################
# Docker Containers for Airflow
####################################################################################################################

docker-spin-up:
	docker compose up airflow-init && docker compose up --build -d

perms:
	sudo mkdir -p logs plugins temp dags tests migrations data visualization && sudo chmod -R u=rwx,g=rwx,o=rwx logs plugins temp dags tests migrations data visualization

setup-conn:
	docker exec scheduler python /opt/airflow/airflow_conn.py

do-sleep:
	sleep 30

up: perms docker-spin-up do-sleep setup-conn

down:
	docker compose down --volumes --rmi all

restart: down up

sh:
	docker exec -ti webserver bash

