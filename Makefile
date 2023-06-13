####################################################################################################################
# Docker Containers for Airflow
####################################################################################################################

docker-spin-up:
	docker compose up airflow-init && docker compose up --build -d

perms:
	sudo mkdir -p logs plugins temp dags tests migrations data visualization && sudo chmod -R u=rwx,g=rwx,o=rwx logs plugins temp dags tests migrations data visualization

up: perms docker-spin-up 

down:
	docker compose down --volumes --rmi all

restart: down up

sh:
	docker exec -ti webserver bash


