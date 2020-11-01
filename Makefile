# Build and run docker containter for MLdemo
# you can connect to jupyter lab with http://localhost:8888
run:
	@cd ./dockerfiles && \
	docker-compose up --build

# Remove all containers
rm:
	@docker rm -f `docker ps -aq`

# Enter in the container
note:
	@docker exec -i -t ml_bots_base bash