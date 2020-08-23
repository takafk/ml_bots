# Remove all containers
docker rm -f $(docker ps -q -a)

# Build and run docker containter for MLdemo
# you can connect to jupyter lab with http://localhost:8888
docker-compose up --build