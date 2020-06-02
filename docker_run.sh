docker rm -f $(docker ps -q -a)
docker build -t crypto_env:latest ./Dockerfiles/
docker run -p 8888:8888 -it -v $PWD:/share crypto_env:latest