run:
	@cd ./dockerfiles && \
	bash run.sh
rm:
	docker rm -f $(docker ps -q -a)

note:
	@docker exec -i -t jupyterlab bash