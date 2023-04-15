db:
	docker-compose -f docker-compose.yml up --build --abort-on-container-exit postgres
kinesis:
	docker-compose -f docker-compose.yml up --build --abort-on-container-exit localstack
