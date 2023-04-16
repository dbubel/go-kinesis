db:
	docker-compose -f docker-compose.yml up --build --abort-on-container-exit postgres
localstack:
	docker-compose -f docker-compose.yml up --build --abort-on-container-exit localstack
kinesis:
	aws --endpoint-url http://localhost:4566 kinesis delete-stream --stream-name test_stream
	aws --endpoint-url http://localhost:4566 kinesis create-stream --shard-count 4 --stream-name test_stream

