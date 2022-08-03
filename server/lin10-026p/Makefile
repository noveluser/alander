SHELL := /bin/bash

# VIDI Deployment

start-splunk:
	docker-compose -f ./docker-compose.yml --env-file ./.env up -d 

restart-splunk:
	docker-compose exec -u splunk szx_vidi_standalone ./bin/splunk restart

stop-splunk:
	docker-compose down
