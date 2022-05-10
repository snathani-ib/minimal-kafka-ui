.PHONY: run-setup
run-setup: setup-local-kafka run-app

.PHONY: run-app
run-app:
	-echo "Running"
	-pip install -r requirements.txt
	-python app.py

.PHONY: setup-local-kafka
setup-local-kafka:
	-echo "Works on Mac and not on Windows"
	-docker-compose up -d
	-echo "Kafka will be available in 5 minutes on port 9092"
	-echo "Messages in resource/messages will to be pushed to kafka"
	-echo "Please run this command after kafka is up: cd Scripts && python producer1.py"

.PHONY: stop-local-kafka
stop-local-kafka:
	-echo "Kafka will be terminated"
	-cd docker && docker-compose down
