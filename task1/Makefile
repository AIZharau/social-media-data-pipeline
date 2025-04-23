.PHONY: setup install run-etl run-stream run-dlt db-init db-migrate docker-up docker-down clean venv browsers deps

venv:
	python3 -m venv venv
	venv/bin/pip install --upgrade pip
	venv/bin/pip install -r requirements.txt

deps:
	sudo apt-get install -y libatk1.0-0 libatk-bridge2.0-0 libcups2 libxkbcommon0 libxdamage1 libgbm1 libpango-1.0-0 libcairo2 libatspi2.0-0

browsers: deps
	playwright install chromium

setup: venv browsers db-init db-migrate

install:
	pip install -r requirements.txt

run-etl:
	python3 src/etl_pipeline.py

run-stream:
	python3 src/kafka_consumer.py

db-init:
	docker-compose up -d postgres
	sleep 5
	@command -v psql >/dev/null 2>&1 || { echo "Error: PostgreSQL client (psql) не установлен. Установите командой: sudo apt install postgresql-client"; exit 1; }
	PGPASSWORD=postgres psql -h localhost -U postgres -d tiktok_data -c "SELECT 1" 2>/dev/null || PGPASSWORD=postgres createdb -h localhost -U postgres tiktok_data

db-migrate:
	@command -v psql >/dev/null 2>&1 || { echo "Error: PostgreSQL client (psql) не установлен. Установите командой: sudo apt install postgresql-client"; exit 1; }
	PGPASSWORD=postgres psql -h localhost -U postgres -d tiktok_data -f migrations/create_tables.sql

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down

clean:
	docker-compose down -v 