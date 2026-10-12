SHELL := /bin/bash
COMPOSE := docker compose

.DEFAULT_GOAL := help

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

env: ## Create .env from the template if it does not exist
	@test -f .env || (cp .env.example .env && echo "created .env")

build: env ## Build the app and dbt images
	$(COMPOSE) --profile jobs build

up: build ## Start infrastructure + streaming services + API
	$(COMPOSE) up -d
	@echo ""
	@echo "  API docs      http://localhost:8000/docs"
	@echo "  Kafka UI      http://localhost:8081"
	@echo "  RustFS console http://localhost:9001 (rustfsadmin / rustfsadmin)"
	@echo "  PostgreSQL    localhost:5432          (jobmarket / jobmarket)"

ingest: ## Pull jobs from the APIs into Kafka
	$(COMPOSE) run --rm ingest

drain: ## Flush everything on the Kafka topic into the raw data lake
	$(COMPOSE) run --rm ingest python -m jobmarket.lake_sink --until-idle 25

spark: ## Run the Spark batch job (lake -> PostgreSQL)
	$(COMPOSE) run --rm --service-ports spark

dbt: ## Run dbt seed + models + tests
	$(COMPOSE) run --rm dbt build

pipeline: ingest drain spark dbt ## Run the whole batch pipeline once, end to end
	@echo "pipeline finished - try: curl -s localhost:8000/skills/top?limit=15 | python3 -m json.tool"

top-skills: ## Print the top 20 skills from the warehouse
	$(COMPOSE) exec postgres psql -U jobmarket -d jobmarket -c \
	  "select demand_rank, skill, skill_category, job_count, pct_of_tech_jobs, median_salary_usd from marts.mart_skill_demand order by job_count desc limit 20"


test: ## Run unit tests locally
	python3 -m pytest -q

logs: ## Tail logs of the streaming services
	$(COMPOSE) logs -f lake-sink realtime api

down: ## Stop everything (keeps data volumes)
	$(COMPOSE) --profile jobs down

clean: ## Stop everything and delete all data volumes
	$(COMPOSE) --profile jobs down -v

.PHONY: help env build up ingest drain spark dbt pipeline top-skills test logs down clean
