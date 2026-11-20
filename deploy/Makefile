SHELL := /bin/bash
COMPOSE := docker compose

.PHONY: up down logs ps pull migrate deploy verify smoke config

up:
	$(COMPOSE) up -d --remove-orphans

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f --tail=200

ps:
	$(COMPOSE) ps

pull:
	$(COMPOSE) pull

migrate:
	./scripts/migrate.sh

deploy:
	@test -n "$(TAG)" || (echo "Usage: make deploy TAG=<sha>" && exit 2)
	./scripts/deploy.sh "$(TAG)"

verify:
	./scripts/verify-role-coverage.sh
	./scripts/verify-build-pipeline.sh

smoke:
	./scripts/smoke-test.sh

config:
	$(COMPOSE) config
