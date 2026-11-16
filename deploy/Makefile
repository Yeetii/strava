SHELL := /bin/bash
COMPOSE := docker compose

.PHONY: up down logs ps pull deploy verify smoke config

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

deploy:
	@test -n "$(TAG)" || (echo "Usage: make deploy TAG=<sha>" && exit 2)
	./scripts/deploy.sh "$(TAG)"

verify:
	./scripts/verify-role-coverage.sh

smoke:
	./scripts/smoke-test.sh

config:
	$(COMPOSE) config
