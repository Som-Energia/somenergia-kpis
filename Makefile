.DEFAULT_GOAL := help
.PHONY: help

# environment variables
include .env


# taken from https://container-solutions.com/tagging-docker-images-the-right-way/

help: ## Print this help
	@grep -E '^[0-9a-zA-Z_\-\.]+:.*?## .*$$' Makefile | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

build.main: ## Build the main image using the Dockerfile in docker/main
	docker compose build main

build.legacy-py38: ## Build the legacy images with py38 using the Dockerfiles in docker/legacy
	docker compose build legacy-py38

build.legacy-py310: ## Build the legacy images with py310 using the Dockerfiles in docker/legacy
	docker compose build legacy-py310