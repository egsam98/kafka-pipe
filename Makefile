IMAGE = egsam98/kafka-pipe
VERSION ?= dev

-include .env

help: ## Show this help.
	@fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/\\$$//' | sed -e 's/##//'

install-tools: ## Install necessary tools for targets
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

lint: ## Run linter
	go mod tidy
	golangci-lint run

warden: ## Run warden validator codegen
	warden --tag=yaml ./ ./cmd/app ./connectors/...

build: ## Build docker image
	docker build --build-arg "VERSION=$(VERSION)" -t $(IMAGE):$(VERSION) .

push: ## Push built docker image to DockerHub
	docker push $(IMAGE):$(VERSION)
