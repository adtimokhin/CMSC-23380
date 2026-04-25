.PHONY: proto build test test-stage1 test-stage2 test-stage3 test-raft clean docker-up docker-down docker-logs docker-partition docker-heal

proto:
	protoc \
		--go_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_out=. \
		--go-grpc_opt=paths=source_relative \
		proto/kvstore.proto proto/raft.proto

build:
	go build ./...

test:
	go test ./internal/... -v -race

test-stage1:
	go test ./raft/... -v -race -run TestStage1 -timeout 30s

test-stage2:
	go test ./raft/... ./server/... -v -race -run TestStage2 -timeout 60s

test-stage3:
	go test ./server/... -v -race -run TestStage3 -timeout 120s

test-raft:
	go test ./raft/... -v -race -timeout 60s

clean:
	rm -f server/kvraft_server client/kvctl

PROJECT := $(notdir $(CURDIR))

docker-up:
	$(eval TMP := $(shell mktemp -d))
	rsync -aL --exclude='.git' --exclude='*.zip' . $(TMP)/build/
	cd $(TMP)/build && docker compose -p $(PROJECT) up -d --build
	rm -rf $(TMP)

docker-down:
	docker compose -p $(PROJECT) down

docker-logs:
	docker compose -p $(PROJECT) logs -f

docker-partition: ## Disconnect a node from the cluster network. Usage: make docker-partition NODE=node0
	docker network disconnect $(PROJECT)_raft $(NODE)

docker-heal: ## Reconnect a node to the cluster network. Usage: make docker-heal NODE=node0
	docker network connect $(PROJECT)_raft $(NODE)
