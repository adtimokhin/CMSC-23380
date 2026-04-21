.PHONY: proto build test clean

# Regenerate Go code from proto definitions.
# Requires: protoc, protoc-gen-go, protoc-gen-go-grpc
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
proto:
	protoc \
		--go_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_out=. \
		--go-grpc_opt=paths=source_relative \
		proto/kvstore.proto

build:
	go build ./...

test:
	go test ./internal/... -v -race

clean:
	rm -f server/kvstore_server client/kvctl
