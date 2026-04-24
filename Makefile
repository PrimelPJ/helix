.PHONY: build test lint clean cluster

build:
	go build -o bin/helixd  ./cmd/helixd
	go build -o bin/helixctl ./cmd/helixctl

test:
	go test ./... -v -race -timeout 60s

lint:
	golangci-lint run ./...

clean:
	rm -rf bin/

cluster:
	docker compose up --build
