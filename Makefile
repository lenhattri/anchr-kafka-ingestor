BINARY_NAME=anchr-kafka-ingestor

.PHONY: build test run fmt tidy

build:
	go build -o bin/$(BINARY_NAME) ./cmd/anchr-kafka-ingestor

test:
	go test ./...

run:
	go run ./cmd/anchr-kafka-ingestor

fmt:
	gofmt -w cmd internal

tidy:
	go mod tidy
