.PHONY: build test lint integration-test clean fmt vet

# Build the project
build:
	go build ./...

# Run unit tests with race detection
test:
	go test -race -v ./...

# Run linter
lint:
	golangci-lint run ./...

# Run integration tests
integration-test:
	go test -race -v -tags=integration ./...

# Clean build artifacts
clean:
	go clean ./...

# Format code
fmt:
	go fmt ./...

# Run go vet
vet:
	go vet ./...

# Run all checks
check: fmt vet lint test
