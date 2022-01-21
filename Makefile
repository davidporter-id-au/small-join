default: build

build:
	@echo "compiling with envvars GOOS: $(GOOS) GOARCH: $(GOARCH)"
	@go build -o small-join main.go

