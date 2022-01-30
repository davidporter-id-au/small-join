default: build

build:
	@echo "testing..."
	@go test ./...
	@echo "compiling "
	@GOOS=darwin go build -o small-join_darwin main.go
	@GOOS=linux go build -o small-join_linux main.go

