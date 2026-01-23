export CGO_ENABLED=0

build:
	go build -o bin/nfsusage main.go

run:
	go run main.go
