.PHONY: build
build: # build go binary
	GOOS=linux GOARCH=amd64 go build -o kafka-dump

.PHONY: docker-build
docker-build: # build docker image from Dockerfile
	docker build . -t kafka-dump:s3 
