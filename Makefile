.PHONY: build run standin test clean

build:
	CGO_ENABLED=1 go build -o bin/server ./cmd/server

standin: media/standin.mp4

media/standin.mp4:
	./scripts/make-standin.sh $@

run: build standin
	./bin/server $(ARGS)

test:
	go test ./...

clean:
	rm -rf bin
