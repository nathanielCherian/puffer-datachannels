.PHONY: build run standin renditions test clean

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

# A simulcast source for testing without the sfu transcoder: ffmpeg encodes
# media/standin.mp4 into three renditions with aligned keyframes and sends them
# as MPEG-TS over UDP to 127.0.0.1:5001-5003. Next to it, run
#   make run ARGS="--simulcast high=127.0.0.1:5001@2500k,mid=127.0.0.1:5002@800k,low=127.0.0.1:5003@250k"
renditions: standin
	./scripts/simulcast-renditions.sh media/standin.mp4
