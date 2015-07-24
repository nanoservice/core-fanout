FROM nanoservice-dev/fanout-base
ADD . /go/src/github.com/nanoservice/core-fanout/fanout
WORKDIR /go/src/github.com/nanoservice/core-fanout/fanout
ENTRYPOINT ["go", "test", "./...", "--tags", "integration"]
