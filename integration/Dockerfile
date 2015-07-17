FROM nanoservice/go

ADD . /go/src/github.com/nanoservice/core-fanout
WORKDIR /go/src/github.com/nanoservice/core-fanout

RUN go get -d -t --tags integration ./...

ENTRYPOINT ["go", "test", "./...", "--tags", "integration"]
