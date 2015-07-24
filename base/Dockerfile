FROM nanoservice/go

MAINTAINER Oleksii Fedorov <waterlink000@gmail.com>

RUN go get github.com/nanoservice/core-fanout/fanout/fanout
WORKDIR /go/src/github.com/nanoservice/core-fanout/fanout/fanout

RUN cd ../ && go get -d -t --tags integration ./...
RUN go test
RUN go install
