FROM golang:1.4

MAINTAINER Oleksii Fedorov <waterlink000@gmail.com>

RUN mkdir -p /go/src/github.com/nanoservice/core-fanout
ADD . /go/src/github.com/nanoservice/core-fanout

WORKDIR /go/src/github.com/nanoservice/core-fanout
RUN ./bin/test

WORKDIR /go/src/github.com/nanoservice/core-fanout/fanout
RUN go install

EXPOSE 6777

ENTRYPOINT ["/go/bin/fanout"]
