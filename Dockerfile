FROM nanoservice/go

MAINTAINER Oleksii Fedorov <waterlink000@gmail.com>

ADD . /go/src/github.com/nanoservice/core-fanout
WORKDIR /go/src/github.com/nanoservice/core-fanout/fanout

RUN go get -d
RUN go test
RUN go install

EXPOSE 6777

ENTRYPOINT ["fanout"]
