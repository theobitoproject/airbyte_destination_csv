FROM golang:1.19-buster as build

WORKDIR /base
ADD . /base/
RUN go build -o /base/app .
ENTRYPOINT ["/base/app"]
