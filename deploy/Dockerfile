FROM golang:1.25-alpine AS builder

WORKDIR /src

COPY ../go.mod go.sum ./
RUN go mod download

COPY .. .

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /out/hive-router ./cmd/router
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /out/hive-master ./cmd/master

# ---- router image ----
FROM alpine:3.21 AS router

RUN apk add --no-cache ca-certificates wget
COPY --from=builder /out/hive-router /usr/local/bin/hive-router

RUN mkdir -p /data /tmp/hive-router-snapshots

ENTRYPOINT ["hive-router"]

# ---- master image ----
FROM alpine:3.21 AS master

RUN apk add --no-cache ca-certificates
COPY --from=builder /out/hive-master /usr/local/bin/hive-master

RUN mkdir -p /data /tmp/hive-snapshots

ENTRYPOINT ["hive-master"]

# ---- example app image ----
FROM golang:1.25-alpine AS example

WORKDIR /src

COPY ../go.mod go.sum ./
RUN go mod download

COPY .. .

RUN mkdir -p /tmp/hive-example

ENTRYPOINT ["go", "run", "./examples/app/main.go"]
