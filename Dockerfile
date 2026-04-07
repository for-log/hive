FROM golang:1.25-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /orchestrator ./cmd/orchestrator

FROM alpine:3.21
RUN apk add --no-cache ca-certificates wget

COPY --from=builder /orchestrator /orchestrator

EXPOSE 8080
ENTRYPOINT ["/orchestrator"]
CMD ["/etc/orchestrator/config.yaml"]
