FROM golang:1.23-alpine AS builder

ARG VERSION

WORKDIR /app

COPY go.mod .
COPY go.sum .
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags "-X github.com/egsam98/kafka-pipe.Version=$VERSION" -o kafka-pipe cmd/app/main.go

FROM alpine
WORKDIR /app
COPY --from=builder /app/kafka-pipe /app
EXPOSE 8081
CMD ["/app/kafka-pipe", "/app/config.yaml"]
