FROM golang:1.21-alpine as BUILDER

WORKDIR /app

COPY go.mod .
COPY go.sum .
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o kafka-pipe cmd/app/main.go

FROM alpine
WORKDIR /app
COPY --from=BUILDER /app/kafka-pipe /app
EXPOSE 8081
CMD ["/app/kafka-pipe", "/app/config.yaml"]
