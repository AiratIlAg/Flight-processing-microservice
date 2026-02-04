FROM golang:1.24-bookworm AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o myapp ./cmd/server

FROM debian:bookworm-slim

WORKDIR /app
COPY --from=builder /app/myapp .

EXPOSE 8080

CMD ["./myapp"]
