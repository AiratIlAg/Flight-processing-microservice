FROM golang:1.24-bookworm AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/myapp ./cmd/server

FROM debian:bookworm-slim

WORKDIR /app
COPY --from=builder /out/myapp /app/myapp
COPY --from=builder /src/migrations /app/migrations

EXPOSE 8080

CMD ["/app/myapp"]
