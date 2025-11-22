# ----------------------------------------------------------
# STAGE 1: Build
# ----------------------------------------------------------
FROM golang:1.22 AS builder

WORKDIR /app

# Dependencias
COPY go.mod go.sum ./
RUN go mod download

# Copiar todo
COPY . .

# Compilar API
RUN go build -o api ./cmd/api

# ----------------------------------------------------------
# STAGE 2: Run
# ----------------------------------------------------------
FROM debian:bookworm-slim

WORKDIR /app

COPY --from=builder /app/api ./api
COPY --from=builder /app/data ./data

# Puerto HTTP
EXPOSE 8080

CMD ["./api"]
