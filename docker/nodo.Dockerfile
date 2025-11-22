# ----------------------------------------------------------
# STAGE 1: Build
# ----------------------------------------------------------
FROM golang:1.22 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o nodo ./cmd/nodo

# ----------------------------------------------------------
# STAGE 2: Run
# ----------------------------------------------------------
FROM debian:bookworm-slim

WORKDIR /app

COPY --from=builder /app/nodo ./nodo
COPY --from=builder /app/data ./data

# Puerto definido por env (9000 o 9001)
EXPOSE 9000

CMD ["./nodo"]
