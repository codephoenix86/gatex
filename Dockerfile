# syntax=docker/dockerfile:1

FROM golang:1.26.1-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/gateway/ ./cmd/gateway/
COPY internal/ ./internal/

RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/gatex \
    ./cmd/gateway

FROM gcr.io/distroless/static-debian12:nonroot AS runtime

WORKDIR /app

COPY --from=build --chown=nonroot:nonroot /out/gatex /usr/local/bin/gatex
COPY --chown=nonroot:nonroot configs/gateway.example.yaml /etc/gatex/gateway.yaml

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/gatex"]
CMD ["-config", "/etc/gatex/gateway.yaml"]
