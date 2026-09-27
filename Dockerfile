# syntax=docker/dockerfile:1

FROM golang:1.26.1-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

FROM build AS gateway-build

COPY cmd/gateway/ ./cmd/gateway/
COPY internal/ ./internal/

RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/gatex \
    ./cmd/gateway

FROM build AS mockbackend-build

COPY cmd/mockbackend/ ./cmd/mockbackend/

RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/mockbackend \
    ./cmd/mockbackend

FROM gcr.io/distroless/static-debian12:nonroot AS mockbackend

COPY --from=mockbackend-build --chown=nonroot:nonroot /out/mockbackend /usr/local/bin/mockbackend

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/mockbackend"]
CMD ["-listen", ":8080"]

FROM gcr.io/distroless/static-debian12:nonroot AS runtime

WORKDIR /app

COPY --from=gateway-build --chown=nonroot:nonroot /out/gatex /usr/local/bin/gatex

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/gatex"]
