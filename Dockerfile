# syntax=docker/dockerfile:1

# Repository foundation image. The binaries are placeholders until runtime behavior
# is implemented, but this build path is intended to remain the multi-binary path.
FROM golang:1.22-alpine AS build
WORKDIR /src

COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/edge-gateway ./cmd/edge-gateway \
 && CGO_ENABLED=0 GOOS=linux go build -o /out/private-connector ./cmd/private-connector \
 && CGO_ENABLED=0 GOOS=linux go build -o /out/signurl ./cmd/signurl

FROM alpine:3.20
RUN adduser -D -H -u 10001 appuser
COPY --from=build /out/ /usr/local/bin/
USER appuser

# Run the edge gateway by default. Override the command to run another binary,
# for example: docker run --rm air3 /usr/local/bin/private-connector
CMD ["/usr/local/bin/edge-gateway"]
